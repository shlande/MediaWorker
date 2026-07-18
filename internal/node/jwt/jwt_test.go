package jwt

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"sync"
	"testing"
	"time"

	"github.com/libp2p/go-libp2p"
	"github.com/libp2p/go-libp2p/core/crypto"
	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/core/pnet"

	"github.com/shlande/mediaworker/internal/config"
	cpjwt "github.com/shlande/mediaworker/internal/controlplane/jwt"
	sjwt "github.com/shlande/mediaworker/internal/shared/jwt"
	"github.com/shlande/mediaworker/internal/types"
)

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

// newTestPSK returns a PSK derived from a seed for use with pnet.
func newTestPSK(seed string) (pnet.PSK, error) {
	hash := sjwt.SHA256Sum([]byte(seed))
	return pnet.PSK(hash[:]), nil
}

// createLibp2pHost creates a libp2p host bound to a random port with the given
// Ed25519 key and PSK.
func createLibp2pHost(privKey ed25519.PrivateKey, psk pnet.PSK) (host.Host, error) {
	libp2pPriv, err := crypto.UnmarshalEd25519PrivateKey(privKey)
	if err != nil {
		return nil, err
	}

	return libp2p.New(
		libp2p.Identity(libp2pPriv),
		libp2p.ListenAddrStrings("/ip4/127.0.0.1/tcp/0"),
		libp2p.PrivateNetwork(psk),
		libp2p.ForceReachabilityPrivate(),
	)
}

// ---------------------------------------------------------------------------
// In-memory PeerStore (satisfies PeerStoreWriter)
// ---------------------------------------------------------------------------

type memPeerStore struct {
	mu   sync.Mutex
	data map[types.PeerId]types.PeerStoreEntry
}

func newMemPeerStore() *memPeerStore {
	return &memPeerStore{data: make(map[types.PeerId]types.PeerStoreEntry)}
}

func (s *memPeerStore) Get(p types.PeerId) (types.PeerStoreEntry, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	e, ok := s.data[p]
	if !ok {
		return types.PeerStoreEntry{}, fmt.Errorf("not found")
	}
	return e, nil
}

func (s *memPeerStore) Put(e types.PeerStoreEntry) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.data[e.PeerID] = e
	return nil
}

// ---------------------------------------------------------------------------
// Test: Sign and Verify
// ---------------------------------------------------------------------------

func TestJWT_SignAndVerify(t *testing.T) {
	cpPub, cpPriv, err := sjwt.GenerateEd25519Key()
	if err != nil {
		t.Fatalf("generate control plane key: %v", err)
	}
	_, nodePriv, err := sjwt.GenerateEd25519Key()
	if err != nil {
		t.Fatalf("generate node key: %v", err)
	}
	nodePeerID, err := sjwt.GeneratePeerID(nodePriv)
	if err != nil {
		t.Fatalf("generate node peer ID: %v", err)
	}

	svc := cpjwt.NewJWTService(cpPriv, cpjwt.NewPeerIdSet(), cpjwt.NewRateLimiter(cpjwt.DefaultRateLimitInterval),
		cpjwt.NewAuditLog(nil), config.JWTPolicyConfig{})

	signedPeerID := sjwt.SignPeerID(nodePriv, nodePeerID)
	req := types.JWTRequest{PeerID: nodePeerID, SignedPeerID: signedPeerID}

	resp, err := svc.HandleJWTRequest(req, "127.0.0.1")
	if err != nil {
		t.Fatalf("HandleJWTRequest: %v", err)
	}

	if resp.RefreshBefore != 300 {
		t.Errorf("RefreshBefore = %d, want 300", resp.RefreshBefore)
	}

	// Verify the JWT
	payload, err := sjwt.VerifyJWT(resp.JWT, cpPub, nodePeerID)
	if err != nil {
		t.Fatalf("VerifyJWT: %v", err)
	}

	if payload.PeerID != nodePeerID {
		t.Errorf("PeerID = %q, want %q", payload.PeerID, nodePeerID)
	}
	if !payload.Capabilities.Edge {
		t.Error("Edge should be true")
	}
	if !payload.Capabilities.PeerICP {
		t.Error("PeerICP should be true")
	}
	if payload.Capabilities.L4Backhaul {
		t.Error("L4Backhaul should be false for non-whitelisted peer")
	}
	if payload.Capabilities.RelayProvider {
		t.Error("RelayProvider should be false (auto-detected later)")
	}
	if payload.BandwidthQuota != 50_000_000 {
		t.Errorf("BandwidthQuota = %d, want 50_000_000", payload.BandwidthQuota)
	}
	if payload.Exp <= payload.Iat {
		t.Errorf("Exp (%d) should be > Iat (%d)", payload.Exp, payload.Iat)
	}
	if payload.Exp-payload.Iat != 3600 {
		t.Errorf("Exp - Iat = %d, want 3600 (1h)", payload.Exp-payload.Iat)
	}
}

// ---------------------------------------------------------------------------
// Test: Expired JWT
// ---------------------------------------------------------------------------

func TestJWT_Expired(t *testing.T) {
	cpPub, cpPriv, err := sjwt.GenerateEd25519Key()
	if err != nil {
		t.Fatalf("generate keys: %v", err)
	}
	_, nodePriv, err := sjwt.GenerateEd25519Key()
	if err != nil {
		t.Fatalf("generate node key: %v", err)
	}
	nodePeerID, err := sjwt.GeneratePeerID(nodePriv)
	if err != nil {
		t.Fatalf("generate peer ID: %v", err)
	}

	// Manually craft an expired JWT
	payload := types.NodeJWTPayload{
		NodeID: sjwt.GenerateNodeID(),
		PeerID: nodePeerID,
		Capabilities: types.NodeCapabilities{
			Edge:    true,
			PeerICP: true,
		},
		BandwidthQuota: 50_000_000,
		Iat:            time.Now().Add(-2 * time.Hour).Unix(),
		Exp:            time.Now().Add(-1 * time.Hour).Unix(), // expired 1h ago
	}
	jwtStr, err := sjwt.SignJWT(payload, cpPriv)
	if err != nil {
		t.Fatalf("signJWT: %v", err)
	}

	_, err = sjwt.VerifyJWT(jwtStr, cpPub, nodePeerID)
	if err == nil {
		t.Fatal("expected error for expired JWT, got nil")
	}
	if err != sjwt.ErrJWTExpired {
		t.Errorf("expected sjwt.ErrJWTExpired, got %v", err)
	}
}

// ---------------------------------------------------------------------------
// Test: Invalid Signature (tampered)
// ---------------------------------------------------------------------------

func TestJWT_InvalidSignature(t *testing.T) {
	cpPub, cpPriv, err := sjwt.GenerateEd25519Key()
	if err != nil {
		t.Fatalf("generate control plane keys: %v", err)
	}
	_, nodePriv, err := sjwt.GenerateEd25519Key()
	if err != nil {
		t.Fatalf("generate node key: %v", err)
	}
	nodePeerID, err := sjwt.GeneratePeerID(nodePriv)
	if err != nil {
		t.Fatalf("generate peer ID: %v", err)
	}

	svc := cpjwt.NewJWTService(cpPriv, cpjwt.NewPeerIdSet(), cpjwt.NewRateLimiter(cpjwt.DefaultRateLimitInterval),
		cpjwt.NewAuditLog(nil), config.JWTPolicyConfig{})
	signedPeerID := sjwt.SignPeerID(nodePriv, nodePeerID)
	req := types.JWTRequest{PeerID: nodePeerID, SignedPeerID: signedPeerID}
	resp, _ := svc.HandleJWTRequest(req, "127.0.0.1")

	// Tamper with the signature portion: flip a byte in the middle
	raw := string(resp.JWT)
	midSigStart := len(raw) - len(raw)/3
	flipped := raw[:midSigStart] + string(raw[midSigStart]^0xFF) + raw[midSigStart+1:]

	_, err = sjwt.VerifyJWT(types.CapabilityJWT(flipped), cpPub, nodePeerID)
	if err == nil {
		t.Fatal("expected error for tampered JWT, got nil")
	}
	if err != sjwt.ErrJWTBadSignature {
		t.Errorf("expected sjwt.ErrJWTBadSignature, got %v", err)
	}
}

// ---------------------------------------------------------------------------
// Test: PeerID Mismatch
// ---------------------------------------------------------------------------

func TestJWT_PeerIDMismatch(t *testing.T) {
	cpPub, cpPriv, err := sjwt.GenerateEd25519Key()
	if err != nil {
		t.Fatalf("generate control plane keys: %v", err)
	}
	_, nodePrivA, err := sjwt.GenerateEd25519Key()
	if err != nil {
		t.Fatalf("generate node A key: %v", err)
	}
	_, nodePrivB, err := sjwt.GenerateEd25519Key()
	if err != nil {
		t.Fatalf("generate node B key: %v", err)
	}
	peerIDA, err := sjwt.GeneratePeerID(nodePrivA)
	if err != nil {
		t.Fatalf("generate peer ID A: %v", err)
	}
	peerIDB, err := sjwt.GeneratePeerID(nodePrivB)
	if err != nil {
		t.Fatalf("generate peer ID B: %v", err)
	}

	svc := cpjwt.NewJWTService(cpPriv, cpjwt.NewPeerIdSet(), cpjwt.NewRateLimiter(cpjwt.DefaultRateLimitInterval),
		cpjwt.NewAuditLog(nil), config.JWTPolicyConfig{})
	signedPeerID := sjwt.SignPeerID(nodePrivA, peerIDA)
	req := types.JWTRequest{PeerID: peerIDA, SignedPeerID: signedPeerID}
	resp, err := svc.HandleJWTRequest(req, "127.0.0.1")
	if err != nil {
		t.Fatalf("HandleJWTRequest: %v", err)
	}

	// Try to verify JWT for peerIDA as if it belongs to peerIDB
	_, err = sjwt.VerifyJWT(resp.JWT, cpPub, peerIDB)
	if err == nil {
		t.Fatal("expected error for PeerID mismatch, got nil")
	}
	if err != sjwt.ErrPeerIDMismatch {
		t.Errorf("expected sjwt.ErrPeerIDMismatch, got %v", err)
	}
}

// ---------------------------------------------------------------------------
// Test: L4 Whitelist
// ---------------------------------------------------------------------------

func TestJWT_L4Whitelist(t *testing.T) {
	cpPub, cpPriv, err := sjwt.GenerateEd25519Key()
	if err != nil {
		t.Fatalf("generate control plane keys: %v", err)
	}
	_, nodePriv, err := sjwt.GenerateEd25519Key()
	if err != nil {
		t.Fatalf("generate node key: %v", err)
	}
	nodePeerID, err := sjwt.GeneratePeerID(nodePriv)
	if err != nil {
		t.Fatalf("generate peer ID: %v", err)
	}

	whitelist := cpjwt.NewPeerIdSet()
	// Don't whitelist yet — test non-whitelisted
	svc := cpjwt.NewJWTService(cpPriv, whitelist, cpjwt.NewRateLimiter(cpjwt.DefaultRateLimitInterval),
		cpjwt.NewAuditLog(nil), config.JWTPolicyConfig{})
	signedPeerID := sjwt.SignPeerID(nodePriv, nodePeerID)
	req := types.JWTRequest{PeerID: nodePeerID, SignedPeerID: signedPeerID}
	resp1, err := svc.HandleJWTRequest(req, "127.0.0.1")
	if err != nil {
		t.Fatalf("HandleJWTRequest (non-whitelisted): %v", err)
	}
	payload1, err := sjwt.VerifyJWT(resp1.JWT, cpPub, nodePeerID)
	if err != nil {
		t.Fatalf("VerifyJWT (non-whitelisted): %v", err)
	}
	if payload1.Capabilities.L4Backhaul {
		t.Error("non-whitelisted peer should have L4Backhaul=false")
	}

	// Now whitelist the peer and request again (use different rate limit to bypass)
	whitelist.Add(nodePeerID)
	svc2 := cpjwt.NewJWTService(cpPriv, whitelist, cpjwt.NewRateLimiter(cpjwt.DefaultRateLimitInterval),
		cpjwt.NewAuditLog(nil), config.JWTPolicyConfig{})
	resp2, err := svc2.HandleJWTRequest(req, "127.0.0.2")
	if err != nil {
		t.Fatalf("HandleJWTRequest (whitelisted): %v", err)
	}
	payload2, err := sjwt.VerifyJWT(resp2.JWT, cpPub, nodePeerID)
	if err != nil {
		t.Fatalf("VerifyJWT (whitelisted): %v", err)
	}
	if !payload2.Capabilities.L4Backhaul {
		t.Error("whitelisted peer should have L4Backhaul=true")
	}
}

// ---------------------------------------------------------------------------
// Test: Rate Limit
// ---------------------------------------------------------------------------

func TestJWT_RateLimit(t *testing.T) {
	_, cpPriv, err := sjwt.GenerateEd25519Key()
	if err != nil {
		t.Fatalf("generate keys: %v", err)
	}
	_, nodePriv, err := sjwt.GenerateEd25519Key()
	if err != nil {
		t.Fatalf("generate node key: %v", err)
	}
	nodePeerID, err := sjwt.GeneratePeerID(nodePriv)
	if err != nil {
		t.Fatalf("generate peer ID: %v", err)
	}

	rl := cpjwt.NewRateLimiter(1 * time.Hour) // 1h interval
	svc := cpjwt.NewJWTService(cpPriv, cpjwt.NewPeerIdSet(), rl, cpjwt.NewAuditLog(nil), config.JWTPolicyConfig{})
	signedPeerID := sjwt.SignPeerID(nodePriv, nodePeerID)
	req := types.JWTRequest{PeerID: nodePeerID, SignedPeerID: signedPeerID}

	// First request: allowed
	_, err = svc.HandleJWTRequest(req, "192.168.1.1")
	if err != nil {
		t.Fatalf("first request should succeed: %v", err)
	}

	// Second request from same IP: rate limited
	_, err = svc.HandleJWTRequest(req, "192.168.1.1")
	if err == nil {
		t.Fatal("second request from same IP should be rate limited")
	}
	if err != sjwt.ErrRateLimited {
		t.Errorf("expected sjwt.ErrRateLimited, got %v", err)
	}

	// Different IP: still allowed
	_, err = svc.HandleJWTRequest(req, "192.168.1.2")
	if err != nil {
		t.Fatalf("request from different IP should succeed: %v", err)
	}
}

// ---------------------------------------------------------------------------
// Test: JWT Dedup rules
// ---------------------------------------------------------------------------

func TestJWT_Dedup(t *testing.T) {
	cpPub, cpPriv, err := sjwt.GenerateEd25519Key()
	if err != nil {
		t.Fatalf("generate control plane keys: %v", err)
	}
	_, nodePriv, err := sjwt.GenerateEd25519Key()
	if err != nil {
		t.Fatalf("generate node key: %v", err)
	}

	psp, _ := newTestPSK("test-dedup-net")
	host1, err := createLibp2pHost(nodePriv, psp)
	if err != nil {
		t.Fatalf("create host1: %v", err)
	}
	defer host1.Close()

	_, host2Priv, err := sjwt.GenerateEd25519Key()
	if err != nil {
		t.Fatalf("generate host2 key: %v", err)
	}
	host2, err := createLibp2pHost(host2Priv, psp)
	if err != nil {
		t.Fatalf("create host2: %v", err)
	}
	defer host2.Close()

	// Connect hosts
	if err := host2.Connect(context.Background(), peer.AddrInfo{ID: host1.ID(), Addrs: host1.Addrs()}); err != nil {
		t.Fatalf("connect: %v", err)
	}

	store := newMemPeerStore()
	verifier := NewJWTVerifier(cpPub)

	type dedupTestCase struct {
		name         string
		existingExp  int64
		newExpOffset time.Duration // relative to now
		wantAccepted bool
		wantError    error
	}

	now := time.Now()
	cases := []dedupTestCase{
		{
			name:         "reject stale (new Exp < existing)",
			existingExp:  now.Add(1 * time.Hour).Unix(),
			newExpOffset: 30 * time.Minute,
			wantAccepted: false,
			wantError:    sjwt.ErrJWTStaleOrDuplicate,
		},
		{
			name:         "accept fresher (new Exp > existing)",
			existingExp:  now.Add(30 * time.Minute).Unix(),
			newExpOffset: 1 * time.Hour,
			wantAccepted: true,
		},
		{
			name:         "skip duplicate (new Exp == existing)",
			existingExp:  now.Add(1 * time.Hour).Unix(),
			newExpOffset: 1 * time.Hour,
			wantAccepted: true, // returns existing entry without error, but JWT unchanged
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			// Setup handler for host1 (the "server" receiving pushes)
			handlerCalled := make(chan struct{}, 1)
			host1.SetStreamHandler(JWTRefreshProtocolID, func(s network.Stream) {
				_, err := HandleJWTPush(s, verifier, store)
				if err != nil && tc.wantError != nil {
					if err.Error() != tc.wantError.Error() {
						t.Errorf("HandleJWTPush error: %v, want %v", err, tc.wantError)
					}
				} else if err != nil && tc.wantAccepted {
					t.Errorf("HandleJWTPush unexpected error: %v", err)
				} else if err == nil && !tc.wantAccepted && tc.wantError != nil {
					t.Errorf("HandleJWTPush should have returned error %v", tc.wantError)
				}
				handlerCalled <- struct{}{}
			})

			// Seed store with existing entry
			if tc.existingExp > 0 {
				store.Put(types.PeerStoreEntry{
					PeerID: types.PeerId(host2.ID().String()),
					JWTExp: tc.existingExp,
					JWT:    types.CapabilityJWT("old-jwt-" + tc.name),
				})
			}

			// Build JWT with requested exp
			payload := types.NodeJWTPayload{
				NodeID: sjwt.GenerateNodeID(),
				PeerID: types.PeerId(host2.ID().String()),
				Capabilities: types.NodeCapabilities{
					Edge:    true,
					PeerICP: true,
				},
				BandwidthQuota: 50_000_000,
				Iat:            now.Unix(),
				Exp:            now.Add(tc.newExpOffset).Unix(),
			}
			jwtStr, err := sjwt.SignJWT(payload, cpPriv)
			if err != nil {
				t.Fatalf("signJWT: %v", err)
			}

			// Push from host2 to host1
			if err := PushJWT(host2, host1.ID(), jwtStr); err != nil {
				t.Fatalf("PushJWT: %v", err)
			}

			select {
			case <-handlerCalled:
			case <-time.After(5 * time.Second):
				t.Fatal("handler never called")
			}
		})
	}
}

// ---------------------------------------------------------------------------
// Test: Push Protocol end-to-end
// ---------------------------------------------------------------------------

func TestJWT_PushProtocol(t *testing.T) {
	cpPub, cpPriv, err := sjwt.GenerateEd25519Key()
	if err != nil {
		t.Fatalf("generate control plane keys: %v", err)
	}

	_, nodeAPriv, err := sjwt.GenerateEd25519Key()
	if err != nil {
		t.Fatalf("generate node A key: %v", err)
	}
	_, nodeBPriv, err := sjwt.GenerateEd25519Key()
	if err != nil {
		t.Fatalf("generate node B key: %v", err)
	}

	psp, err := newTestPSK("test-push-protocol-net")
	if err != nil {
		t.Fatalf("newTestPSK: %v", err)
	}

	hostA, err := createLibp2pHost(nodeAPriv, psp)
	if err != nil {
		t.Fatalf("create hostA: %v", err)
	}
	defer hostA.Close()

	hostB, err := createLibp2pHost(nodeBPriv, psp)
	if err != nil {
		t.Fatalf("create hostB: %v", err)
	}
	defer hostB.Close()

	// Connect A → B (necessary for PushJWT, which opens a stream)
	if err := hostA.Connect(context.Background(), peer.AddrInfo{ID: hostB.ID(), Addrs: hostB.Addrs()}); err != nil {
		t.Fatalf("connect A→B: %v", err)
	}

	// B's peer store and verifier
	storeB := newMemPeerStore()
	verifierB := NewJWTVerifier(cpPub)

	// B registers stream handler
	receivedCh := make(chan *types.PeerStoreEntry, 1)
	hostB.SetStreamHandler(JWTRefreshProtocolID, func(s network.Stream) {
		entry, err := HandleJWTPush(s, verifierB, storeB)
		if err != nil {
			t.Errorf("HandleJWTPush error: %v", err)
			return
		}
		receivedCh <- entry
	})

	// Create JWT service for A (simulating control plane)
	svc := cpjwt.NewJWTService(cpPriv, cpjwt.NewPeerIdSet(), cpjwt.NewRateLimiter(cpjwt.DefaultRateLimitInterval),
		cpjwt.NewAuditLog(nil), config.JWTPolicyConfig{})
	aPeerID := types.PeerId(hostA.ID().String())
	signed := sjwt.SignPeerID(nodeAPriv, aPeerID)
	req := types.JWTRequest{PeerID: aPeerID, SignedPeerID: signed}
	resp, err := svc.HandleJWTRequest(req, "127.0.0.1")
	if err != nil {
		t.Fatalf("HandleJWTRequest for A: %v", err)
	}

	// Verify it's valid
	payload, err := sjwt.VerifyJWT(resp.JWT, cpPub, aPeerID)
	if err != nil {
		t.Fatalf("self-verify JWT: %v", err)
	}

	// A pushes JWT to B
	if err := PushJWT(hostA, hostB.ID(), resp.JWT); err != nil {
		t.Fatalf("PushJWT A→B: %v", err)
	}

	select {
	case entry := <-receivedCh:
		if entry.PeerID != aPeerID {
			t.Errorf("PeerID = %q, want %q", entry.PeerID, aPeerID)
		}
		if entry.JWTExp != payload.Exp {
			t.Errorf("JWTExp = %d, want %d", entry.JWTExp, payload.Exp)
		}
		if !entry.Capabilities.Edge {
			t.Error("Edge should be true")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("timeout waiting for JWT push handler")
	}

	// Verify B's store was updated
	stored, err := storeB.Get(aPeerID)
	if err != nil {
		t.Fatalf("storeB.Get: %v", err)
	}
	if stored.JWT != resp.JWT {
		t.Error("stored JWT should match pushed JWT")
	}
}

// ---------------------------------------------------------------------------
// Test: JWTSetupIntegration (simulates the full flow with an HTTP server)
// ---------------------------------------------------------------------------

func TestJWT_ClientIntegration(t *testing.T) {
	cpPub, cpPriv, err := sjwt.GenerateEd25519Key()
	if err != nil {
		t.Fatalf("generate cp keys: %v", err)
	}
	_, nodePriv, err := sjwt.GenerateEd25519Key()
	if err != nil {
		t.Fatalf("generate node key: %v", err)
	}
	nodePeerID, err := sjwt.GeneratePeerID(nodePriv)
	if err != nil {
		t.Fatalf("generate peer ID: %v", err)
	}

	// Start a mock HTTP server acting as the control plane JWT endpoint
	svc := cpjwt.NewJWTService(cpPriv, cpjwt.NewPeerIdSet(), cpjwt.NewRateLimiter(cpjwt.DefaultRateLimitInterval),
		cpjwt.NewAuditLog(nil), config.JWTPolicyConfig{})

	mux := http.NewServeMux()
	mux.HandleFunc("/v1/node/jwt", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		body, _ := io.ReadAll(r.Body)
		var req types.JWTRequest
		if err := json.Unmarshal(body, &req); err != nil {
			http.Error(w, "bad request", http.StatusBadRequest)
			return
		}
		resp, err := svc.HandleJWTRequest(req, r.RemoteAddr)
		if err != nil {
			http.Error(w, err.Error(), http.StatusForbidden)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(resp)
	})

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	go http.Serve(listener, mux)

	endpoint := "http://" + listener.Addr().String() + "/v1/node/jwt"

	// Client
	client := NewJWTClient(nodePriv, nodePeerID, endpoint, types.NodeCapabilities{
		Edge:    true,
		PeerICP: true,
	})
	ctx := context.Background()
	resp, err := client.RequestJWT(ctx)
	if err != nil {
		t.Fatalf("RequestJWT: %v", err)
	}

	// Verify the returned JWT
	payload, err := sjwt.VerifyJWT(resp.JWT, cpPub, nodePeerID)
	if err != nil {
		t.Fatalf("VerifyJWT: %v", err)
	}
	if payload.PeerID != nodePeerID {
		t.Errorf("PeerID = %q, want %q", payload.PeerID, nodePeerID)
	}
	if client.CurrentJWT() != resp.JWT {
		t.Error("CurrentJWT should match returned JWT")
	}
}

// ---------------------------------------------------------------------------
// Test: Client retry with degraded mode
// ---------------------------------------------------------------------------

func TestJWT_ClientRetryDegraded(t *testing.T) {
	_, nodePriv, err := sjwt.GenerateEd25519Key()
	if err != nil {
		t.Fatalf("generate node key: %v", err)
	}
	nodePeerID, err := sjwt.GeneratePeerID(nodePriv)
	if err != nil {
		t.Fatalf("generate peer ID: %v", err)
	}

	client := NewJWTClient(nodePriv, nodePeerID, "http://127.0.0.1:19999/v1/node/jwt", types.NodeCapabilities{
		Edge:    true,
		PeerICP: true,
	})
	client.retryBackoff = 1 * time.Millisecond
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	_, err = client.RequestJWTWithRetry(ctx)
	if err == nil {
		t.Fatal("expected error from retry exhaustion")
	}
	if !client.IsDegraded() {
		t.Error("client should be in degraded mode after retry exhaustion")
	}
}

// ---------------------------------------------------------------------------
// Test: RateLimiter unit tests
// ---------------------------------------------------------------------------

func TestRateLimiter_Allow(t *testing.T) {
	rl := cpjwt.NewRateLimiter(cpjwt.DefaultRateLimitInterval)

	if !rl.Allow("10.0.0.1") {
		t.Error("first request should be allowed")
	}
	if rl.Allow("10.0.0.1") {
		t.Error("second request within interval should be denied")
	}

	// Quick interval for test
	rl2 := cpjwt.NewRateLimiter(1 * time.Millisecond)
	rl2.Allow("10.0.0.2")
	time.Sleep(2 * time.Millisecond)
	if !rl2.Allow("10.0.0.2") {
		t.Error("request after interval should be allowed")
	}
}

// ---------------------------------------------------------------------------
// Test: PeerIdSet
// ---------------------------------------------------------------------------

func TestPeerIdSet_Basic(t *testing.T) {
	s := cpjwt.NewPeerIdSet()
	if s.Contains("QmTest1") {
		t.Error("empty set should not contain any peer")
	}
	s.Add("QmTest1")
	if !s.Contains("QmTest1") {
		t.Error("set should contain added peer")
	}
	if s.Contains("QmTest2") {
		t.Error("set should not contain non-added peer")
	}
	s.Remove("QmTest1")
	if s.Contains("QmTest1") {
		t.Error("set should not contain removed peer")
	}
}

// ---------------------------------------------------------------------------
// Test: AuditLog
// ---------------------------------------------------------------------------

func TestAuditLog_Log(t *testing.T) {
	var buf bytes.Buffer
	al := cpjwt.NewAuditLog(&buf)
	al.Log("QmTest", "192.168.1.1", true, 50_000_000, time.Now().Unix()+3600)

	line := buf.String()
	if line == "" {
		t.Fatal("audit log should have produced output")
	}
	line = line[:len(line)-1]
	var entry cpjwt.AuditEntry
	if err := json.Unmarshal([]byte(line), &entry); err != nil {
		t.Fatalf("unmarshal audit entry: %v", err)
	}
	if entry.PeerID != "QmTest" {
		t.Errorf("PeerID = %q", entry.PeerID)
	}
	if !entry.L4Whitelisted {
		t.Error("L4Whitelisted should be true")
	}
}

func TestAuditLog_NilWriter(t *testing.T) {
	al := cpjwt.NewAuditLog(nil)
	al.Log("QmTest", "10.0.0.1", false, 50_000_000, time.Now().Unix()+3600)
}

// ---------------------------------------------------------------------------
// Test: RequestJWT includes declared_capabilities in request body
// ---------------------------------------------------------------------------

func TestJWT_ClientSendsDeclaredCapabilities(t *testing.T) {
	cpPub, cpPriv, err := sjwt.GenerateEd25519Key()
	if err != nil {
		t.Fatalf("generate cp keys: %v", err)
	}
	_, nodePriv, err := sjwt.GenerateEd25519Key()
	if err != nil {
		t.Fatalf("generate node key: %v", err)
	}
	nodePeerID, err := sjwt.GeneratePeerID(nodePriv)
	if err != nil {
		t.Fatalf("generate peer ID: %v", err)
	}

	// Real CP-side service so the JWT we get back is verifiable.
	policy := config.JWTPolicyConfig{
		TTL:                  "1h",
		RefreshBeforeSeconds: 300,
		BandwidthQuotaBytes:  50_000_000,
		DefaultCapabilities:  config.JWTPolicyDefaultCapabilities{Edge: true, PeerICP: true, RelayProvider: true},
	}
	svc := cpjwt.NewJWTService(cpPriv, cpjwt.NewPeerIdSet(),
		cpjwt.NewRateLimiter(cpjwt.DefaultRateLimitInterval),
		cpjwt.NewAuditLog(nil), policy)

	type capturedReq struct {
		req types.JWTRequest
		ok  bool
	}
	capturedCh := make(chan capturedReq, 1)

	mux := http.NewServeMux()
	mux.HandleFunc("/v1/node/jwt", func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		var req types.JWTRequest
		if err := json.Unmarshal(body, &req); err != nil {
			http.Error(w, "bad request", http.StatusBadRequest)
			return
		}
		capturedCh <- capturedReq{req: req, ok: true}
		resp, err := svc.HandleJWTRequest(req, r.RemoteAddr)
		if err != nil {
			http.Error(w, err.Error(), http.StatusForbidden)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(resp)
	})

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	go http.Serve(listener, mux)
	defer listener.Close()

	endpoint := "http://" + listener.Addr().String() + "/v1/node/jwt"

	// Declare a distinctive capability set so we can assert it round-trips.
	declared := types.NodeCapabilities{
		Edge:          true,
		L4Backhaul:    true,
		RelayProvider: false,
		PeerICP:       true,
	}
	client := NewJWTClient(nodePriv, nodePeerID, endpoint, declared)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	resp, err := client.RequestJWT(ctx)
	if err != nil {
		t.Fatalf("RequestJWT: %v", err)
	}

	select {
	case cap := <-capturedCh:
		if !cap.ok || cap.req.DeclaredCapabilities == nil {
			t.Fatal("captured request missing DeclaredCapabilities")
		}
		got := *cap.req.DeclaredCapabilities
		if got != declared {
			t.Errorf("declared = %+v, want %+v", got, declared)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for request capture")
	}

	// Sanity: the granted JWT reflects declared ∩ default policy.
	// declared={Edge,L4Backhaul,PeerICP}, default={Edge,PeerICP,RelayProvider}
	// → granted={Edge,PeerICP} (L4 ignored — whitelist only; Relay declared=false).
	payload, err := sjwt.VerifyJWT(resp.JWT, cpPub, nodePeerID)
	if err != nil {
		t.Fatalf("VerifyJWT: %v", err)
	}
	if !payload.Capabilities.Edge || !payload.Capabilities.PeerICP {
		t.Errorf("granted Edge=%v PeerICP=%v, want both true", payload.Capabilities.Edge, payload.Capabilities.PeerICP)
	}
	if payload.Capabilities.RelayProvider {
		t.Error("RelayProvider should be false (declared=false)")
	}
	if payload.Capabilities.L4Backhaul {
		t.Error("L4Backhaul should be false (declared ignored, not whitelisted)")
	}

	if client.CurrentJWT() != resp.JWT {
		t.Error("CurrentJWT should match returned JWT")
	}
}

// ---------------------------------------------------------------------------
// Test: CurrentJWT is concurrent-safe and empty before first success
// ---------------------------------------------------------------------------

func TestJWT_CurrentJWT_EmptyInitially(t *testing.T) {
	_, nodePriv, err := sjwt.GenerateEd25519Key()
	if err != nil {
		t.Fatalf("generate node key: %v", err)
	}
	nodePeerID, err := sjwt.GeneratePeerID(nodePriv)
	if err != nil {
		t.Fatalf("generate peer ID: %v", err)
	}
	client := NewJWTClient(nodePriv, nodePeerID, "http://127.0.0.1:1/v1/node/jwt", types.NodeCapabilities{})

	if got := client.CurrentJWT(); got != "" {
		t.Errorf("CurrentJWT before first success = %q, want empty", got)
	}
}

// ---------------------------------------------------------------------------
// Test: Refresh loop fires ≥2 times with short interval + short TTL
// ---------------------------------------------------------------------------

// runRefreshLoopForTesting mirrors cmd/edge-node/main.go:runJWTRefreshLoop but
// is exposed here so the test can drive it without importing main. We instead
// re-implement the minimal loop using the public JWTClient API and rely on the
// server side to control TTL via the policy.
func runRefreshLoopForTesting(
	ctx context.Context,
	client *JWTClient,
	refreshInterval time.Duration,
) {
	for {
		select {
		case <-ctx.Done():
			return
		case <-time.After(refreshInterval):
		}
		_, _ = client.RequestJWT(ctx)
	}
}

func TestJWT_RefreshLoopFiresMultipleTimes(t *testing.T) {
	_, cpPriv, err := sjwt.GenerateEd25519Key()
	if err != nil {
		t.Fatalf("generate cp keys: %v", err)
	}
	_, nodePriv, err := sjwt.GenerateEd25519Key()
	if err != nil {
		t.Fatalf("generate node key: %v", err)
	}
	nodePeerID, err := sjwt.GeneratePeerID(nodePriv)
	if err != nil {
		t.Fatalf("generate peer ID: %v", err)
	}

	// Short TTL so the JWT is meaningfully short-lived; the loop refreshes
	// purely on a short interval here (no expiry-driven shortening needed).
	policy := config.JWTPolicyConfig{
		TTL:                  "200ms",
		RefreshBeforeSeconds: 0,
		BandwidthQuotaBytes:  50_000_000,
		DefaultCapabilities:  config.JWTPolicyDefaultCapabilities{Edge: true, PeerICP: true},
	}
	svc := cpjwt.NewJWTService(cpPriv, cpjwt.NewPeerIdSet(),
		// Very short rate-limit interval so successive refreshes aren't blocked.
		cpjwt.NewRateLimiter(1*time.Millisecond),
		cpjwt.NewAuditLog(nil), policy)

	var (
		mu       sync.Mutex
		reqCount int
	)
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/node/jwt", func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		var req types.JWTRequest
		if err := json.Unmarshal(body, &req); err != nil {
			http.Error(w, "bad request", http.StatusBadRequest)
			return
		}
		resp, err := svc.HandleJWTRequest(req, r.RemoteAddr)
		if err != nil {
			http.Error(w, err.Error(), http.StatusForbidden)
			return
		}
		mu.Lock()
		reqCount++
		mu.Unlock()
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(resp)
	})

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	go http.Serve(listener, mux)
	defer listener.Close()

	endpoint := "http://" + listener.Addr().String() + "/v1/node/jwt"
	client := NewJWTClient(nodePriv, nodePeerID, endpoint, types.NodeCapabilities{Edge: true, PeerICP: true})

	// Initial request.
	ctx, cancel := context.WithTimeout(context.Background(), 700*time.Millisecond)
	defer cancel()
	if _, err := client.RequestJWT(ctx); err != nil {
		t.Fatalf("initial RequestJWT: %v", err)
	}

	// Drive the refresh loop on a 50ms cadence for ~500ms → expect ≥2 refreshes
	// (initial + 2+ from the loop = ≥3 total requests).
	go runRefreshLoopForTesting(ctx, client, 50*time.Millisecond)

	<-ctx.Done()

	mu.Lock()
	got := reqCount
	mu.Unlock()
	if got < 3 {
		t.Errorf("expected ≥3 total requests (initial + ≥2 refreshes), got %d", got)
	}
}

// ---------------------------------------------------------------------------
// Test: Refresh loop tolerates server 500 (does not exit / no panic)
// ---------------------------------------------------------------------------

func TestJWT_RefreshLoopToleratesServerErrors(t *testing.T) {
	_, nodePriv, err := sjwt.GenerateEd25519Key()
	if err != nil {
		t.Fatalf("generate node key: %v", err)
	}
	nodePeerID, err := sjwt.GeneratePeerID(nodePriv)
	if err != nil {
		t.Fatalf("generate peer ID: %v", err)
	}

	var (
		mu       sync.Mutex
		reqCount int
	)
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/node/jwt", func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		reqCount++
		mu.Unlock()
		// Always 500 — simulates a down CP.
		http.Error(w, "internal error", http.StatusInternalServerError)
	})

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	go http.Serve(listener, mux)
	defer listener.Close()

	endpoint := "http://" + listener.Addr().String() + "/v1/node/jwt"
	client := NewJWTClient(nodePriv, nodePeerID, endpoint, types.NodeCapabilities{})

	ctx, cancel := context.WithTimeout(context.Background(), 250*time.Millisecond)
	defer cancel()
	// Should NOT panic on errors; should keep retrying until ctx cancels.
	runRefreshLoopForTesting(ctx, client, 30*time.Millisecond)

	mu.Lock()
	got := reqCount
	mu.Unlock()
	if got < 2 {
		t.Errorf("expected ≥2 requests despite 500s, got %d", got)
	}
}
