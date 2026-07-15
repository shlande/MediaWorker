// Package libp2phost manages the libp2p host lifecycle.
package libp2phost

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"net/netip"
	"time"

	"github.com/libp2p/go-libp2p/core/control"
	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/core/protocol"
	"github.com/multiformats/go-multiaddr"
	"golang.org/x/time/rate"

	"github.com/shlande/mediaworker/internal/node/jwt"
	"github.com/shlande/mediaworker/internal/node/peerstore"
	sjwt "github.com/shlande/mediaworker/internal/shared/jwt"
	"github.com/shlande/mediaworker/internal/types"
)

// AuthProtocol is the stream protocol ID for JWT verification on first connection.
// Nodes present their JWT on this protocol; the receiver verifies and writes PeerEntryStore.
const AuthProtocol = protocol.ID("/edge/auth/1.0.0")

// EdgeConnectionGater implements connmgr.ConnectionGater for edge-distribution nodes.
// It provides IP-level DDoS defense (rate limiting + optional CIDR allowlist) and
// peer-level access control (JWT expiry + score threshold checks).
type EdgeConnectionGater struct {
	peerStore   *peerstore.PeerEntryStore
	jwtVerifier *jwt.JWTVerifier
	ipLimiter   *rate.Limiter

	// cidrRanges is an optional set of allowed CIDR prefixes.
	// When non-empty, only connections from IPs within these ranges are accepted.
	// An empty slice means no CIDR filtering.
	cidrRanges []netip.Prefix
}

// NewEdgeConnectionGater creates an EdgeConnectionGater.
//
// ipRate is the per-IP connection rate (e.g. 50 connections/second).
// ipBurst is the burst allowance for short spikes.
// cidrRanges is an optional list of allowed CIDR prefixes; nil or empty means no CIDR filtering.
func NewEdgeConnectionGater(
	peerStore *peerstore.PeerEntryStore,
	jwtVerifier *jwt.JWTVerifier,
	ipRate rate.Limit,
	ipBurst int,
	cidrRanges []netip.Prefix,
) *EdgeConnectionGater {
	return &EdgeConnectionGater{
		peerStore:   peerStore,
		jwtVerifier: jwtVerifier,
		ipLimiter:   rate.NewLimiter(ipRate, ipBurst),
		cidrRanges:  cidrRanges,
	}
}

// ─── connmgr.ConnectionGater ─────────────────────────────────────────────────

// InterceptPeerDial allows all outbound peer dials (peer-level filtering is
// handled downstream by InterceptSecured/InterceptUpgraded).
func (g *EdgeConnectionGater) InterceptPeerDial(_ peer.ID) bool {
	return true
}

// InterceptAddrDial allows all outbound address dials (address-level filtering
// is for inbound only).
func (g *EdgeConnectionGater) InterceptAddrDial(_ peer.ID, _ multiaddr.Multiaddr) bool {
	return true
}

// InterceptAccept applies IP-level defense before TCP/QUIC handshake:
// optional CIDR allowlist and per-IP rate limiting.
func (g *EdgeConnectionGater) InterceptAccept(addrs network.ConnMultiaddrs) bool {
	remoteIP := extractIP(addrs)
	if remoteIP == "" {
		// Cannot extract IP — reject conservatively.
		return false
	}

	// CIDR allowlist check (optional).
	if len(g.cidrRanges) > 0 && !ipInCIDRRanges(remoteIP, g.cidrRanges) {
		return false
	}

	// Per-IP rate limiting.
	if !g.ipLimiter.Allow() {
		return false
	}

	return true
}

// InterceptSecured checks the peer after Noise/TLS handshake. It looks up the
// peer in the PeerEntryStore and rejects stale or low-score peers. Unknown
// peers are allowed (they will be required to present a JWT via the auth
// stream protocol later).
func (g *EdgeConnectionGater) InterceptSecured(_ network.Direction, p peer.ID, _ network.ConnMultiaddrs) bool {
	entry, ok := g.peerStore.Get(types.PeerId(p.String()))
	if !ok {
		// Unknown peer — allow. Will require JWT via /edge/auth/1.0.0.
		return true
	}

	if entry.Stale {
		// JWT expired and not refreshed.
		return false
	}

	if entry.Score < peerstore.GraylistThreshold {
		// Malicious or severely degraded peer.
		return false
	}

	return true
}

// InterceptUpgraded runs after multiplexer negotiation. For known peers, it
// checks whether the JWT has expired (no grace period). Unknown peers are
// allowed; they must present a JWT via the auth stream protocol.
func (g *EdgeConnectionGater) InterceptUpgraded(conn network.Conn) (bool, control.DisconnectReason) {
	p := conn.RemotePeer()
	entry, ok := g.peerStore.Get(types.PeerId(p.String()))

	if !ok {
		// Unknown peer — allow. Will require JWT via /edge/auth/1.0.0.
		return true, 0
	}

	now := time.Now().Unix()
	if entry.JWTExp < now {
		// JWT expired with no refresh — mark stale and reject.
		_ = g.peerStore.MarkStale(types.PeerId(p.String()))
		return false, control.DisconnectReason(0)
	}

	return true, 0
}

// ─── /edge/auth/1.0.0 stream protocol ────────────────────────────────────────

// HandleAuth reads a JWT from the stream, verifies it, checks PeerID binding,
// and writes the peer into PeerEntryStore with an initial neutral score.
func HandleAuth(stream network.Stream, gater *EdgeConnectionGater) error {
	defer stream.Close()

	line, err := bufio.NewReader(stream).ReadString('\n')
	if err != nil && err != io.EOF {
		return fmt.Errorf("auth handler: read: %w", err)
	}

	jwtStr := types.CapabilityJWT(line)
	if len(jwtStr) > 0 && jwtStr[len(jwtStr)-1] == '\n' {
		jwtStr = jwtStr[:len(jwtStr)-1]
	}

	payload, err := gater.jwtVerifier.Verify(jwtStr)
	if err != nil {
		return fmt.Errorf("auth handler: verify JWT: %w", err)
	}

	remotePeerID := types.PeerId(stream.Conn().RemotePeer().String())
	if payload.PeerID != remotePeerID {
		return fmt.Errorf("auth handler: %w", sjwt.ErrPeerIDMismatch)
	}

	now := time.Now().Unix()
	if payload.Exp < now {
		return fmt.Errorf("auth handler: %w", sjwt.ErrJWTExpired)
	}

	entry := types.PeerStoreEntry{
		PeerID:       remotePeerID,
		JWT:          jwtStr,
		Capabilities: payload.Capabilities,
		JWTExp:       payload.Exp,
		LastSeen:     now,
		Score:        0, // Initial neutral score.
		Stale:        false,
	}

	if err := gater.peerStore.Put(remotePeerID, entry); err != nil {
		return fmt.Errorf("auth handler: put peer store: %w", err)
	}

	return nil
}

// PresentAuth opens a /edge/auth/1.0.0 stream to target and sends the node's
// JWT. The target verifies the JWT and writes the peer into its PeerEntryStore.
func PresentAuth(ctx context.Context, h host.Host, target peer.ID, localJWT types.CapabilityJWT) error {
	s, err := h.NewStream(ctx, target, AuthProtocol)
	if err != nil {
		return fmt.Errorf("present auth: open stream to %s: %w", target.ShortString(), err)
	}
	defer s.Close()

	if _, err := fmt.Fprintln(s, localJWT); err != nil {
		return fmt.Errorf("present auth: write JWT: %w", err)
	}

	return nil
}

// ─── helpers ─────────────────────────────────────────────────────────────────

// extractIP extracts the remote IP address from a ConnMultiaddrs as a string.
// Returns "" if the remote multiaddr does not contain an IP component.
func extractIP(addrs network.ConnMultiaddrs) string {
	ma := addrs.RemoteMultiaddr()
	c, err := ma.ValueForProtocol(multiaddr.P_IP4)
	if err == nil {
		return c
	}
	c, err = ma.ValueForProtocol(multiaddr.P_IP6)
	if err == nil {
		return c
	}
	return ""
}

// ipInCIDRRanges returns true if ipStr is contained in any of the given CIDR prefixes.
func ipInCIDRRanges(ipStr string, ranges []netip.Prefix) bool {
	addr, err := netip.ParseAddr(ipStr)
	if err != nil {
		return false
	}
	for _, pfx := range ranges {
		if pfx.Contains(addr) {
			return true
		}
	}
	return false
}
