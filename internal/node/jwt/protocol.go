package jwt

import (
	"bufio"
	"context"
	"crypto/ed25519"
	"fmt"
	"io"

	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/peer"

	sjwt "github.com/shlande/mediaworker/internal/shared/jwt"
	"github.com/shlande/mediaworker/internal/types"
)

// ---------------------------------------------------------------------------
// Protocol ID
// ---------------------------------------------------------------------------

// JWTRefreshProtocolID is the custom stream protocol used to propagate JWT
// refreshes between peers. We use a custom protocol rather than IdentifyPush
// because IdentifyPush cannot push custom data.
const JWTRefreshProtocolID = "/edge/jwt-refresh/1.0.0"

// ---------------------------------------------------------------------------
// JWTVerifier
// ---------------------------------------------------------------------------

// JWTVerifier verifies a CapabilityJWT using the control plane's public key.
type JWTVerifier struct {
	controlPlanePubKey ed25519.PublicKey
}

// NewJWTVerifier creates a JWTVerifier.
func NewJWTVerifier(controlPlanePubKey ed25519.PublicKey) *JWTVerifier {
	return &JWTVerifier{controlPlanePubKey: controlPlanePubKey}
}

// Verify checks a JWT string: splits, base64url-decodes, verifies Ed25519
// signature, checks Exp (no grace period). Does NOT check PeerID binding — the
// caller handles that by comparing payload.PeerID against the stream's remote peer.
func (v *JWTVerifier) Verify(jwtStr types.CapabilityJWT) (*types.NodeJWTPayload, error) {
	return sjwt.VerifyJWTAnyPeerID(jwtStr, v.controlPlanePubKey)
}

// ---------------------------------------------------------------------------
// PushJWT — sender side
// ---------------------------------------------------------------------------

// PushJWT opens a /edge/jwt-refresh/1.0.0 stream to the given peer and writes
// the JWT as a single line, then closes the write side.
func PushJWT(ctx context.Context, h host.Host, peerID peer.ID, jwt types.CapabilityJWT) error {
	ctx = network.WithNoDial(ctx, "jwt-push")
	s, err := h.NewStream(ctx, peerID, JWTRefreshProtocolID)
	if err != nil {
		return fmt.Errorf("jwt push: open stream to %s: %w", peerID.ShortString(), err)
	}
	defer func() { _ = s.Close() }()

	if _, err := fmt.Fprintln(s, jwt); err != nil {
		return fmt.Errorf("jwt push: write: %w", err)
	}

	return nil
}

// ---------------------------------------------------------------------------
// HandleJWTPush — receiver side stream handler
// ---------------------------------------------------------------------------

// PeerStoreWriter is the interface a PeerStore must satisfy to accept JWT updates.
type PeerStoreWriter interface {
	Get(peerID types.PeerId) (types.PeerStoreEntry, error)
	Put(entry types.PeerStoreEntry) error
}

// HandleJWTPush is a stream handler for /edge/jwt-refresh/1.0.0. It reads a
// single line (the JWT) from the stream, verifies it, applies dedup logic, and
// returns the updated PeerStoreEntry (or the existing one if the JWT was
// rejected/skipped).
//
// Dedup rules:
//   - Reject if payload.Exp <= existingEntry.JWTExp (stale JWT)
//   - Accept if payload.Exp > existingEntry.JWTExp (fresher JWT)
//   - Skip if payload.Exp == existingEntry.JWTExp (duplicate, no-op)
func HandleJWTPush(stream network.Stream, verifier *JWTVerifier, store PeerStoreWriter) (*types.PeerStoreEntry, error) {
	defer func() { _ = stream.Close() }()

	// Read a single line (JWT string)
	line, err := bufio.NewReader(stream).ReadString('\n')
	if err != nil && err != io.EOF {
		return nil, fmt.Errorf("jwt push handler: read: %w", err)
	}
	jwtStr := types.CapabilityJWT(line)
	// Trim trailing newline
	if len(jwtStr) > 0 && jwtStr[len(jwtStr)-1] == '\n' {
		jwtStr = jwtStr[:len(jwtStr)-1]
	}

	// Verify the JWT
	payload, err := verifier.Verify(jwtStr)
	if err != nil {
		return nil, fmt.Errorf("jwt push handler: verify: %w", err)
	}

	// Get remote peer ID from stream
	remotePeer := stream.Conn().RemotePeer()
	remotePeerID := types.PeerId(remotePeer.String())

	// Double-check PeerID binding
	if payload.PeerID != remotePeerID {
		return nil, sjwt.ErrPeerIDMismatch
	}

	// Dedup against existing entry
	existingEntry, err := store.Get(remotePeerID)
	if err != nil {
		// No existing entry — accept
		entry := types.PeerStoreEntry{
			PeerID:       remotePeerID,
			JWT:          jwtStr,
			Capabilities: payload.Capabilities,
			JWTExp:       payload.Exp,
		}
		if putErr := store.Put(entry); putErr != nil {
			return nil, putErr
		}
		return &entry, nil
	}

	if payload.Exp < existingEntry.JWTExp {
		return nil, sjwt.ErrJWTStaleOrDuplicate
	}
	if payload.Exp == existingEntry.JWTExp {
		// Duplicate — return existing entry, no update needed
		return &existingEntry, nil
	}

	// Fresher JWT — update
	existingEntry.JWT = jwtStr
	existingEntry.Capabilities = payload.Capabilities
	existingEntry.JWTExp = payload.Exp
	if err := store.Put(existingEntry); err != nil {
		return nil, err
	}
	return &existingEntry, nil
}
