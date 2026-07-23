// Package app provides the reusable edge-node assembly. This file contains
// the eight adapters that bridge internal/node interfaces at the assembly site.
// They are unexported — callers construct them via New() or pass them directly
// into component constructors.
package app

import (
	"context"
	"crypto/ed25519"
	"fmt"
	"io"
	"time"

	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/peer"

	"github.com/shlande/mediaworker/internal/node/backhaul"
	"github.com/shlande/mediaworker/internal/node/cache"
	"github.com/shlande/mediaworker/internal/node/dialassist"
	"github.com/shlande/mediaworker/internal/node/gossippop"
	"github.com/shlande/mediaworker/internal/node/hashring"
	"github.com/shlande/mediaworker/internal/node/icp"
	"github.com/shlande/mediaworker/internal/node/peerstore"
	"github.com/shlande/mediaworker/internal/types"
)

// ----- peerStoreWriterAdapter (main.go adapter #1, for JWT push handler) -----

// peerStoreWriterAdapter adapts *peerstore.PeerEntryStore to
// nodejwt.PeerStoreWriter for HandleJWTPush.
//
// PeerEntryStore.Get returns (types.PeerStoreEntry, bool);
// PeerStoreWriter.Get expects (types.PeerStoreEntry, error).
// The adapter converts "not found" (false) into a non-nil error.
//
// PeerEntryStore.Put expects (peerID, entry);
// PeerStoreWriter.Put expects only the entry (PeerID is inside).
// The adapter extracts PeerID before delegating.
type peerStoreWriterAdapter struct {
	store *peerstore.PeerEntryStore
}

func newPeerStoreWriterAdapter(s *peerstore.PeerEntryStore) *peerStoreWriterAdapter {
	return &peerStoreWriterAdapter{store: s}
}

func (a *peerStoreWriterAdapter) Get(peerID types.PeerId) (types.PeerStoreEntry, error) {
	entry, ok := a.store.Get(peerID)
	if !ok {
		return types.PeerStoreEntry{}, fmt.Errorf("peer not found: %s", peerID)
	}
	return entry, nil
}

func (a *peerStoreWriterAdapter) Put(entry types.PeerStoreEntry) error {
	return a.store.Put(entry.PeerID, entry)
}

// ----- warmCacheBlobStore (main.go adapter #2, for ICP handler registration) -----

// warmCacheBlobStore adapts *cache.WarmCache to icp.BlobStore.
// WarmCache.Get returns ([]byte, bool); BlobStore.Get returns (io.ReadCloser, error).
type warmCacheBlobStore struct {
	wc *cache.WarmCache
}

func (s warmCacheBlobStore) Has(blobHash string) bool {
	if s.wc == nil {
		return false
	}
	return s.wc.Has(blobHash)
}

func (s warmCacheBlobStore) Get(blobHash string) (io.ReadCloser, error) {
	if s.wc == nil {
		return nil, fmt.Errorf("warm cache disabled")
	}
	data, ok := s.wc.Get(blobHash)
	if !ok {
		return nil, fmt.Errorf("blob %s not in warm cache", blobHash)
	}
	return &byteReadCloser{data: data}, nil
}

// ----- byteReadCloser (main.go adapter #3, io.ReadCloser over byte slice) -----

// byteReadCloser is an io.ReadCloser wrapping a byte slice.
type byteReadCloser struct {
	data   []byte
	offset int
}

func (r *byteReadCloser) Read(p []byte) (int, error) {
	if r.offset >= len(r.data) {
		return 0, io.EOF
	}
	n := copy(p, r.data[r.offset:])
	r.offset += n
	return n, nil
}

func (r *byteReadCloser) Close() error {
	r.offset = 0
	return nil
}

// ----- ed25519PeerstoreAdapter (main.go adapter #4, for gossipsub pubkey view) -----

// ed25519PeerstoreAdapter adapts a libp2p host.Host to the narrow interface
// expected by gossippop.HandlePopularityMessage. libp2p's Peerstore.PubKey
// returns the generic ic.PubKey interface; HandlePopularityMessage wants an
// ed25519.PublicKey directly so it can call ed25519.Verify without a type
// assertion. The adapter bridges this by extracting the raw Ed25519 bytes via
// gossippop.Ed25519PubKey (which itself uses peer.ID.ExtractPublicKey).
//
// If a peer.ID does not embed an Ed25519 key (e.g. RSA or Secp256k1 host),
// PubKey returns nil and HandlePopularityMessage drops the message.
type ed25519PeerstoreAdapter struct {
	h host.Host
}

func (a ed25519PeerstoreAdapter) Peerstore() interface {
	PubKey(peer.ID) ed25519.PublicKey
} {
	return ed25519PopKeyView(a)
}

// ----- ed25519PopKeyView (main.go adapter #5, for gossipsub) -----

type ed25519PopKeyView struct {
	h host.Host
}

func (v ed25519PopKeyView) PubKey(id peer.ID) ed25519.PublicKey {
	raw := gossippop.Ed25519PubKey(id)
	if raw == nil {
		return nil
	}
	return ed25519.PublicKey(raw)
}

// ----- peerEntryLookupAdapter (main.go adapter #6, for gossipsub) -----

// peerEntryLookupAdapter adapts *peerstore.PeerEntryStore to the narrow
// gossippop.PeerEntryLookup interface. HandlePopularityMessage consults it to
// discard heat from peers that are either absent from the local store
// (unknown — never JWT-verified) or marked Stale (JWT expired / evicted).
type peerEntryLookupAdapter struct {
	store *peerstore.PeerEntryStore
}

func (a peerEntryLookupAdapter) StaleOrUnknown(peerID types.PeerId) bool {
	entry, ok := a.store.Get(peerID)
	if !ok {
		return true
	}
	return entry.Stale
}

// ----- backhaulWarmCache (main.go adapter #7, for BackhaulManager cache RW) -----

// backhaulWarmCache adapts *cache.WarmCache to backhaul.CacheReader and
// backhaul.CacheWriter (via its Put method). The adapter is necessary because
// backhaul.BackhaulManager accepts the narrow CacheReader/CacheWriter
// interfaces rather than *cache.WarmCache directly.
type backhaulWarmCache struct {
	wc *cache.WarmCache
}

func (c backhaulWarmCache) Get(blobHash string) ([]byte, bool) {
	if c.wc == nil {
		return nil, false
	}
	return c.wc.Get(blobHash)
}

func (c backhaulWarmCache) Put(blobHash string, data []byte, bitrate int) error {
	if c.wc == nil {
		return fmt.Errorf("warm cache disabled")
	}
	return c.wc.Put(blobHash, data, bitrate)
}

// ----- backhaulICPFetcher (main.go adapter #8, for BackhaulManager ICP fetch) -----

// backhaulICPFetcher adapts ICP FetchFromPeer to backhaul.ICPFetcher.
//
// Routing: ring.Get(blobHash) returns the primary peer for this blob.
//   - empty ring (target == "")        → return (nil, false, nil): fall back
//     to local backhaul (data plane / L4 stream). Zero network calls.
//   - target == self (we are primary)  → return (nil, false, nil): same
//     fall-back; avoids looping a fetch to ourselves.
//   - target is a sibling              → call icp.FetchFromPeer once
//     (single ICP request, no retry storm per plan line 203) and return
//     its (io.ReadCloser, bool, error) as-is.
type backhaulICPFetcher struct {
	h       host.Host
	ring    *hashring.HashRing
	self    types.PeerId
	addrSrc dialassist.AddrSource
}

var _ backhaul.ICPFetcher = backhaulICPFetcher{}

func (f backhaulICPFetcher) FetchFromPeer(ctx context.Context, blobHash string) (interface{}, bool, error) {
	if f.ring == nil {
		return nil, false, nil
	}
	target := f.ring.Get(blobHash)
	if target == "" || target == f.self {
		return nil, false, nil
	}
	targetID, err := peer.Decode(string(target))
	if err != nil {
		return nil, false, fmt.Errorf("backhaul icp: decode target peer %q: %w", target, err)
	}

	has, headErr := f.fetchHeadWithReseed(ctx, targetID, blobHash)
	if headErr != nil {
		return nil, false, fmt.Errorf("backhaul icp: head probe to %s: %w", targetID, headErr)
	}
	if !has {
		return nil, false, nil
	}

	stream, getErr := f.fetchGetWithReseed(ctx, targetID, blobHash)
	if getErr != nil {
		return nil, false, fmt.Errorf("backhaul icp: get fetch from %s: %w", targetID, getErr)
	}
	return stream, true, nil
}

func (f backhaulICPFetcher) fetchHeadWithReseed(ctx context.Context, pid peer.ID, blobHash string) (bool, error) {
	has, err := icp.FetchFromPeerHead(ctx, f.h, pid, blobHash)
	if err == nil {
		return has, nil
	}

	if f.addrSrc == nil {
		return false, err
	}
	if addrs, ok := f.addrSrc.AddrsOf(pid); ok && len(addrs) > 0 {
		if mas := dialassist.ParseAddrs(addrs); len(mas) > 0 {
			f.h.Peerstore().AddAddrs(pid, mas, 10*time.Minute)
			return icp.FetchFromPeerHead(ctx, f.h, pid, blobHash)
		}
	}
	return false, err
}

func (f backhaulICPFetcher) fetchGetWithReseed(ctx context.Context, pid peer.ID, blobHash string) (io.ReadCloser, error) {
	rc, err := icp.FetchFromPeerGet(ctx, f.h, pid, blobHash)
	if err == nil {
		return rc, nil
	}

	if f.addrSrc == nil {
		return nil, err
	}
	if addrs, ok := f.addrSrc.AddrsOf(pid); ok && len(addrs) > 0 {
		if mas := dialassist.ParseAddrs(addrs); len(mas) > 0 {
			f.h.Peerstore().AddAddrs(pid, mas, 10*time.Minute)
			return icp.FetchFromPeerGet(ctx, f.h, pid, blobHash)
		}
	}
	return nil, err
}
