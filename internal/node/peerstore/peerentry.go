// Package peerstore provides persistent peer metadata storage (BadgerDB).
package peerstore

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/dgraph-io/badger/v4"
	"github.com/libp2p/go-libp2p/core/peer"

	"github.com/shlande/mediaworker/internal/types"
)

const (
	// GraylistThreshold is the GossipSub score below which a peer is rejected
	// by ConnectionGater and excluded from the hash ring.
	//
	// Must be ≤ PublishThreshold per pubsub library constraint.
	GraylistThreshold = -20.0

	// keyPrefix is the BadgerDB key prefix for PeerStoreEntry records.
	keyPrefix = "p:"
)

// PeerEntryStore is an application-level persistent store for PeerStoreEntry records.
// It is NOT a libp2p peerstore.Peerstore implementation — it is a distribution-domain
// store queried by ConnectionGater, HashRing, and GossipSub.
type PeerEntryStore struct {
	db     *badger.DB
	index  sync.Map // map[string]*types.PeerStoreEntry (key = PeerId string)
	dbPath string
	logger *slog.Logger

	gcCancel context.CancelFunc
	gcDone   chan struct{}
	gcCalls  atomic.Uint64
}

// NewPeerEntryStore opens (or creates) a BadgerDB at dbPath and returns a
// PeerEntryStore with an empty in-memory index. Callers must call Restore()
// to rebuild the index from persisted data, or Close() to release resources.
func NewPeerEntryStore(dbPath string) (*PeerEntryStore, error) {
	opts := badger.DefaultOptions(dbPath).
		WithLoggingLevel(badger.ERROR).
		WithNumVersionsToKeep(1).
		WithSyncWrites(true)

	db, err := badger.Open(opts)
	if err != nil {
		return nil, fmt.Errorf("peerentry open badger: %w", err)
	}

	return &PeerEntryStore{
		db:     db,
		dbPath: dbPath,
		logger: slog.Default().With("component", "peerstore"),
	}, nil
}

// makeKey returns the BadgerDB key for a given PeerId.
func makeKey(peerID types.PeerId) []byte {
	return []byte(keyPrefix + string(peerID))
}

// Put writes an entry to BadgerDB and updates the in-memory index atomically.
func (s *PeerEntryStore) Put(peerID types.PeerId, entry types.PeerStoreEntry) error {
	data, err := json.Marshal(entry)
	if err != nil {
		return fmt.Errorf("peerentry marshal: %w", err)
	}

	key := makeKey(peerID)
	if err := s.db.Update(func(txn *badger.Txn) error {
		return txn.Set(key, data)
	}); err != nil {
		return fmt.Errorf("peerentry put: %w", err)
	}

	_, existedBefore := s.index.Load(string(peerID))
	s.index.Store(string(peerID), &entry)
	if !existedBefore {
		s.logger.Debug("peer added to store", "peer", peerID, "addrs", entry.Addrs)
	}
	return nil
}

// PutDiscovery writes a discovery-sourced entry (no JWT / capabilities).
// It differs from Put: if the peer already exists in the store, PutDiscovery
// preserves existing auth fields (JWT, Capabilities, JWTExp, Score, Stale)
// and only refreshes Addrs (when non-empty) and LastSeen. For new peers it
// inserts a zero-value discovery entry (same as Put+FromDiscovery).
func (s *PeerEntryStore) PutDiscovery(peerID types.PeerId, addrs []string) error {
	if existing, ok := s.Get(peerID); ok {
		existing.LastSeen = time.Now().Unix()
		if len(addrs) > 0 {
			existing.Addrs = addrs
		}
		return s.Put(peerID, existing)
	}
	return s.Put(peerID, types.PeerStoreEntry{
		PeerID:   peerID,
		Addrs:    addrs,
		LastSeen: time.Now().Unix(),
	})
}

// Get reads an entry from the in-memory index (fast path, no DB read).
func (s *PeerEntryStore) Get(peerID types.PeerId) (types.PeerStoreEntry, bool) {
	val, ok := s.index.Load(string(peerID))
	if !ok {
		return types.PeerStoreEntry{}, false
	}
	entry, ok := val.(*types.PeerStoreEntry)
	if !ok {
		return types.PeerStoreEntry{}, false
	}
	return *entry, true
}

// Has reports whether an entry exists for the given peer.
func (s *PeerEntryStore) Has(peerID types.PeerId) bool {
	_, ok := s.index.Load(string(peerID))
	return ok
}

// AddrsOf returns the stored multiaddr strings for the given libp2p peer ID.
// It implements dialassist.AddrSource for dial-retry reseed operations.
func (s *PeerEntryStore) AddrsOf(pid peer.ID) ([]string, bool) {
	entry, ok := s.Get(types.PeerId(pid.String()))
	if !ok {
		return nil, false
	}
	return entry.Addrs, true
}

// Delete removes an entry from BadgerDB and the in-memory index.
func (s *PeerEntryStore) Delete(peerID types.PeerId) error {
	key := makeKey(peerID)
	if err := s.db.Update(func(txn *badger.Txn) error {
		return txn.Delete(key)
	}); err != nil {
		return fmt.Errorf("peerentry delete: %w", err)
	}

	_, existed := s.index.Load(string(peerID))
	s.index.Delete(string(peerID))
	if existed {
		s.logger.Debug("peer removed from store", "peer", peerID)
	}
	return nil
}

// ActivePeers returns all entries that are healthy: not Stale AND Score >= GraylistThreshold.
func (s *PeerEntryStore) ActivePeers() []types.PeerStoreEntry {
	var result []types.PeerStoreEntry
	s.index.Range(func(_, val any) bool {
		entry, ok := val.(*types.PeerStoreEntry)
		if !ok {
			return true
		}
		if !entry.Stale && entry.Score >= GraylistThreshold {
			result = append(result, *entry)
		}
		return true
	})
	return result
}

// List returns ALL entries regardless of Stale/Score, sorted by PeerID for
// deterministic admin output. ActivePeers is the filtered routing view; List
// is the unfiltered management view (GET /v1/peers).
func (s *PeerEntryStore) List() []types.PeerStoreEntry {
	var result []types.PeerStoreEntry
	s.index.Range(func(_, val any) bool {
		if entry, ok := val.(*types.PeerStoreEntry); ok {
			result = append(result, *entry)
		}
		return true
	})
	sort.Slice(result, func(i, j int) bool { return result[i].PeerID < result[j].PeerID })
	return result
}

// MarkStale sets Stale=true on the entry for the given peer, persisting to
// BadgerDB and updating the in-memory index.
func (s *PeerEntryStore) MarkStale(peerID types.PeerId, reason string) error {
	key := makeKey(peerID)

	return s.db.Update(func(txn *badger.Txn) error {
		item, err := txn.Get(key)
		if err != nil {
			return fmt.Errorf("peerentry markstale get: %w", err)
		}

		var entry types.PeerStoreEntry
		if err := item.Value(func(val []byte) error {
			return json.Unmarshal(val, &entry)
		}); err != nil {
			return fmt.Errorf("peerentry markstale unmarshal: %w", err)
		}

		entry.Stale = true

		data, err := json.Marshal(entry)
		if err != nil {
			return fmt.Errorf("peerentry markstale marshal: %w", err)
		}

		if err := txn.Set(key, data); err != nil {
			return fmt.Errorf("peerentry markstale set: %w", err)
		}

		s.index.Store(string(peerID), &entry)
		s.logger.Warn("peer marked stale", "peer", peerID, "reason", reason)
		return nil
	})
}

// Restore rebuilds the in-memory sync.Map index by iterating all BadgerDB
// entries with the "p:" prefix.
func (s *PeerEntryStore) Restore() error {
	return s.db.View(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.Prefix = []byte(keyPrefix)
		it := txn.NewIterator(opts)
		defer it.Close()

		for it.Rewind(); it.Valid(); it.Next() {
			item := it.Item()
			if err := item.Value(func(val []byte) error {
				var entry types.PeerStoreEntry
				if err := json.Unmarshal(val, &entry); err != nil {
					return fmt.Errorf("restore unmarshal key %q: %w", item.Key(), err)
				}
				s.index.Store(string(entry.PeerID), &entry)
				return nil
			}); err != nil {
				return err
			}
		}
		return nil
	})
}

// Close releases the BadgerDB handle. The in-memory index is discarded.
// If StartValueLogGC was called, the GC goroutine is signalled to stop and
// waited for before closing the DB.
func (s *PeerEntryStore) Close() error {
	if s.gcCancel != nil {
		s.gcCancel()
		<-s.gcDone
		s.gcCancel = nil
		s.gcDone = nil
	}
	if err := s.db.Close(); err != nil {
		return fmt.Errorf("peerentry close: %w", err)
	}
	return nil
}

// StartValueLogGC starts a background goroutine that periodically invokes
// Badger's RunValueLogGC (value-log garbage collection) at the given interval.
// The goroutine stops when Close() is called or when ctx is cancelled.
//
// RunValueLogGC only reclaims space when the value log discard ratio exceeds
// a threshold (Badger defaults to 0.5); calls below the threshold are no-ops
// (and return nil with no error). This is the expected steady state.
//
// GCCalls returns the number of times RunValueLogGC was invoked (useful for
// assembly-test assertions that the ticker is wired up).
func (s *PeerEntryStore) StartValueLogGC(ctx context.Context, interval time.Duration) {
	if interval <= 0 {
		return
	}
	gcCtx, cancel := context.WithCancel(ctx)
	done := make(chan struct{})

	s.gcCancel = cancel
	s.gcDone = done

	go s.runValueLogGCLoop(gcCtx, interval, done)
}

// GCCalls returns the number of times the value-log GC loop has invoked
// Badger's RunValueLogGC since StartValueLogGC was called.
func (s *PeerEntryStore) GCCalls() uint64 {
	return s.gcCalls.Load()
}

func (s *PeerEntryStore) runValueLogGCLoop(ctx context.Context, interval time.Duration, done chan struct{}) {
	defer close(done)
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			// RunValueLogGC returns:
			//   nil  — nothing to GC (discard ratio below threshold) OR GC ran
			//   error — only on DB-level failures (e.g. value log not initialised)
			// Either way we log at Debug and continue; GC failures are not fatal.
			if err := s.db.RunValueLogGC(0.5); err != nil {
				s.logger.Debug("badger value-log gc", "err", err)
			}
			s.gcCalls.Add(1)
		}
	}
}

// PeerIdFromPeerID converts a libp2p peer.ID to a domain PeerId.
func PeerIdFromPeerID(p peer.ID) types.PeerId {
	return types.PeerId(p.String())
}

// PeerStoreEntryFromDiscovery creates a PeerStoreEntry from a discovered peer
// with minimal metadata (no JWT yet — assigned after JWT handshake in a later task).
func PeerStoreEntryFromDiscovery(p peer.ID, addrs []string) types.PeerStoreEntry {
	return types.PeerStoreEntry{
		PeerID:   PeerIdFromPeerID(p),
		Addrs:    addrs,
		LastSeen: time.Now().Unix(),
	}
}
