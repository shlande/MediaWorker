package jwt

import (
	"fmt"

	"github.com/dgraph-io/badger/v4"
	"github.com/shlande/mediaworker/internal/types"
)

const whitelistKeyPrefix = "w:"

// WhitelistStore persists L4 whitelist PeerId entries to BadgerDB.
// It mirrors the in-memory PeerIdSet, allowing the whitelist to survive
// control-plane restarts.
type WhitelistStore struct {
	db     *badger.DB
	dbPath string
}

// NewWhitelistStore opens (or creates) a BadgerDB at dbPath and returns a
// WhitelistStore. Callers must call Restore() to populate an in-memory
// PeerIdSet from persisted data, or Close() to release resources.
func NewWhitelistStore(dbPath string) (*WhitelistStore, error) {
	opts := badger.DefaultOptions(dbPath).
		WithLoggingLevel(badger.ERROR).
		WithNumVersionsToKeep(1).
		WithSyncWrites(true)

	db, err := badger.Open(opts)
	if err != nil {
		return nil, fmt.Errorf("whitelist open badger: %w", err)
	}

	return &WhitelistStore{db: db, dbPath: dbPath}, nil
}

// makeWhitelistKey returns the BadgerDB key for a given PeerId.
func makeWhitelistKey(peerID types.PeerId) []byte {
	return []byte(whitelistKeyPrefix + string(peerID))
}

// Add persists a PeerId to the whitelist.
func (s *WhitelistStore) Add(peerID types.PeerId) error {
	key := makeWhitelistKey(peerID)
	return s.db.Update(func(txn *badger.Txn) error {
		return txn.Set(key, []byte{1})
	})
}

// Remove deletes a PeerId from the whitelist.
func (s *WhitelistStore) Remove(peerID types.PeerId) error {
	key := makeWhitelistKey(peerID)
	return s.db.Update(func(txn *badger.Txn) error {
		return txn.Delete(key)
	})
}

// Contains reports whether the PeerId exists in the whitelist.
// It reads directly from BadgerDB (no in-memory cache).
func (s *WhitelistStore) Contains(peerID types.PeerId) bool {
	key := makeWhitelistKey(peerID)
	err := s.db.View(func(txn *badger.Txn) error {
		_, err := txn.Get(key)
		return err
	})
	return err == nil
}

// ListAll returns all PeerIds currently in the whitelist.
func (s *WhitelistStore) ListAll() ([]types.PeerId, error) {
	var peers []types.PeerId
	err := s.db.View(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.Prefix = []byte(whitelistKeyPrefix)
		it := txn.NewIterator(opts)
		defer it.Close()

		for it.Rewind(); it.Valid(); it.Next() {
			item := it.Item()
			key := item.Key()
			// Strip prefix to get the PeerId string.
			peerID := types.PeerId(string(key[len(whitelistKeyPrefix):]))
			peers = append(peers, peerID)
		}
		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("whitelist listall: %w", err)
	}
	return peers, nil
}

// Restore loads all whitelist entries from BadgerDB into the in-memory
// PeerIdSet. Call this on startup to rebuild the whitelist.
func (s *WhitelistStore) Restore(ps *PeerIdSet) error {
	return s.db.View(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.Prefix = []byte(whitelistKeyPrefix)
		it := txn.NewIterator(opts)
		defer it.Close()

		for it.Rewind(); it.Valid(); it.Next() {
			item := it.Item()
			key := item.Key()
			peerID := types.PeerId(string(key[len(whitelistKeyPrefix):]))
			ps.Add(peerID)
		}
		return nil
	})
}

// Close releases the BadgerDB handle.
func (s *WhitelistStore) Close() error {
	if err := s.db.Close(); err != nil {
		return fmt.Errorf("whitelist close: %w", err)
	}
	return nil
}
