package jwt

import (
	"encoding/json"
	"fmt"
	"log/slog"
	"time"

	"github.com/dgraph-io/badger/v4"
	"github.com/shlande/mediaworker/internal/types"
)

const whitelistKeyPrefix = "w:"

// WhitelistEntry is the persisted metadata for a single whitelist peer.
// The JSON-serialized form is stored as the BadgerDB value.
type WhitelistEntry struct {
	PeerID  string    `json:"peer_id"`
	AddedAt time.Time `json:"added_at"`
	AddedBy string    `json:"added_by"`
}

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

// Add persists a PeerId to the whitelist with metadata about who added it.
func (s *WhitelistStore) Add(peerID types.PeerId, addedBy string) error {
	entry := WhitelistEntry{
		PeerID:  string(peerID),
		AddedAt: time.Now(),
		AddedBy: addedBy,
	}
	val, err := json.Marshal(entry)
	if err != nil {
		return fmt.Errorf("whitelist add marshal: %w", err)
	}
	key := makeWhitelistKey(peerID)
	return s.db.Update(func(txn *badger.Txn) error {
		return txn.Set(key, val)
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

// ListAll returns all whitelist entries with their metadata.
// Legacy entries (value == []byte{1}) are returned with zero AddedAt and empty AddedBy.
// Corrupt JSON values are returned with empty metadata and a warning log; the list is not aborted.
func (s *WhitelistStore) ListAll() ([]WhitelistEntry, error) {
	var entries []WhitelistEntry
	err := s.db.View(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.Prefix = []byte(whitelistKeyPrefix)
		it := txn.NewIterator(opts)
		defer it.Close()

		for it.Rewind(); it.Valid(); it.Next() {
			item := it.Item()
			key := item.Key()
			peerID := string(key[len(whitelistKeyPrefix):])

			entry := WhitelistEntry{PeerID: peerID}

			val, getErr := item.ValueCopy(nil)
			if getErr != nil {
				slog.Warn("whitelist listall get value error, returning empty metadata",
					"peer_id", peerID, "err", getErr)
				entries = append(entries, entry)
				continue
			}

			// Legacy format: single byte 0x01 (no metadata).
			if len(val) == 1 && val[0] == 1 {
				entries = append(entries, entry)
				continue
			}

			if err := json.Unmarshal(val, &entry); err != nil {
				slog.Warn("whitelist listall corrupt json, returning empty metadata",
					"peer_id", peerID, "err", err)
				entry = WhitelistEntry{PeerID: peerID}
			}
			entries = append(entries, entry)
		}
		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("whitelist listall: %w", err)
	}
	return entries, nil
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
