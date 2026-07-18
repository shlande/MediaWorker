// Package pinstore provides persistent pin-state management for content blobs.
// PinStore is the infrastructure layer — it executes pin/unpin operations but
// contains no strategy logic (strategy lives in the pinstrategy package).
//
// Storage backend: BadgerDB (embedded KV) for pin metadata + NVMe prefix
// partition for blob data. The in-memory sync.Map index is rebuilt from
// BadgerDB on restart via Restore().
package pinstore

import (
	"encoding/json"
	"fmt"
	"log"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"github.com/dgraph-io/badger/v4"

	"github.com/shlande/mediaworker/internal/types"
)

// ─── Delta constants ───

const (
	DeltaPin   = 1
	DeltaUnpin = 2
)

// keyPrefix is the BadgerDB key prefix for PinEntry records.
const keyPrefix = "p:"

// ─── Types ───

// PinEntry is the persisted pin record for a single blob.
type PinEntry struct {
	BlobHash string    `json:"blob_hash"`
	BlobType string    `json:"blob_type"`
	Role     string    `json:"role"`
	Size     int64     `json:"size"`
	PinnedAt time.Time `json:"pinned_at"`
	Ready    atomic.Bool `json:"-"`
}

// PinDelta records an incremental pin/unpin operation for logging/audit.
type PinDelta struct {
	Type     int    `json:"type"` // DeltaPin or DeltaUnpin
	BlobHash string `json:"blob_hash"`
	BlobType string `json:"blob_type"`
	Role     string `json:"role"`
}

// DeltaBuffer accumulates pin/unpin deltas.
type DeltaBuffer struct {
	mu     sync.Mutex
	deltas []PinDelta
}

// Append adds a delta to the buffer.
func (db *DeltaBuffer) Append(d PinDelta) {
	db.mu.Lock()
	defer db.mu.Unlock()
	db.deltas = append(db.deltas, d)
}

// ─── PinStore ───

// PinStore manages persistent pin-state for content blobs.
// It uses BadgerDB for metadata persistence and a PrefixPartition for blob data.
type PinStore struct {
	db          *badger.DB
	storage     *PrefixPartition
	index       sync.Map // map[string]*PinEntry
	deltaBuffer *DeltaBuffer
	fetchFunc   func(blobHash string) ([]byte, error)
}

// NewPinStore opens (or creates) a BadgerDB at dbPath and returns a PinStore.
// The fetchFunc is an injected blob fetcher used by fetchPinnedBlob.
// Callers must call Restore() to rebuild the index, or Close() to release resources.
func NewPinStore(dbPath string, storagePath string, maxSize int64, fetchFunc func(string) ([]byte, error)) (*PinStore, error) {
	// Ensure storage directory exists.
	if err := os.MkdirAll(storagePath, 0o755); err != nil {
		return nil, fmt.Errorf("pinstore mkdir storage: %w", err)
	}

	opts := badger.DefaultOptions(dbPath).
		WithLoggingLevel(badger.ERROR).
		WithNumVersionsToKeep(1).
		WithSyncWrites(true)

	db, err := badger.Open(opts)
	if err != nil {
		return nil, fmt.Errorf("pinstore open badger: %w", err)
	}

	return &PinStore{
		db:          db,
		storage:     newPrefixPartition(storagePath, maxSize),
		deltaBuffer: &DeltaBuffer{},
		fetchFunc:   fetchFunc,
	}, nil
}

// ─── Internal helpers ───

func makePinKey(blobHash string) []byte {
	return []byte(keyPrefix + blobHash)
}

type pinEntryJSON struct {
	BlobHash string    `json:"blob_hash"`
	BlobType string    `json:"blob_type"`
	Size     int64     `json:"size"`
	PinnedAt time.Time `json:"pinned_at"`
	Ready    bool      `json:"ready"`
}

func encodePinEntry(entry *PinEntry) ([]byte, error) {
	return json.Marshal(pinEntryJSON{
		BlobHash: entry.BlobHash,
		BlobType: entry.BlobType,
		Size:     entry.Size,
		PinnedAt: entry.PinnedAt,
		Ready:    entry.Ready.Load(),
	})
}

func decodePinEntry(data []byte) (*PinEntry, error) {
	var j pinEntryJSON
	if err := json.Unmarshal(data, &j); err != nil {
		return nil, fmt.Errorf("pinstore decode pin entry: %w", err)
	}
	entry := &PinEntry{
		BlobHash: j.BlobHash,
		BlobType: j.BlobType,
		Size:     j.Size,
		PinnedAt: j.PinnedAt,
	}
	entry.Ready.Store(j.Ready)
	return entry, nil
}

// dbUpdate runs fn inside a BadgerDB Update transaction.
func (ps *PinStore) dbUpdate(fn func(txn *badger.Txn) error) {
	if err := ps.db.Update(fn); err != nil {
		log.Printf("[pinstore] db update error: %v", err)
	}
}

// dbView runs fn inside a BadgerDB View transaction.
func (ps *PinStore) dbView(fn func(txn *badger.Txn) error) error {
	return ps.db.View(fn)
}

// ─── Infrastructure API ───

// ApplyPin persists a pin record, updates the in-memory index, appends a delta,
// and asynchronously fetches the blob to the prefix partition.
// Idempotent: if the blob is already pinned, this is a no-op.
// blobType: binary output type ("mp4_init_segment" etc).
// role: semantic role of the blob within its content ("init"/"media" etc), used by eviction logic.
func (ps *PinStore) ApplyPin(blobHash string, blobType string, role string, size int64) {
	if _, ok := ps.index.Load(blobHash); ok {
		return // already pinned, idempotent
	}

	entry := &PinEntry{
		BlobHash: blobHash,
		BlobType: blobType,
		Role:     role,
		Size:     size,
		PinnedAt: time.Now(),
	}

	// 1. Persist to BadgerDB.
	data, err := encodePinEntry(entry)
	if err != nil {
		log.Printf("[pinstore] encode pin entry %s: %v", blobHash, err)
		return
	}
	ps.dbUpdate(func(txn *badger.Txn) error {
		return txn.Set(makePinKey(blobHash), data)
	})

	// 2. Update in-memory index.
	ps.index.Store(blobHash, entry)

	// 3. Append delta.
	ps.deltaBuffer.Append(PinDelta{Type: DeltaPin, BlobHash: blobHash, BlobType: blobType, Role: role})

	// 4. Asynchronously fetch blob to prefix partition.
	go ps.fetchPinnedBlob(blobHash)
}

// ApplyUnpin removes a pin record from BadgerDB, the in-memory index, and the
// prefix partition. Idempotent: if the blob is not pinned, this is a no-op.
func (ps *PinStore) ApplyUnpin(blobHash string) {
	if _, ok := ps.index.Load(blobHash); !ok {
		return // not pinned, idempotent
	}

	// 1. Delete from BadgerDB.
	ps.dbUpdate(func(txn *badger.Txn) error {
		return txn.Delete(makePinKey(blobHash))
	})

	// 2. Delete from in-memory index.
	ps.index.Delete(blobHash)

	// 3. Append delta.
	ps.deltaBuffer.Append(PinDelta{Type: DeltaUnpin, BlobHash: blobHash})

	// 4. Delete blob data from prefix partition.
	if err := ps.storage.RemoveContent(blobHash); err != nil {
		log.Printf("[pinstore] remove content %s: %v", blobHash, err)
	}
}

// ApplyPartialUnpin is equivalent to ApplyUnpin in the blob-level pin model.
func (ps *PinStore) ApplyPartialUnpin(blobHash string) {
	ps.ApplyUnpin(blobHash)
}

// IsPinned returns true if the blob hash is present in the in-memory index.
// This is a pure memory query — no disk I/O.
func (ps *PinStore) IsPinned(blobHash string) bool {
	_, ok := ps.index.Load(blobHash)
	return ok
}

// IsReady returns true if the pinned blob has been successfully fetched and is
// present in the prefix partition.
func (ps *PinStore) IsReady(blobHash string) bool {
	val, ok := ps.index.Load(blobHash)
	if !ok {
		return false
	}
	entry, ok := val.(*PinEntry)
	if !ok || !entry.Ready.Load() {
		return false
	}
	return ps.storage.Has(blobHash)
}

// fetchPinnedBlob asynchronously fetches a blob and writes it to the prefix
// partition. Panics in the fetchFunc are recovered so the node does not crash.
// On failure, Ready stays false and the blob is served via the normal origin path.
func (ps *PinStore) fetchPinnedBlob(blobHash string) {
	defer func() {
		if r := recover(); r != nil {
			log.Printf("[pinstore] fetchPinnedBlob panicked: blob=%s panic=%v", blobHash, r)
		}
	}()

	data, err := ps.fetchFunc(blobHash)
	if err != nil {
		log.Printf("[pinstore] fetch pinned blob failed: blob=%s err=%v", blobHash, err)
		return
	}

	if err := ps.storage.Put(blobHash, data); err != nil {
		log.Printf("[pinstore] storage put failed: blob=%s err=%v", blobHash, err)
		return
	}

	// Mark Ready in both BadgerDB and the in-memory index.
	val, ok := ps.index.Load(blobHash)
	if !ok {
		return
	}
	entry, ok := val.(*PinEntry)
	if !ok {
		return
	}
	entry.Ready.Store(true)

	data, err = encodePinEntry(entry)
	if err != nil {
		log.Printf("[pinstore] encode ready entry %s: %v", blobHash, err)
		return
	}
	ps.dbUpdate(func(txn *badger.Txn) error {
		return txn.Set(makePinKey(blobHash), data)
	})
}

// ─── Space queries ───

// pinCount returns the number of pinned blobs.
func (ps *PinStore) pinCount() int32 {
	var count int32
	ps.index.Range(func(_, _ any) bool {
		count++
		return true
	})
	return count
}

// totalPinnedSize returns the sum of sizes for all pinned blobs.
func (ps *PinStore) totalPinnedSize() int64 {
	var total int64
	ps.index.Range(func(_, val any) bool {
		entry, ok := val.(*PinEntry)
		if !ok {
			return true
		}
		total += entry.Size
		return true
	})
	return total
}

// QuerySpace returns the pin-space statistics for RPC queries.
func (ps *PinStore) QuerySpace() types.PinSpaceInfo {
	return types.PinSpaceInfo{
		AvailableBytes:  ps.storage.Available(),
		PinnedCount:     ps.pinCount(),
		TotalPinnedSize: ps.totalPinnedSize(),
	}
}

// HandleQueryPinSpace is the RPC handler for pin-space queries.
func (ps *PinStore) HandleQueryPinSpace() types.PinSpaceInfo {
	return ps.QuerySpace()
}

// ─── Persistence ───

// Restore rebuilds the in-memory sync.Map index by iterating all BadgerDB
// entries with the "p:" prefix. Call this after NewPinStore on node restart.
func (ps *PinStore) Restore() error {
	return ps.dbView(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.Prefix = []byte(keyPrefix)
		it := txn.NewIterator(opts)
		defer it.Close()

		for it.Rewind(); it.Valid(); it.Next() {
			item := it.Item()
			if err := item.Value(func(val []byte) error {
				entry, err := decodePinEntry(val)
				if err != nil {
					return fmt.Errorf("restore decode key %q: %w", item.Key(), err)
				}
				ps.index.Store(entry.BlobHash, entry)
				return nil
			}); err != nil {
				return err
			}
		}
		return nil
	})
}

// Close releases the BadgerDB handle. The in-memory index is discarded.
func (ps *PinStore) Close() error {
	if err := ps.db.Close(); err != nil {
		return fmt.Errorf("pinstore close: %w", err)
	}
	return nil
}
