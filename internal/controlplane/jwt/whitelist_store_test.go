package jwt_test

import (
	"encoding/json"
	"os"
	"testing"
	"time"

	"github.com/dgraph-io/badger/v4"
	"github.com/shlande/mediaworker/internal/controlplane/jwt"
	"github.com/shlande/mediaworker/internal/types"
)

func TestWhitelistStore_persistAcrossRestarts(t *testing.T) {
	// Given: a fresh WhitelistStore on a temporary directory.
	dir := t.TempDir()
	dbPath := dir + "/whitelist"

	store, err := jwt.NewWhitelistStore(dbPath)
	if err != nil {
		t.Fatalf("new store: %v", err)
	}

	// When: Add a peer and close the store.
	peer := types.PeerId("12D3KooWTestPersistPeerID")
	if err := store.Add(peer, "test"); err != nil {
		t.Fatalf("add: %v", err)
	}
	_ = store.Close()

	// Then: after reopening, Contains is true and ListAll returns it.
	store2, err := jwt.NewWhitelistStore(dbPath)
	if err != nil {
		t.Fatalf("reopen store: %v", err)
	}
	defer func() { _ = store2.Close() }()

	if !store2.Contains(peer) {
		t.Fatal("expected peer to persist across restart")
	}

	all, err := store2.ListAll()
	if err != nil {
		t.Fatalf("listall: %v", err)
	}
	if len(all) != 1 || all[0].PeerID != string(peer) {
		t.Fatalf("expected [%q], got %v", peer, all)
	}
}

func TestWhitelistStore_removePersists(t *testing.T) {
	// Given: a WhitelistStore with two peers.
	dir := t.TempDir()
	dbPath := dir + "/whitelist"

	store, err := jwt.NewWhitelistStore(dbPath)
	if err != nil {
		t.Fatalf("new store: %v", err)
	}

	peerA := types.PeerId("12D3KooWTestPeerA")
	peerB := types.PeerId("12D3KooWTestPeerB")
	if err := store.Add(peerA, "test"); err != nil {
		t.Fatalf("add peerA: %v", err)
	}
	if err := store.Add(peerB, "test"); err != nil {
		t.Fatalf("add peerB: %v", err)
	}

	// When: remove peerA and close.
	if err := store.Remove(peerA); err != nil {
		t.Fatalf("remove: %v", err)
	}
	_ = store.Close()

	// Then: after reopen, peerA is gone, peerB remains.
	store2, err := jwt.NewWhitelistStore(dbPath)
	if err != nil {
		t.Fatalf("reopen store: %v", err)
	}
	defer func() { _ = store2.Close() }()

	if store2.Contains(peerA) {
		t.Fatal("peerA should be removed")
	}
	if !store2.Contains(peerB) {
		t.Fatal("peerB should persist")
	}
}

func TestWhitelistStore_restoreIntoPeerIdSet(t *testing.T) {
	// Given: a WhitelistStore with two peers, closed.
	dir := t.TempDir()
	dbPath := dir + "/whitelist"

	store, err := jwt.NewWhitelistStore(dbPath)
	if err != nil {
		t.Fatalf("new store: %v", err)
	}

	peer1 := types.PeerId("12D3KooWRestorePeer1")
	peer2 := types.PeerId("12D3KooWRestorePeer2")
	if err := store.Add(peer1, "test"); err != nil {
		t.Fatalf("add peer1: %v", err)
	}
	if err := store.Add(peer2, "test"); err != nil {
		t.Fatalf("add peer2: %v", err)
	}
	_ = store.Close()

	// When: reopen and Restore into an empty PeerIdSet.
	store2, err := jwt.NewWhitelistStore(dbPath)
	if err != nil {
		t.Fatalf("reopen store: %v", err)
	}
	defer func() { _ = store2.Close() }()

	ps := jwt.NewPeerIdSet()
	if err := store2.Restore(ps); err != nil {
		t.Fatalf("restore: %v", err)
	}

	// Then: both peers are in the PeerIdSet.
	if !ps.Contains(peer1) {
		t.Fatal("peer1 should be in PeerIdSet after restore")
	}
	if !ps.Contains(peer2) {
		t.Fatal("peer2 should be in PeerIdSet after restore")
	}
	if ps.Contains(types.PeerId("neverAdded")) {
		t.Fatal("non-existent peer should not be in PeerIdSet")
	}
}

func TestWhitelistStore_containsNotExist(t *testing.T) {
	// Given: an empty store.
	dir := t.TempDir()
	dbPath := dir + "/whitelist"

	store, err := jwt.NewWhitelistStore(dbPath)
	if err != nil {
		t.Fatalf("new store: %v", err)
	}
	defer func() { _ = store.Close() }()

	// Then: Contains returns false for an unknown peer.
	if store.Contains(types.PeerId("12D3KooWDoesNotExist")) {
		t.Fatal("expected false for unknown peer")
	}
}

func TestWhitelistStore_emptyListAll(t *testing.T) {
	dir := t.TempDir()
	dbPath := dir + "/whitelist"

	store, err := jwt.NewWhitelistStore(dbPath)
	if err != nil {
		t.Fatalf("new store: %v", err)
	}
	defer func() { _ = store.Close() }()

	all, err := store.ListAll()
	if err != nil {
		t.Fatalf("listall: %v", err)
	}
	if len(all) != 0 {
		t.Fatalf("expected empty list, got %v", all)
	}
}

func TestWhitelistStore_NewWhitelistStore_createsDir(t *testing.T) {
	dir := t.TempDir()
	dbPath := dir + "/nonexistent/sub/whitelist"

	store, err := jwt.NewWhitelistStore(dbPath)
	if err != nil {
		t.Fatalf("new store with nested path: %v", err)
	}
	defer func() { _ = store.Close() }()

	// Verify the directory was created.
	if _, err := os.Stat(dbPath); os.IsNotExist(err) {
		t.Fatal("expected dbPath to be created")
	}
}

// --- New tests for JSON metadata ---

func TestWhitelistStore_addWithMetadata_roundTrips(t *testing.T) {
	// Given: a fresh WhitelistStore.
	dir := t.TempDir()
	dbPath := dir + "/whitelist"

	store, err := jwt.NewWhitelistStore(dbPath)
	if err != nil {
		t.Fatalf("new store: %v", err)
	}
	defer func() { _ = store.Close() }()

	// When: Add a peer with an addedBy value.
	peer := types.PeerId("12D3KooWMetadataPeer")
	before := time.Now()
	if err := store.Add(peer, "admin-ui"); err != nil {
		t.Fatalf("add: %v", err)
	}

	// Then: ListAll returns the full WhitelistEntry.
	all, err := store.ListAll()
	if err != nil {
		t.Fatalf("listall: %v", err)
	}
	if len(all) != 1 {
		t.Fatalf("expected 1 entry, got %d", len(all))
	}
	e := all[0]
	if e.PeerID != string(peer) {
		t.Errorf("PeerID: expected %q, got %q", peer, e.PeerID)
	}
	if e.AddedBy != "admin-ui" {
		t.Errorf("AddedBy: expected %q, got %q", "admin-ui", e.AddedBy)
	}
	if e.AddedAt.Before(before) || e.AddedAt.After(time.Now()) {
		t.Errorf("AddedAt %v not in expected range [%v, now]", e.AddedAt, before)
	}
}

func TestWhitelistStore_addWithAddedBy_emptyString(t *testing.T) {
	// Given: empty addedBy should be stored as-is.
	dir := t.TempDir()
	dbPath := dir + "/whitelist"

	store, err := jwt.NewWhitelistStore(dbPath)
	if err != nil {
		t.Fatalf("new store: %v", err)
	}
	defer func() { _ = store.Close() }()

	peer := types.PeerId("12D3KooWEmptyAddedBy")
	if err := store.Add(peer, ""); err != nil {
		t.Fatalf("add: %v", err)
	}

	all, err := store.ListAll()
	if err != nil {
		t.Fatalf("listall: %v", err)
	}
	if len(all) != 1 {
		t.Fatalf("expected 1 entry, got %d", len(all))
	}
	if all[0].PeerID != string(peer) {
		t.Errorf("PeerID: expected %q, got %q", peer, all[0].PeerID)
	}
	if all[0].AddedBy != "" {
		t.Errorf("AddedBy: expected empty, got %q", all[0].AddedBy)
	}
	if all[0].AddedAt.IsZero() {
		t.Error("AddedAt should not be zero for a valid add")
	}
}

func TestWhitelistStore_restoreWithJSONValues(t *testing.T) {
	// Given: entries persisted with the new JSON format, then closed.
	dir := t.TempDir()
	dbPath := dir + "/whitelist"

	store, err := jwt.NewWhitelistStore(dbPath)
	if err != nil {
		t.Fatalf("new store: %v", err)
	}

	peer1 := types.PeerId("12D3KooWRestoreJSON1")
	peer2 := types.PeerId("12D3KooWRestoreJSON2")
	if err := store.Add(peer1, "alice"); err != nil {
		t.Fatalf("add peer1: %v", err)
	}
	if err := store.Add(peer2, "bob"); err != nil {
		t.Fatalf("add peer2: %v", err)
	}
	_ = store.Close()

	// When: reopen and Restore.
	store2, err := jwt.NewWhitelistStore(dbPath)
	if err != nil {
		t.Fatalf("reopen store: %v", err)
	}
	defer func() { _ = store2.Close() }()

	ps := jwt.NewPeerIdSet()
	if err := store2.Restore(ps); err != nil {
		t.Fatalf("restore: %v", err)
	}

	// Then: PeerIdSet contains both — Restore only extracts peerID regardless of format.
	if !ps.Contains(peer1) {
		t.Fatal("peer1 should be in PeerIdSet after restore with JSON values")
	}
	if !ps.Contains(peer2) {
		t.Fatal("peer2 should be in PeerIdSet after restore with JSON values")
	}
}

func TestWhitelistStore_listAll_readsLegacyFormat(t *testing.T) {
	// Given: manually write a legacy []byte{1} value directly to BadgerDB.
	dir := t.TempDir()
	dbPath := dir + "/whitelist"

	store, err := jwt.NewWhitelistStore(dbPath)
	if err != nil {
		t.Fatalf("new store: %v", err)
	}
	defer func() { _ = store.Close() }()

	// Write a legacy entry — we must access the underlying *badger.DB.
	// Since WhitelistStore.db is unexported, we open a second DB handle on the same path
	// to inject a legacy value, close it, then read from the reopened store.
	legacyPeer := types.PeerId("12D3KooWLegacyPeer")
	legacyKey := []byte("w:" + string(legacyPeer))

	// Inject legacy format via the existing store's open DB — we can't access
	// s.db directly from _test, so we use a brief store close/reopen cycle.
	_ = store.Close()

	// Reopen to inject legacy data.
	store2, err := jwt.NewWhitelistStore(dbPath)
	if err != nil {
		t.Fatalf("reopen for injection: %v", err)
	}
	// Add a normal entry first.
	if err := store2.Add(types.PeerId("12D3KooWNormalPeer"), "test"); err != nil {
		t.Fatalf("add normal: %v", err)
	}
	_ = store2.Close()

	// Now write legacy value directly via badger. We open our own badger DB to do this.
	legacyDB, err := badger.Open(badger.DefaultOptions(dbPath).WithLoggingLevel(badger.ERROR))
	if err != nil {
		t.Fatalf("open legacy db: %v", err)
	}
	err = legacyDB.Update(func(txn *badger.Txn) error {
		return txn.Set(legacyKey, []byte{1})
	})
	if err != nil {
		legacyDB.Close()
		t.Fatalf("write legacy value: %v", err)
	}
	legacyDB.Close()

	// When: reopen the store and ListAll.
	store3, err := jwt.NewWhitelistStore(dbPath)
	if err != nil {
		t.Fatalf("reopen for read: %v", err)
	}
	defer func() { _ = store3.Close() }()

	all, err := store3.ListAll()
	if err != nil {
		t.Fatalf("listall: %v", err)
	}

	// Then: both entries are present; legacy entry has zero AddedAt and empty AddedBy.
	if len(all) < 2 {
		t.Fatalf("expected at least 2 entries, got %d: %v", len(all), all)
	}

	var foundLegacy, foundNormal bool
	for _, e := range all {
		switch e.PeerID {
		case string(legacyPeer):
			foundLegacy = true
			if !e.AddedAt.IsZero() {
				t.Errorf("legacy entry AddedAt should be zero, got %v", e.AddedAt)
			}
			if e.AddedBy != "" {
				t.Errorf("legacy entry AddedBy should be empty, got %q", e.AddedBy)
			}
		case "12D3KooWNormalPeer":
			foundNormal = true
			if e.AddedAt.IsZero() {
				t.Error("normal entry AddedAt should not be zero")
			}
			if e.AddedBy != "test" {
				t.Errorf("normal entry AddedBy: expected %q, got %q", "test", e.AddedBy)
			}
		}
	}
	if !foundLegacy {
		t.Fatal("legacy entry not found in ListAll")
	}
	if !foundNormal {
		t.Fatal("normal entry not found in ListAll")
	}
}

func TestWhitelistStore_listAll_corruptJSON_doesNotAbort(t *testing.T) {
	// Given: a corrupt JSON value directly in BadgerDB alongside a valid JSON entry.
	dir := t.TempDir()
	dbPath := dir + "/whitelist"

	store, err := jwt.NewWhitelistStore(dbPath)
	if err != nil {
		t.Fatalf("new store: %v", err)
	}

	validPeer := types.PeerId("12D3KooWValidPeer")
	if err := store.Add(validPeer, "test"); err != nil {
		t.Fatalf("add valid peer: %v", err)
	}
	_ = store.Close()

	// Inject corrupt JSON value via direct badger access.
	corruptPeer := types.PeerId("12D3KooWCorruptPeer")
	corruptKey := []byte("w:" + string(corruptPeer))

	corruptDB, err := badger.Open(badger.DefaultOptions(dbPath).WithLoggingLevel(badger.ERROR))
	if err != nil {
		t.Fatalf("open corrupt db: %v", err)
	}
	err = corruptDB.Update(func(txn *badger.Txn) error {
		return txn.Set(corruptKey, []byte("this is not valid json {{{"))
	})
	if err != nil {
		corruptDB.Close()
		t.Fatalf("write corrupt value: %v", err)
	}
	corruptDB.Close()

	// When: ListAll.
	store2, err := jwt.NewWhitelistStore(dbPath)
	if err != nil {
		t.Fatalf("reopen for read: %v", err)
	}
	defer func() { _ = store2.Close() }()

	all, err := store2.ListAll()

	// Then: no error, both entries returned, corrupt entry has empty metadata.
	if err != nil {
		t.Fatalf("listall should not error on corrupt JSON: %v", err)
	}
	if len(all) < 2 {
		t.Fatalf("expected at least 2 entries, got %d: %v", len(all), all)
	}

	var foundCorrupt, foundValid bool
	for _, e := range all {
		switch e.PeerID {
		case string(corruptPeer):
			foundCorrupt = true
			if !e.AddedAt.IsZero() {
				t.Errorf("corrupt entry AddedAt should be zero, got %v", e.AddedAt)
			}
			if e.AddedBy != "" {
				t.Errorf("corrupt entry AddedBy should be empty, got %q", e.AddedBy)
			}
		case string(validPeer):
			foundValid = true
			if e.AddedAt.IsZero() {
				t.Error("valid entry AddedAt should not be zero")
			}
			if e.AddedBy != "test" {
				t.Errorf("valid entry AddedBy: expected %q, got %q", "test", e.AddedBy)
			}
		}
	}
	if !foundCorrupt {
		t.Fatal("corrupt entry not found in ListAll")
	}
	if !foundValid {
		t.Fatal("valid entry not found in ListAll")
	}
}

func TestWhitelistStore_add_validatesJSONRoundTrip(t *testing.T) {
	// Given: multiple peers added with different metadata.
	dir := t.TempDir()
	dbPath := dir + "/whitelist"

	store, err := jwt.NewWhitelistStore(dbPath)
	if err != nil {
		t.Fatalf("new store: %v", err)
	}
	defer func() { _ = store.Close() }()

	peers := []struct {
		id      types.PeerId
		addedBy string
	}{
		{types.PeerId("12D3KooWUserA"), "alice"},
		{types.PeerId("12D3KooWUserB"), "bob"},
		{types.PeerId("12D3KooWUserC"), "carol"},
	}

	for _, p := range peers {
		if err := store.Add(p.id, p.addedBy); err != nil {
			t.Fatalf("add %s: %v", p.id, err)
		}
	}

	// When: ListAll.
	all, err := store.ListAll()
	if err != nil {
		t.Fatalf("listall: %v", err)
	}

	// Then: all entries match their metadata.
	if len(all) != len(peers) {
		t.Fatalf("expected %d entries, got %d", len(peers), len(all))
	}

	byID := make(map[string]jwt.WhitelistEntry, len(all))
	for _, e := range all {
		byID[e.PeerID] = e
	}

	for _, p := range peers {
		e, ok := byID[string(p.id)]
		if !ok {
			t.Errorf("missing entry for %s", p.id)
			continue
		}
		if e.AddedBy != p.addedBy {
			t.Errorf("%s AddedBy: expected %q, got %q", p.id, p.addedBy, e.AddedBy)
		}
		if e.AddedAt.IsZero() {
			t.Errorf("%s AddedAt should not be zero", p.id)
		}
		// Verify JSON round-trip works for each.
		raw, err := json.Marshal(e)
		if err != nil {
			t.Errorf("%s marshal: %v", p.id, err)
			continue
		}
		var decoded jwt.WhitelistEntry
		if err := json.Unmarshal(raw, &decoded); err != nil {
			t.Errorf("%s unmarshal: %v", p.id, err)
			continue
		}
		if decoded.PeerID != e.PeerID || decoded.AddedBy != e.AddedBy {
			t.Errorf("%s JSON round-trip mismatch", p.id)
		}
	}
}

func TestWhitelistStore_restoreFromLegacyValues(t *testing.T) {
	// Given: a legacy entry ([]byte{1}) written directly to BadgerDB.
	dir := t.TempDir()
	dbPath := dir + "/whitelist"

	legacyPeer := types.PeerId("12D3KooWRestoreLegacyPeer")
	legacyKey := []byte("w:" + string(legacyPeer))

	db, err := badger.Open(badger.DefaultOptions(dbPath).WithLoggingLevel(badger.ERROR))
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	err = db.Update(func(txn *badger.Txn) error {
		return txn.Set(legacyKey, []byte{1})
	})
	if err != nil {
		db.Close()
		t.Fatalf("write legacy: %v", err)
	}
	db.Close()

	// When: Restore from the legacy data.
	store, err := jwt.NewWhitelistStore(dbPath)
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	defer func() { _ = store.Close() }()

	ps := jwt.NewPeerIdSet()
	if err := store.Restore(ps); err != nil {
		t.Fatalf("restore: %v", err)
	}

	// Then: the legacy peer is restored into the PeerIdSet.
	if !ps.Contains(legacyPeer) {
		t.Fatal("legacy peer should be in PeerIdSet after Restore")
	}
}
