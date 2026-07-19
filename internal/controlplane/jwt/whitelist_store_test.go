package jwt_test

import (
	"os"
	"testing"

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
	if err := store.Add(peer); err != nil {
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
	if len(all) != 1 || all[0] != peer {
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
	if err := store.Add(peerA); err != nil {
		t.Fatalf("add peerA: %v", err)
	}
	if err := store.Add(peerB); err != nil {
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
	if err := store.Add(peer1); err != nil {
		t.Fatalf("add peer1: %v", err)
	}
	if err := store.Add(peer2); err != nil {
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
