package app

import (
	"context"
	"errors"
	"io"
	"strings"
	"testing"
)

// F5 (live campaign): pins dispatched to edge nodes never completed because
// the pinstore fetchFunc consulted ONLY the warm cache, so a cold node had
// no source to fetch from and every pin landed in State=failed.
// Per docs/distribution/README.md §9.1, fetchPinnedBlob must walk the node's
// normal backhaul path (cache → ICP sibling → L4/data-plane). These tests
// pin that contract on makePinFetchFunc.

func TestMakePinFetchFunc_WarmHit_SkipsBackhaul(t *testing.T) {
	// Given a warm cache holding the blob and a backhaul that must not be consulted
	warmGet := func(string) ([]byte, bool) { return []byte("warm-data"), true }
	backhaulCalled := false
	backhaulFetch := func(context.Context, io.Writer, string) error {
		backhaulCalled = true
		return nil
	}
	fetch := makePinFetchFunc(warmGet, backhaulFetch)

	// When fetching the blob
	data, err := fetch("sha256:hit")

	// Then the warm bytes are returned and the network source stays untouched
	if err != nil {
		t.Fatalf("fetch: %v", err)
	}
	if string(data) != "warm-data" {
		t.Fatalf("data = %q, want warm-data", data)
	}
	if backhaulCalled {
		t.Fatal("backhaul consulted on a warm-cache hit")
	}
}

func TestMakePinFetchFunc_WarmMiss_UsesBackhaul(t *testing.T) {
	// Given an empty warm cache (cold node) and a backhaul serving the blob
	warmGet := func(string) ([]byte, bool) { return nil, false }
	backhaulFetch := func(_ context.Context, w io.Writer, blobHash string) error {
		if blobHash != "sha256:net" {
			t.Fatalf("backhaul got hash %q, want sha256:net", blobHash)
		}
		_, err := w.Write([]byte("net-data"))
		return err
	}
	fetch := makePinFetchFunc(warmGet, backhaulFetch)

	// When fetching the blob
	data, err := fetch("sha256:net")

	// Then the bytes come from the network source (this is the F5 regression case)
	if err != nil {
		t.Fatalf("fetch: %v", err)
	}
	if string(data) != "net-data" {
		t.Fatalf("data = %q, want net-data", data)
	}
}

func TestMakePinFetchFunc_BackhaulError_Propagates(t *testing.T) {
	// Given a warm miss and a backhaul whose sources are all unreachable
	warmGet := func(string) ([]byte, bool) { return nil, false }
	backhaulFetch := func(context.Context, io.Writer, string) error {
		return errors.New("blob not found (L4 unavailable)")
	}
	fetch := makePinFetchFunc(warmGet, backhaulFetch)

	// When fetching the blob
	_, err := fetch("sha256:gone")

	// Then the error propagates so fetchPinnedBlob marks the pin failed
	if err == nil {
		t.Fatal("expected error, got nil")
	}
	if !strings.Contains(err.Error(), "L4 unavailable") {
		t.Fatalf("err = %q, want it to wrap the backhaul failure", err)
	}
}

func TestMakePinFetchFunc_NoSources_ReturnsError(t *testing.T) {
	// Given neither a warm cache nor a backhaul source
	fetch := makePinFetchFunc(nil, nil)

	// When fetching the blob
	_, err := fetch("sha256:nothing")

	// Then a descriptive error is returned (pin → failed, retryable)
	if err == nil {
		t.Fatal("expected error, got nil")
	}
}
