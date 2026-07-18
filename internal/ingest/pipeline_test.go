package ingest

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"strings"
	"sync"
	"testing"

	"github.com/shlande/mediaworker/internal/types"
)

// ─── mocks ───────────────────────────────────────────────────────────────

// mockBlobStoreWriter captures WriteIngestTransaction args for assertions.
type mockBlobStoreWriter struct {
	mu      sync.Mutex
	content *types.ContentMeta
	blobs   []types.BlobDescriptor
	roles   []types.BlobRole
	locs    []types.BlobLocation
	err     error
}

func (m *mockBlobStoreWriter) WriteIngestTransaction(
	_ context.Context,
	content types.ContentMeta,
	blobs []types.BlobDescriptor,
	roles []types.BlobRole,
	locations []types.BlobLocation,
) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.content = &content
	m.blobs = blobs
	m.roles = roles
	m.locs = locations
	return m.err
}

// mockBackendUploader returns a fixed BackendLocation on Put.
type mockBackendUploader struct {
	backendID string
	err       error
}

func (m *mockBackendUploader) Put(_ context.Context, blobHash string, reader io.Reader, _ int64) (BackendLocation, error) {
	// Drain reader to simulate upload behaviour.
	io.Copy(io.Discard, reader)
	if m.err != nil {
		return BackendLocation{}, m.err
	}
	return BackendLocation{BackendID: m.backendID, FileID: blobHash + "_file"}, nil
}

// mockBackendPool returns fixed mock uploaders.
type mockBackendPool struct {
	err       error
	uploaders []*mockBackendUploader
}

func (m *mockBackendPool) SelectKForUpload(_ int) ([]BackendUploader, error) {
	if m.err != nil {
		return nil, m.err
	}
	out := make([]BackendUploader, len(m.uploaders))
	for i, u := range m.uploaders {
		out[i] = u
	}
	return out, nil
}

// mockEventPublisher captures published events and optionally signals a channel.
type mockEventPublisher struct {
	mu   sync.Mutex
	evts []types.ContentIngestedEvent
	// done is optional — if non-nil, closed when event is published.
	done chan struct{}
}

func (m *mockEventPublisher) Publish(evt types.ContentIngestedEvent) {
	m.mu.Lock()
	m.evts = append(m.evts, evt)
	m.mu.Unlock()
	if m.done != nil {
		close(m.done)
	}
}

// stubIngester implements ContentIngester and returns a fixed ProcessResult.
type stubIngester struct {
	contentType string
	result      *ProcessResult
	err         error
}

func (s *stubIngester) ContentType() string { return s.contentType }

func (s *stubIngester) Process(_ context.Context, _ io.Reader, _ ProcessOptions) (*ProcessResult, error) {
	return s.result, s.err
}

// ─── helpers ────────────────────────────────────────────────────────────

func mustCreateFile(t *testing.T, dir, name string, content []byte) string {
	t.Helper()
	p := dir + "/" + name
	if err := os.WriteFile(p, content, 0644); err != nil {
		t.Fatalf("write %s: %v", p, err)
	}
	return p
}

func simpleBlob(hash, blobType string, size int64) types.BlobDescriptor {
	return types.BlobDescriptor{BlobHash: hash, BlobType: blobType, Size: size}
}

func buildPipeline(
	t *testing.T,
	contentType string,
	result *ProcessResult,
	ingestErr error,
	backends []*mockBackendUploader,
	poolErr error,
	storeErr error,
	eventBus *mockEventPublisher,
) *IngestPipeline {
	t.Helper()
	store := &mockBlobStoreWriter{err: storeErr}
	pool := &mockBackendPool{uploaders: backends, err: poolErr}
	p := NewIngestPipeline(pool, store, eventBus)
	p.RegisterIngester(&stubIngester{contentType: contentType, result: result, err: ingestErr})
	return p
}

// ─── tests ───────────────────────────────────────────────────────────────

func TestIngest_Success(t *testing.T) {
	tmpDir := t.TempDir()
	fileA := mustCreateFile(t, tmpDir, "blob_a.m4s", []byte("mock init segment"))
	fileB := mustCreateFile(t, tmpDir, "blob_b.m4s", []byte("mock media segment"))

	blobs := []types.BlobDescriptor{
		simpleBlob("sha256:aaa", "mp4_init_segment", 100),
		simpleBlob("sha256:bbb", "m4s_media_segment", 200),
	}
	roles := []types.BlobRole{
		{BlobHash: "sha256:aaa", Role: "init", SortOrder: 0},
		{BlobHash: "sha256:bbb", Role: "media", SortOrder: 1, BusinessMeta: map[string]any{"seg": 1}},
	}
	typeMeta := []byte(`{"mpd":"mock"}`)

	result := &ProcessResult{
		ContentID:    "cid-foo",
		ContentType:  "dash_video",
		Blobs:        blobs,
		Roles:        roles,
		TypeMetadata: typeMeta,
		BlobFiles: map[string]string{
			"sha256:aaa": fileA,
			"sha256:bbb": fileB,
		},
	}

	backends := []*mockBackendUploader{
		{backendID: "115:acct_01"},
		{backendID: "115:acct_02"},
	}
	done := make(chan struct{})
	eventBus := &mockEventPublisher{done: done}

	p := buildPipeline(t, "dash_video", result, nil, backends, nil, nil, eventBus)

	contentID, err := p.Ingest(context.Background(), "dash_video", strings.NewReader("dummy"), ProcessOptions{})
	if err != nil {
		t.Fatalf("Ingest failed: %v", err)
	}
	if contentID != "cid-foo" {
		t.Errorf("contentID = %s, want cid-foo", contentID)
	}

	store := p.blobStore.(*mockBlobStoreWriter)
	store.mu.Lock()
	{
		if store.content == nil {
			t.Fatal("WriteIngestTransaction was not called")
		}
		if store.content.ContentID != "cid-foo" {
			t.Errorf("ContentID = %s, want cid-foo", store.content.ContentID)
		}
		if store.content.ContentType != "dash_video" {
			t.Errorf("ContentType = %s, want dash_video", store.content.ContentType)
		}
		if len(store.blobs) != 2 {
			t.Errorf("len(blobs) = %d, want 2", len(store.blobs))
		}
		if len(store.roles) != 2 {
			t.Errorf("len(roles) = %d, want 2", len(store.roles))
		}
		if len(store.locs) != 4 { // 2 blobs × 2 backends
			t.Errorf("len(locations) = %d, want 4 (2 blobs × 2 backends)", len(store.locs))
		}
		for _, loc := range store.locs {
			if loc.BlobHash != "sha256:aaa" && loc.BlobHash != "sha256:bbb" {
				t.Errorf("unexpected BlobHash in location: %s", loc.BlobHash)
			}
			if loc.BackendID != "115:acct_01" && loc.BackendID != "115:acct_02" {
				t.Errorf("unexpected BackendID in location: %s", loc.BackendID)
			}
		}
	}
	store.mu.Unlock()

	// Wait for async event publication.
	<-done

	eventBus.mu.Lock()
	if len(eventBus.evts) == 0 {
		t.Fatal("no event published")
	}
	evt := eventBus.evts[0]
	if evt.ContentID != "cid-foo" {
		t.Errorf("event.ContentID = %s, want cid-foo", evt.ContentID)
	}
	if evt.ContentType != "dash_video" {
		t.Errorf("event.ContentType = %s, want dash_video", evt.ContentType)
	}
	if len(evt.Blobs) != 2 {
		t.Errorf("event len(Blobs) = %d, want 2", len(evt.Blobs))
	}
	if len(evt.Roles) != 2 {
		t.Errorf("event len(Roles) = %d, want 2", len(evt.Roles))
	}
	if evt.Timestamp <= 0 {
		t.Error("event Timestamp should be > 0")
	}
	eventBus.mu.Unlock()
}

func TestIngest_UnsupportedContentType(t *testing.T) {
	backends := []*mockBackendUploader{{backendID: "115:acct_01"}}
	eventBus := &mockEventPublisher{}
	p := buildPipeline(t, "dash_video", nil, nil, backends, nil, nil, eventBus)
	_, err := p.Ingest(context.Background(), "nonexistent_type", strings.NewReader("x"), ProcessOptions{})
	if err == nil {
		t.Fatal("expected error for unsupported content type")
	}
	if !strings.Contains(err.Error(), "unsupported content type") {
		t.Errorf("error = %v, want 'unsupported content type'", err)
	}
}

func TestIngest_ProcessFails(t *testing.T) {
	backends := []*mockBackendUploader{{backendID: "115:acct_01"}}
	eventBus := &mockEventPublisher{}
	p := buildPipeline(t, "dash_video", nil, fmt.Errorf("proc boom"), backends, nil, nil, eventBus)
	_, err := p.Ingest(context.Background(), "dash_video", strings.NewReader("x"), ProcessOptions{})
	if err == nil {
		t.Fatal("expected error from Process")
	}
	if !strings.Contains(err.Error(), "process:") {
		t.Errorf("error = %v, want to contain 'process:'", err)
	}
}

func TestIngest_UploadPartialFailure(t *testing.T) {
	tmpDir := t.TempDir()
	fileA := mustCreateFile(t, tmpDir, "blob_a.m4s", []byte("init content"))

	blobs := []types.BlobDescriptor{
		simpleBlob("sha256:aaa", "mp4_init_segment", 100),
	}
	result := &ProcessResult{
		ContentID:   "cid-partial",
		ContentType: "dash_video",
		Blobs:       blobs,
		Roles:       []types.BlobRole{{BlobHash: "sha256:aaa", Role: "init", SortOrder: 0}},
		BlobFiles:   map[string]string{"sha256:aaa": fileA},
	}

	backends := []*mockBackendUploader{
		{backendID: "115:ok"},
		{backendID: "115:fail", err: errors.New("upload failed")},
	}
	eventBus := &mockEventPublisher{}

	p := buildPipeline(t, "dash_video", result, nil, backends, nil, nil, eventBus)

	_, err := p.Ingest(context.Background(), "dash_video", strings.NewReader("x"), ProcessOptions{})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	store := p.blobStore.(*mockBlobStoreWriter)
	store.mu.Lock()
	if len(store.locs) != 1 { // only 1 successful backend
		t.Errorf("expected 1 location, got %d", len(store.locs))
	}
	if store.locs[0].BackendID != "115:ok" {
		t.Errorf("expected surviving backend 115:ok, got %s", store.locs[0].BackendID)
	}
	store.mu.Unlock()
}

func TestIngest_UploadAllFail(t *testing.T) {
	tmpDir := t.TempDir()
	fileA := mustCreateFile(t, tmpDir, "blob_a.m4s", []byte("init content"))

	blobs := []types.BlobDescriptor{
		simpleBlob("sha256:aaa", "mp4_init_segment", 100),
	}
	result := &ProcessResult{
		ContentID:   "cid-allfail",
		ContentType: "dash_video",
		Blobs:       blobs,
		Roles:       []types.BlobRole{{BlobHash: "sha256:aaa", Role: "init", SortOrder: 0}},
		BlobFiles:   map[string]string{"sha256:aaa": fileA},
	}

	backends := []*mockBackendUploader{
		{backendID: "115:fail1", err: errors.New("upload fail 1")},
		{backendID: "115:fail2", err: errors.New("upload fail 2")},
	}
	eventBus := &mockEventPublisher{}

	p := buildPipeline(t, "dash_video", result, nil, backends, nil, nil, eventBus)

	_, err := p.Ingest(context.Background(), "dash_video", strings.NewReader("x"), ProcessOptions{})
	if err == nil {
		t.Fatal("expected error when all backends fail")
	}
	if !strings.Contains(err.Error(), "blob upload") {
		t.Errorf("error = %v, want to contain 'blob upload'", err)
	}
}

func TestIngest_WriteTransactionFails(t *testing.T) {
	tmpDir := t.TempDir()
	fileA := mustCreateFile(t, tmpDir, "blob_a.m4s", []byte("init content"))

	blobs := []types.BlobDescriptor{
		simpleBlob("sha256:aaa", "mp4_init_segment", 100),
	}
	result := &ProcessResult{
		ContentID:   "cid-storefail",
		ContentType: "dash_video",
		Blobs:       blobs,
		Roles:       []types.BlobRole{{BlobHash: "sha256:aaa", Role: "init", SortOrder: 0}},
		BlobFiles:   map[string]string{"sha256:aaa": fileA},
	}

	backends := []*mockBackendUploader{{backendID: "115:acct_01"}}
	eventBus := &mockEventPublisher{}

	p := buildPipeline(t, "dash_video", result, nil, backends, nil, errors.New("store boom"), eventBus)

	_, err := p.Ingest(context.Background(), "dash_video", strings.NewReader("x"), ProcessOptions{})
	if err == nil {
		t.Fatal("expected error from WriteIngestTransaction")
	}
	if !strings.Contains(err.Error(), "store boom") {
		t.Errorf("error = %v, want to contain 'store boom'", err)
	}
}
