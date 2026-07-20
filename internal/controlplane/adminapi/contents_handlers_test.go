package adminapi

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/shlande/mediaworker/internal/storage/metadata"
	"github.com/shlande/mediaworker/internal/types"
)

// ─── Mock helpers ─────────────────────────────────────────────────────────

type mockContentsListReader struct {
	rows  []metadata.AdminContentRow
	total int
	err   error
}

func (m *mockContentsListReader) ListContents(_ context.Context, _ metadata.ListContentsQuery) ([]metadata.AdminContentRow, int, error) {
	return m.rows, m.total, m.err
}

type mockContentsDetailReader struct {
	detail *metadata.AdminContentDetail
	err    error
}

func (m *mockContentsDetailReader) GetContentDetail(_ context.Context, contentID string) (*metadata.AdminContentDetail, error) {
	return m.detail, m.err
}

type mockPinCountReader struct {
	counts map[string]int
}

func (m *mockPinCountReader) CountByContent() map[string]int {
	return m.counts
}

type mockContentMetaReader struct {
	meta *types.ContentMeta
	err  error
}

func (m *mockContentMetaReader) GetContentMeta(_ context.Context, contentID string) (*types.ContentMeta, error) {
	return m.meta, m.err
}

type mockContentDeleter struct {
	err error
}

func (m *mockContentDeleter) SoftDeleteContent(_ context.Context, contentID string) error {
	return m.err
}

// ─── Server builder ───────────────────────────────────────────────────────

const contentsTestSecret = "test-secret-contents-handlers"

func makeContentsServer(mc struct {
	ContentsListReader
	ContentsDetailReader
	ContentMetaReader
}, dlog PinCountReader, deleter ContentDeleter) *Server {
	srv := NewServer([]byte(contentsTestSecret))
	RegisterContentsRoutes(srv, mc, dlog, deleter, nil)
	return srv
}

func contentsAuthToken(t *testing.T) string {
	t.Helper()
	token, err := SignUserToken(UserTokenPayload{
		UserID:   "user-1",
		Username: "root",
		Roles:    []string{"admin"},
		Iat:      time.Now().Unix(),
		Exp:      time.Now().Add(time.Hour).Unix(),
	}, []byte(contentsTestSecret))
	if err != nil {
		t.Fatalf("SignUserToken: %v", err)
	}
	return token
}

func contentsGet(t *testing.T, srv *Server, path, token string) *http.Response {
	t.Helper()
	ts := httptest.NewServer(srv.mux)
	t.Cleanup(ts.Close)

	req, err := http.NewRequest(http.MethodGet, ts.URL+path, nil)
	if err != nil {
		t.Fatalf("NewRequest: %v", err)
	}
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("GET %s: %v", path, err)
	}
	return resp
}

func decodeContentsList(t *testing.T, resp *http.Response) ([]contentRowResponse, int) {
	t.Helper()
	defer func() { _ = resp.Body.Close() }()
	var body struct {
		Contents []contentRowResponse `json:"contents"`
		Total    int                  `json:"total"`
		Page     int                  `json:"page"`
		PageSize int                  `json:"page_size"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
		t.Fatalf("decode body: %v", err)
	}
	return body.Contents, body.Total
}

func decodeContentDetail(t *testing.T, resp *http.Response) contentDetailResponse {
	t.Helper()
	defer func() { _ = resp.Body.Close() }()
	var body contentDetailResponse
	if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
		t.Fatalf("decode body: %v", err)
	}
	return body
}

func decodeError(t *testing.T, resp *http.Response) (int, string) {
	t.Helper()
	defer func() { _ = resp.Body.Close() }()
	var body map[string]string
	if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
		t.Fatalf("decode error body: %v", err)
	}
	return resp.StatusCode, body["error"]
}

// ─── List tests ───────────────────────────────────────────────────────────

func TestContentsList_HappyPath(t *testing.T) {
	mc := struct {
		ContentsListReader
		ContentsDetailReader
		ContentMetaReader
	}{
		ContentsListReader: &mockContentsListReader{
			rows: []metadata.AdminContentRow{
				{ContentID: "abc-123-xyz", Title: "My Video", ContentType: "dash", TotalBytes: 1024, BlobCount: 3, ReplicasHave: 2, Window24h: 500},
				{ContentID: "def-456-uvw", Title: "", ContentType: "image", TotalBytes: 512, BlobCount: 1, ReplicasHave: 1, Window24h: 100},
			},
			total: 2,
		},
	}

	dlog := &mockPinCountReader{
		counts: map[string]int{
			"abc-123-xyz": 3,
		},
	}

	srv := makeContentsServer(mc, dlog, nil)
	token := contentsAuthToken(t)

	resp := contentsGet(t, srv, "/v1/admin/contents", token)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200, got %d", resp.StatusCode)
	}

	contents, total := decodeContentsList(t, resp)

	if total != 2 {
		t.Errorf("total = %d, want 2", total)
	}
	if len(contents) != 2 {
		t.Fatalf("len(contents) = %d, want 2", len(contents))
	}

	c0 := contents[0]
	if c0.ContentID != "abc-123-xyz" {
		t.Errorf("ContentID[0] = %q, want abc-123-xyz", c0.ContentID)
	}
	if c0.Title != "My Video" {
		t.Errorf("Title[0] = %q, want My Video", c0.Title)
	}
	if c0.ContentType != "dash" {
		t.Errorf("ContentType[0] = %q, want dash", c0.ContentType)
	}
	if c0.TotalBytes != 1024 {
		t.Errorf("TotalBytes[0] = %d, want 1024", c0.TotalBytes)
	}
	if c0.BlobCount != 3 {
		t.Errorf("BlobCount[0] = %d, want 3", c0.BlobCount)
	}
	if c0.Replicas.Have != 2 || c0.Replicas.Want != 2 {
		t.Errorf("Replicas[0] = {%d, %d}, want {2,2}", c0.Replicas.Have, c0.Replicas.Want)
	}
	if c0.Window24h != 500 {
		t.Errorf("Window24h[0] = %d, want 500", c0.Window24h)
	}
	if c0.PinNodeCount != 3 {
		t.Errorf("PinNodeCount[0] = %d, want 3 (from dlog)", c0.PinNodeCount)
	}
	if c0.PendingDelete != false {
		t.Errorf("PendingDelete[0] = %v, want false", c0.PendingDelete)
	}

	c1 := contents[1]
	if c1.Title != "def-456-" {
		t.Errorf("Title[1] = %q, want fallback 'def-456-'", c1.Title)
	}
	if c1.PinNodeCount != 0 {
		t.Errorf("PinNodeCount[1] = %d, want 0 (no dlog entry)", c1.PinNodeCount)
	}
}

func TestContentsList_PinNodeCountMerge(t *testing.T) {
	mc := struct {
		ContentsListReader
		ContentsDetailReader
		ContentMetaReader
	}{
		ContentsListReader: &mockContentsListReader{
			rows: []metadata.AdminContentRow{
				{ContentID: "cont-aaa", Title: "Content A", ContentType: "dash", ReplicasHave: 2},
				{ContentID: "cont-bbb", Title: "Content B", ContentType: "dash", ReplicasHave: 1},
			},
			total: 2,
		},
	}

	dlog := &mockPinCountReader{
		counts: map[string]int{
			"cont-aaa": 5,
			"cont-bbb": 1,
			"cont-ccc": 9,
		},
	}

	srv := makeContentsServer(mc, dlog, nil)
	resp := contentsGet(t, srv, "/v1/admin/contents", contentsAuthToken(t))

	contents, _ := decodeContentsList(t, resp)
	if len(contents) != 2 {
		t.Fatalf("len = %d, want 2", len(contents))
	}
	if contents[0].PinNodeCount != 5 {
		t.Errorf("cont-aaa pin_node_count = %d, want 5", contents[0].PinNodeCount)
	}
	if contents[1].PinNodeCount != 1 {
		t.Errorf("cont-bbb pin_node_count = %d, want 1", contents[1].PinNodeCount)
	}
}

func TestContentsList_EmptyTitleFallback(t *testing.T) {
	mc := struct {
		ContentsListReader
		ContentsDetailReader
		ContentMetaReader
	}{
		ContentsListReader: &mockContentsListReader{
			rows: []metadata.AdminContentRow{
				{ContentID: "abcdefghijklmnop", Title: "", ContentType: "dash", ReplicasHave: 2},
				{ContentID: "short", Title: "", ContentType: "image", ReplicasHave: 0},
			},
			total: 2,
		},
	}

	dlog := &mockPinCountReader{counts: map[string]int{}}
	srv := makeContentsServer(mc, dlog, nil)
	resp := contentsGet(t, srv, "/v1/admin/contents", contentsAuthToken(t))

	contents, _ := decodeContentsList(t, resp)
	if len(contents) != 2 {
		t.Fatalf("len = %d, want 2", len(contents))
	}
	if contents[0].Title != "abcdefgh" {
		t.Errorf("Title fallback long = %q, want 'abcdefgh'", contents[0].Title)
	}
	if contents[1].Title != "short" {
		t.Errorf("Title fallback short = %q, want 'short'", contents[1].Title)
	}
}

func TestContentsList_ReplicasDegradedFilter(t *testing.T) {
	mc := struct {
		ContentsListReader
		ContentsDetailReader
		ContentMetaReader
	}{
		ContentsListReader: &mockContentsListReader{
			rows: []metadata.AdminContentRow{
				{ContentID: "full-ok", Title: "Healthy", ContentType: "dash", ReplicasHave: 2},
				{ContentID: "degraded", Title: "Degraded", ContentType: "dash", ReplicasHave: 1},
				{ContentID: "zero", Title: "Zero rep", ContentType: "dash", ReplicasHave: 0},
				{ContentID: "excess", Title: "Over reps", ContentType: "dash", ReplicasHave: 3},
			},
			total: 4,
		},
	}

	dlog := &mockPinCountReader{counts: map[string]int{}}

	srv := makeContentsServer(mc, dlog, nil)
	resp := contentsGet(t, srv, "/v1/admin/contents", contentsAuthToken(t))
	contents, total := decodeContentsList(t, resp)
	if total != 4 || len(contents) != 4 {
		t.Fatalf("unfiltered: total=%d, len=%d, want 4", total, len(contents))
	}

	resp = contentsGet(t, srv, "/v1/admin/contents?replicas=degraded", contentsAuthToken(t))
	contents, _ = decodeContentsList(t, resp)
	if len(contents) != 2 {
		t.Fatalf("degraded filter: len=%d, want 2", len(contents))
	}
	ids := make(map[string]bool)
	for _, c := range contents {
		ids[c.ContentID] = true
	}
	if !ids["degraded"] {
		t.Error("missing 'degraded' (have=1 < want=2)")
	}
	if !ids["zero"] {
		t.Error("missing 'zero' (have=0 < want=2)")
	}
	if ids["full-ok"] {
		t.Error("should NOT include 'full-ok' (have=2, not degraded)")
	}
	if ids["excess"] {
		t.Error("should NOT include 'excess' (have=3 >= want=2)")
	}
}

func TestContentsList_MetadataError(t *testing.T) {
	mc := struct {
		ContentsListReader
		ContentsDetailReader
		ContentMetaReader
	}{
		ContentsListReader: &mockContentsListReader{
			err: errors.New("db connection lost"),
		},
	}

	dlog := &mockPinCountReader{}
	srv := makeContentsServer(mc, dlog, nil)
	resp := contentsGet(t, srv, "/v1/admin/contents", contentsAuthToken(t))

	status, msg := decodeError(t, resp)
	if status != http.StatusInternalServerError {
		t.Errorf("status = %d, want 500", status)
	}
	if msg != "internal error" {
		t.Errorf("error msg = %q, want 'internal error'", msg)
	}
}

func TestContentsList_NoToken(t *testing.T) {
	mc := struct {
		ContentsListReader
		ContentsDetailReader
		ContentMetaReader
	}{}
	dlog := &mockPinCountReader{}
	srv := makeContentsServer(mc, dlog, nil)
	resp := contentsGet(t, srv, "/v1/admin/contents", "")

	if resp.StatusCode != http.StatusUnauthorized {
		t.Errorf("status = %d, want 401", resp.StatusCode)
	}
}

func TestContentsList_EmptyResults(t *testing.T) {
	mc := struct {
		ContentsListReader
		ContentsDetailReader
		ContentMetaReader
	}{
		ContentsListReader: &mockContentsListReader{
			rows:  []metadata.AdminContentRow{},
			total: 0,
		},
	}

	dlog := &mockPinCountReader{}
	srv := makeContentsServer(mc, dlog, nil)
	resp := contentsGet(t, srv, "/v1/admin/contents", contentsAuthToken(t))

	contents, total := decodeContentsList(t, resp)
	if total != 0 {
		t.Errorf("total = %d, want 0", total)
	}
	if len(contents) != 0 {
		t.Errorf("len(contents) = %d, want 0", len(contents))
	}
}

func TestContentsList_PaginatedParams(t *testing.T) {
	mc := struct {
		ContentsListReader
		ContentsDetailReader
		ContentMetaReader
	}{
		ContentsListReader: &mockContentsListReader{
			rows: []metadata.AdminContentRow{
				{ContentID: "pg-01", Title: "Page 1", ContentType: "dash", ReplicasHave: 2},
			},
			total: 42,
		},
	}

	dlog := &mockPinCountReader{}
	srv := makeContentsServer(mc, dlog, nil)
	resp := contentsGet(t, srv, "/v1/admin/contents?page=2&page_size=10&sort=popularity&type=dash", contentsAuthToken(t))

	contents, total := decodeContentsList(t, resp)
	if total != 42 {
		t.Errorf("total = %d, want 42", total)
	}
	if len(contents) != 1 {
		t.Errorf("len = %d, want 1", len(contents))
	}
}

// ─── Detail tests ─────────────────────────────────────────────────────────

func TestContentsDetail_HappyPath(t *testing.T) {
	deletedAt := time.Date(2026, 7, 1, 0, 0, 0, 0, time.UTC)
	mc := struct {
		ContentsListReader
		ContentsDetailReader
		ContentMetaReader
	}{
		ContentsDetailReader: &mockContentsDetailReader{
			detail: &metadata.AdminContentDetail{
				Meta: &types.ContentMeta{
					ContentID:    "11111111-1111-1111-1111-111111111111",
					ContentType:  "dash",
					TypeMetadata: []byte(`{"resolution":"1080p"}`),
					Title:        "Detail Video",
					DeletedAt:    &deletedAt,
				},
				Blobs: []metadata.AdminContentBlob{
					{Hash: "hash-init", Role: "init", SortOrder: 1, Size: 2048, BlobType: "mp4_init"},
					{Hash: "hash-seg1", Role: "segment", SortOrder: 2, Size: 4096, BlobType: "mp4_segment"},
				},
				Locations: []metadata.AdminContentLocation{
					{BlobHash: "hash-init", BackendID: "baidu:acct1", FileID: "fid-1", AccountHealth: strPtr("healthy")},
				},
			},
		},
	}

	dlog := &mockPinCountReader{}
	srv := makeContentsServer(mc, dlog, nil)
	resp := contentsGet(t, srv, "/v1/admin/contents/11111111-1111-1111-1111-111111111111", contentsAuthToken(t))

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200", resp.StatusCode)
	}

	detail := decodeContentDetail(t, resp)

	if detail.Meta.ContentID != "11111111-1111-1111-1111-111111111111" {
		t.Errorf("ContentID = %q", detail.Meta.ContentID)
	}
	if detail.Meta.Title != "Detail Video" {
		t.Errorf("Title = %q", detail.Meta.Title)
	}
	if detail.Meta.ContentType != "dash" {
		t.Errorf("ContentType = %q", detail.Meta.ContentType)
	}
	if string(detail.Meta.TypeMetadata) != `{"resolution":"1080p"}` {
		t.Errorf("TypeMetadata = %q", string(detail.Meta.TypeMetadata))
	}
	if !detail.Meta.PendingDelete {
		t.Error("PendingDelete = false, want true (deleted_at != nil)")
	}

	if len(detail.Blobs) != 2 {
		t.Fatalf("len(Blobs) = %d, want 2", len(detail.Blobs))
	}
	if detail.Blobs[0].Hash != "hash-init" {
		t.Errorf("Blobs[0].Hash = %q", detail.Blobs[0].Hash)
	}
	if detail.Blobs[0].Role != "init" {
		t.Errorf("Blobs[0].Role = %q", detail.Blobs[0].Role)
	}
	if detail.Blobs[0].Size != 2048 {
		t.Errorf("Blobs[0].Size = %d", detail.Blobs[0].Size)
	}
	if detail.Blobs[0].BlobType != "mp4_init" {
		t.Errorf("Blobs[0].BlobType = %q", detail.Blobs[0].BlobType)
	}

	if len(detail.Locations) != 1 {
		t.Fatalf("len(Locations) = %d, want 1", len(detail.Locations))
	}
	if detail.Locations[0].BlobHash != "hash-init" {
		t.Errorf("Locations[0].BlobHash = %q", detail.Locations[0].BlobHash)
	}
	if detail.Locations[0].BackendID != "baidu:acct1" {
		t.Errorf("Locations[0].BackendID = %q", detail.Locations[0].BackendID)
	}
	if detail.Locations[0].FileID != "fid-1" {
		t.Errorf("Locations[0].FileID = %q", detail.Locations[0].FileID)
	}
	if detail.Locations[0].AccountHealth == nil || *detail.Locations[0].AccountHealth != "healthy" {
		t.Errorf("Locations[0].AccountHealth = %v", detail.Locations[0].AccountHealth)
	}
}

func TestContentsDetail_NotFound(t *testing.T) {
	mc := struct {
		ContentsListReader
		ContentsDetailReader
		ContentMetaReader
	}{
		ContentsDetailReader: &mockContentsDetailReader{
			err: fmt.Errorf("wrap: %w", metadata.ErrContentNotFound),
		},
	}
	dlog := &mockPinCountReader{}
	srv := makeContentsServer(mc, dlog, nil)
	resp := contentsGet(t, srv, "/v1/admin/contents/00000000-0000-0000-0000-000000000000", contentsAuthToken(t))

	if resp.StatusCode != http.StatusNotFound {
		t.Errorf("status = %d, want 404", resp.StatusCode)
	}
}

func TestContentsDetail_PendingDelete(t *testing.T) {
	deletedAt := time.Date(2026, 6, 15, 0, 0, 0, 0, time.UTC)
	mc := struct {
		ContentsListReader
		ContentsDetailReader
		ContentMetaReader
	}{
		ContentsDetailReader: &mockContentsDetailReader{
			detail: &metadata.AdminContentDetail{
				Meta: &types.ContentMeta{
					ContentID:   "22222222-2222-2222-2222-222222222222",
					ContentType: "dash",
					DeletedAt:   &deletedAt,
				},
			},
		},
	}

	dlog := &mockPinCountReader{}
	srv := makeContentsServer(mc, dlog, nil)
	resp := contentsGet(t, srv, "/v1/admin/contents/22222222-2222-2222-2222-222222222222", contentsAuthToken(t))

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200", resp.StatusCode)
	}

	detail := decodeContentDetail(t, resp)
	if !detail.Meta.PendingDelete {
		t.Error("PendingDelete = false, want true for deleted_at!=nil content")
	}
}

func TestContentsDetail_NotDeleted(t *testing.T) {
	mc := struct {
		ContentsListReader
		ContentsDetailReader
		ContentMetaReader
	}{
		ContentsDetailReader: &mockContentsDetailReader{
			detail: &metadata.AdminContentDetail{
				Meta: &types.ContentMeta{
					ContentID:  "33333333-3333-3333-3333-333333333333",
					ContentType: "dash",
					DeletedAt:  nil,
				},
			},
		},
	}

	dlog := &mockPinCountReader{}
	srv := makeContentsServer(mc, dlog, nil)
	resp := contentsGet(t, srv, "/v1/admin/contents/33333333-3333-3333-3333-333333333333", contentsAuthToken(t))

	detail := decodeContentDetail(t, resp)
	if detail.Meta.PendingDelete {
		t.Error("PendingDelete = true, want false for DeletedAt==nil")
	}
}

func TestContentsDetail_MalformedID_Returns404(t *testing.T) {
	// The detail handler must reject non-UUID-shaped IDs before calling
	// the storage layer. content_id is UUID PRIMARY KEY; PostgreSQL
	// throws SQLSTATE 22P02 (invalid_input_syntax) for non-UUID strings,
	// which is NOT sql.ErrNoRows and NOT ErrContentNotFound — the handler
	// must catch it at the validation layer, not at the error-mapping layer.
	//
	// This test sends a non-UUID string and verifies 404.
	t.Run("non_uuid_string", func(t *testing.T) {
		mc := struct {
			ContentsListReader
			ContentsDetailReader
			ContentMetaReader
		}{
			ContentsDetailReader: &mockContentsDetailReader{
				// The mock should never be reached; the handler must
				// reject the malformed ID before calling GetContentDetail.
				// If it IS reached, the handler has a bug.
				err: errors.New("mock should not be called for malformed ID"),
			},
		}
		dlog := &mockPinCountReader{}
		srv := makeContentsServer(mc, dlog, nil)
		resp := contentsGet(t, srv, "/v1/admin/contents/nonexistent123", contentsAuthToken(t))
		if resp.StatusCode != http.StatusNotFound {
			t.Errorf("status = %d, want 404 for non-UUID content ID", resp.StatusCode)
		}
	})

	// A well-formed UUID that does not exist in the database must still
	// return 404 (this path goes through the storage layer).
	t.Run("absent_uuid", func(t *testing.T) {
		mc := struct {
			ContentsListReader
			ContentsDetailReader
			ContentMetaReader
		}{
			ContentsDetailReader: &mockContentsDetailReader{
				err: fmt.Errorf("metadata: content %q: %w", "123e4567-e89b-12d3-a456-426614174000", metadata.ErrContentNotFound),
			},
		}
		dlog := &mockPinCountReader{}
		srv := makeContentsServer(mc, dlog, nil)
		resp := contentsGet(t, srv, "/v1/admin/contents/123e4567-e89b-12d3-a456-426614174000", contentsAuthToken(t))
		if resp.StatusCode != http.StatusNotFound {
			t.Errorf("status = %d, want 404 for absent UUID", resp.StatusCode)
		}
	})

	// A genuine DB fault (non-"not found" error) must still return 500.
	t.Run("db_fault_still_500", func(t *testing.T) {
		mc := struct {
			ContentsListReader
			ContentsDetailReader
			ContentMetaReader
		}{
			ContentsDetailReader: &mockContentsDetailReader{
				err: fmt.Errorf("metadata: get content detail %q: %w", "123e4567-e89b-12d3-a456-426614174000", errors.New("connection reset")),
			},
		}
		dlog := &mockPinCountReader{}
		srv := makeContentsServer(mc, dlog, nil)
		resp := contentsGet(t, srv, "/v1/admin/contents/123e4567-e89b-12d3-a456-426614174000", contentsAuthToken(t))
		if resp.StatusCode != http.StatusInternalServerError {
			t.Errorf("status = %d, want 500 for genuine DB fault", resp.StatusCode)
		}
	})
}

func TestContentsDetail_MetadataError(t *testing.T) {
	mc := struct {
		ContentsListReader
		ContentsDetailReader
		ContentMetaReader
	}{
		ContentsDetailReader: &mockContentsDetailReader{
			err: errors.New("database crashed"),
		},
	}

	dlog := &mockPinCountReader{}
	srv := makeContentsServer(mc, dlog, nil)
	resp := contentsGet(t, srv, "/v1/admin/contents/44444444-4444-4444-4444-444444444444", contentsAuthToken(t))

	status, msg := decodeError(t, resp)
	if status != http.StatusInternalServerError {
		t.Errorf("status = %d, want 500", status)
	}
	if msg != "internal error" {
		t.Errorf("error msg = %q, want 'internal error'", msg)
	}
}

func TestContentsDetail_NoToken(t *testing.T) {
	mc := struct {
		ContentsListReader
		ContentsDetailReader
		ContentMetaReader
	}{}
	dlog := &mockPinCountReader{}
	srv := makeContentsServer(mc, dlog, nil)
	resp := contentsGet(t, srv, "/v1/admin/contents/99999999-9999-9999-9999-999999999999", "")

	if resp.StatusCode != http.StatusUnauthorized {
		t.Errorf("status = %d, want 401", resp.StatusCode)
	}
}

func TestContentsDetail_BadContentIDShort(t *testing.T) {
	mc := struct {
		ContentsListReader
		ContentsDetailReader
		ContentMetaReader
	}{
		ContentsDetailReader: &mockContentsDetailReader{},
	}
	dlog := &mockPinCountReader{}
	srv := makeContentsServer(mc, dlog, nil)
	resp := contentsGet(t, srv, "/v1/admin/contents/ab", contentsAuthToken(t))

	if resp.StatusCode != http.StatusNotFound {
		t.Errorf("status = %d, want 404 for short content ID", resp.StatusCode)
	}
}

func TestContentsDetail_TitleFallback(t *testing.T) {
	mc := struct {
		ContentsListReader
		ContentsDetailReader
		ContentMetaReader
	}{
		ContentsDetailReader: &mockContentsDetailReader{
			detail: &metadata.AdminContentDetail{
				Meta: &types.ContentMeta{
					ContentID: "55555555-5555-5555-5555-555555555555",
					Title:     "",
				},
			},
		},
	}

	dlog := &mockPinCountReader{}
	srv := makeContentsServer(mc, dlog, nil)
	resp := contentsGet(t, srv, "/v1/admin/contents/55555555-5555-5555-5555-555555555555", contentsAuthToken(t))

	detail := decodeContentDetail(t, resp)
	if detail.Meta.Title != "55555555" {
		t.Errorf("Title fallback = %q, want 'titleles' (first 8 chars)", detail.Meta.Title)
	}
}

func TestContentsDetail_NilBlobsAndLocations(t *testing.T) {
	mc := struct {
		ContentsListReader
		ContentsDetailReader
		ContentMetaReader
	}{
		ContentsDetailReader: &mockContentsDetailReader{
			detail: &metadata.AdminContentDetail{
				Meta: &types.ContentMeta{
					ContentID:  "66666666-6666-6666-6666-666666666666",
					ContentType: "dash",
				},
				Blobs:     nil,
				Locations: nil,
			},
		},
	}

	dlog := &mockPinCountReader{}
	srv := makeContentsServer(mc, dlog, nil)
	resp := contentsGet(t, srv, "/v1/admin/contents/66666666-6666-6666-6666-666666666666", contentsAuthToken(t))

	detail := decodeContentDetail(t, resp)
	if detail.Blobs == nil {
		t.Error("Blobs should be non-nil empty array")
	}
	if len(detail.Blobs) != 0 {
		t.Errorf("len(Blobs) = %d, want 0", len(detail.Blobs))
	}
	if detail.Locations == nil {
		t.Error("Locations should be non-nil empty array")
	}
	if len(detail.Locations) != 0 {
		t.Errorf("len(Locations) = %d, want 0", len(detail.Locations))
	}
}

// ─── Route prefix clash check ─────────────────────────────────────────────

func TestContentsRoutes_NoPrefixClash(t *testing.T) {
	mc := struct {
		ContentsListReader
		ContentsDetailReader
		ContentMetaReader
	}{
		ContentsListReader: &mockContentsListReader{
			rows:  []metadata.AdminContentRow{},
			total: 0,
		},
		ContentsDetailReader: &mockContentsDetailReader{
			err: fmt.Errorf("wrap: %w", metadata.ErrContentNotFound),
		},
	}

	dlog := &mockPinCountReader{}
	srv := makeContentsServer(mc, dlog, nil)
	token := contentsAuthToken(t)

	resp := contentsGet(t, srv, "/v1/admin/contents", token)
	if resp.StatusCode != http.StatusOK {
		t.Errorf("list status = %d, want 200", resp.StatusCode)
	}

	resp = contentsGet(t, srv, "/v1/admin/contents/77777777-7777-7777-7777-777777777777", token)
	if resp.StatusCode != http.StatusNotFound {
		t.Errorf("detail status = %d, want 404", resp.StatusCode)
	}

	resp = contentsGet(t, srv, "/v1/admin/contents?page=1", token)
	if resp.StatusCode != http.StatusOK {
		t.Errorf("list with query status = %d, want 200", resp.StatusCode)
	}
}

// ─── Helpers ──────────────────────────────────────────────────────────────

func strPtr(s string) *string {
	return &s
}



var _ = strings.Repeat

// ─── Delete helpers ────────────────────────────────────────────────────────

func contentsDelete(t *testing.T, srv *Server, path, token string) *http.Response {
	t.Helper()
	ts := httptest.NewServer(srv.mux)
	t.Cleanup(ts.Close)

	req, err := http.NewRequest(http.MethodDelete, ts.URL+path, nil)
	if err != nil {
		t.Fatalf("NewRequest DELETE: %v", err)
	}
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("DELETE %s: %v", path, err)
	}
	return resp
}

func decodeDeleteResponse(t *testing.T, resp *http.Response) contentDeleteResponse {
	t.Helper()
	defer func() { _ = resp.Body.Close() }()
	var body contentDeleteResponse
	if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
		t.Fatalf("decode delete response: %v", err)
	}
	return body
}

// ─── Delete tests ──────────────────────────────────────────────────────────

func TestDeleteContent_HappyPath(t *testing.T) {
	mc := struct {
		ContentsListReader
		ContentsDetailReader
		ContentMetaReader
	}{
		ContentMetaReader: &mockContentMetaReader{
			meta: &types.ContentMeta{
				ContentID:   "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa",
				ContentType: "dash",
			},
		},
	}
	deleter := &mockContentDeleter{}
	srv := makeContentsServer(mc, &mockPinCountReader{}, deleter)
	token := contentsAuthToken(t)

	resp := contentsDelete(t, srv, "/v1/admin/contents/aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa", token)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200", resp.StatusCode)
	}

	body := decodeDeleteResponse(t, resp)
	if body.ContentID != "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa" {
		t.Errorf("content_id = %q", body.ContentID)
	}
	if !body.PendingDelete {
		t.Error("pending_delete = false, want true")
	}
	if body.Note != deleteNoteFirst {
		t.Errorf("note = %q, want %q", body.Note, deleteNoteFirst)
	}
}

func TestDeleteContent_AlreadyDeleted(t *testing.T) {
	deletedAt := time.Date(2026, 7, 1, 0, 0, 0, 0, time.UTC)
	mc := struct {
		ContentsListReader
		ContentsDetailReader
		ContentMetaReader
	}{
		ContentMetaReader: &mockContentMetaReader{
			meta: &types.ContentMeta{
				ContentID:   "bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb",
				ContentType: "dash",
				DeletedAt:   &deletedAt,
			},
		},
	}
	deleter := &mockContentDeleter{}
	srv := makeContentsServer(mc, &mockPinCountReader{}, deleter)
	token := contentsAuthToken(t)

	resp := contentsDelete(t, srv, "/v1/admin/contents/bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb", token)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200", resp.StatusCode)
	}

	body := decodeDeleteResponse(t, resp)
	if body.ContentID != "bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb" {
		t.Errorf("content_id = %q", body.ContentID)
	}
	if !body.PendingDelete {
		t.Error("pending_delete = false, want true")
	}
	if body.Note != deleteNoteAlreadyDeleted {
		t.Errorf("note = %q, want %q", body.Note, deleteNoteAlreadyDeleted)
	}
}

func TestDeleteContent_NotFound(t *testing.T) {
	mc := struct {
		ContentsListReader
		ContentsDetailReader
		ContentMetaReader
	}{
		ContentMetaReader: &mockContentMetaReader{
			err: sql.ErrNoRows,
		},
	}
	srv := makeContentsServer(mc, &mockPinCountReader{}, &mockContentDeleter{})
	token := contentsAuthToken(t)

	resp := contentsDelete(t, srv, "/v1/admin/contents/cccccccc-cccc-cccc-cccc-cccccccccccc", token)
	if resp.StatusCode != http.StatusNotFound {
		t.Errorf("status = %d, want 404", resp.StatusCode)
	}
}

func TestDeleteContent_MetadataError(t *testing.T) {
	mc := struct {
		ContentsListReader
		ContentsDetailReader
		ContentMetaReader
	}{
		ContentMetaReader: &mockContentMetaReader{
			err: errors.New("db crashed"),
		},
	}
	srv := makeContentsServer(mc, &mockPinCountReader{}, &mockContentDeleter{})
	token := contentsAuthToken(t)

	resp := contentsDelete(t, srv, "/v1/admin/contents/dddddddd-dddd-dddd-dddd-dddddddddddd", token)
	status, msg := decodeError(t, resp)
	if status != http.StatusInternalServerError {
		t.Errorf("status = %d, want 500", status)
	}
	if msg != "internal error" {
		t.Errorf("error msg = %q, want 'internal error'", msg)
	}
}

func TestDeleteContent_DeleterError(t *testing.T) {
	mc := struct {
		ContentsListReader
		ContentsDetailReader
		ContentMetaReader
	}{
		ContentMetaReader: &mockContentMetaReader{
			meta: &types.ContentMeta{
				ContentID:   "eeeeeeee-eeee-eeee-eeee-eeeeeeeeeeee",
				ContentType: "dash",
			},
		},
	}
	deleter := &mockContentDeleter{err: errors.New("tx commit failed")}
	srv := makeContentsServer(mc, &mockPinCountReader{}, deleter)
	token := contentsAuthToken(t)

	resp := contentsDelete(t, srv, "/v1/admin/contents/eeeeeeee-eeee-eeee-eeee-eeeeeeeeeeee", token)
	status, msg := decodeError(t, resp)
	if status != http.StatusInternalServerError {
		t.Errorf("status = %d, want 500", status)
	}
	if msg != "internal error" {
		t.Errorf("error msg = %q, want 'internal error'", msg)
	}
}

func TestDeleteContent_NoToken(t *testing.T) {
	mc := struct {
		ContentsListReader
		ContentsDetailReader
		ContentMetaReader
	}{}
	srv := makeContentsServer(mc, &mockPinCountReader{}, &mockContentDeleter{})

	resp := contentsDelete(t, srv, "/v1/admin/contents/ffffffff-ffff-ffff-ffff-ffffffffffff", "")
	if resp.StatusCode != http.StatusUnauthorized {
		t.Errorf("status = %d, want 401", resp.StatusCode)
	}
}

func TestDeleteContent_ShortID(t *testing.T) {
	mc := struct {
		ContentsListReader
		ContentsDetailReader
		ContentMetaReader
	}{}
	srv := makeContentsServer(mc, &mockPinCountReader{}, &mockContentDeleter{})
	token := contentsAuthToken(t)

	resp := contentsDelete(t, srv, "/v1/admin/contents/ab", token)
	if resp.StatusCode != http.StatusNotFound {
		t.Errorf("status = %d, want 404", resp.StatusCode)
	}
}

func TestDeleteContent_SubsequentGetShowsPendingDelete(t *testing.T) {
	deletedAt := time.Date(2026, 7, 20, 0, 0, 0, 0, time.UTC)

	detailReader := &mockContentsDetailReader{
		detail: &metadata.AdminContentDetail{
			Meta: &types.ContentMeta{
				ContentID:   "deadbeef-dead-beef-dead-beefdeadbeef",
				ContentType: "dash",
				DeletedAt:   &deletedAt,
			},
		},
	}

	mc := struct {
		ContentsListReader
		ContentsDetailReader
		ContentMetaReader
	}{
		ContentsDetailReader: detailReader,
	}
	srv := makeContentsServer(mc, &mockPinCountReader{}, &mockContentDeleter{})
	token := contentsAuthToken(t)

	resp := contentsGet(t, srv, "/v1/admin/contents/deadbeef-dead-beef-dead-beefdeadbeef", token)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("detail status = %d, want 200", resp.StatusCode)
	}

	detail := decodeContentDetail(t, resp)
	if !detail.Meta.PendingDelete {
		t.Error("PendingDelete = false, want true after delete")
	}
}
