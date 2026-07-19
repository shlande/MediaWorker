package adminapi

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

type fakeWarmFlusher struct {
	calls   atomic.Int32
	blockCh chan struct{}
}

func (f *fakeWarmFlusher) Flush(ctx context.Context) error {
	f.calls.Add(1)
	if f.blockCh != nil {
		<-f.blockCh
	}
	return nil
}

func doFlushPost(t *testing.T, s *Server, body string, token string) *httptest.ResponseRecorder {
	t.Helper()
	req := httptest.NewRequest(http.MethodPost, "/v1/admin/flush-cache",
		strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	if token != "" {
		req.Header.Set(TokenHeader, token)
	}
	rr := httptest.NewRecorder()
	s.mux.ServeHTTP(rr, req)
	return rr
}

func assertFlushStatus(t *testing.T, rr *httptest.ResponseRecorder, wantStatus int, wantErr string) {
	t.Helper()
	if rr.Code != wantStatus {
		t.Fatalf("status = %d, want %d", rr.Code, wantStatus)
	}
	var body map[string]interface{}
	if err := json.Unmarshal(rr.Body.Bytes(), &body); err != nil {
		t.Fatalf("decode body: %v", err)
	}
	if wantErr != "" {
		if body["error"] != wantErr {
			t.Fatalf("error = %q, want %q", body["error"], wantErr)
		}
	}
}

func TestFlushHandler_HappyFlush(t *testing.T) {
	fake := &fakeWarmFlusher{}
	s := NewServer(testToken)
	RegisterFlushRoutes(s, fake)

	rr := doFlushPost(t, s, `{"partitions":["warm"]}`, testToken)
	if rr.Code != http.StatusAccepted {
		t.Fatalf("status = %d, want 202", rr.Code)
	}
	var resp flushResponse
	if err := json.Unmarshal(rr.Body.Bytes(), &resp); err != nil {
		t.Fatalf("decode body: %v", err)
	}
	if resp.Status != "flushing" {
		t.Fatalf("status field = %q, want \"flushing\"", resp.Status)
	}
	if len(resp.Partitions) != 1 || resp.Partitions[0] != "warm" {
		t.Fatalf("partitions = %v, want [\"warm\"]", resp.Partitions)
	}

	deadline := time.Now().Add(2 * time.Second)
	for fake.calls.Load() == 0 && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}
	if calls := fake.calls.Load(); calls != 1 {
		t.Fatalf("Flush calls: want 1, got %d", calls)
	}
}

func TestFlushHandler_PrefixRejection(t *testing.T) {
	s := NewServer(testToken)
	RegisterFlushRoutes(s, nil)
	rr := doFlushPost(t, s, `{"partitions":["prefix"]}`, testToken)
	assertFlushStatus(t, rr, http.StatusBadRequest, "prefix partition is pin-managed; use unpin")
}

func TestFlushHandler_ColdRejection(t *testing.T) {
	s := NewServer(testToken)
	RegisterFlushRoutes(s, nil)
	rr := doFlushPost(t, s, `{"partitions":["cold"]}`, testToken)
	assertFlushStatus(t, rr, http.StatusBadRequest, "cold partition is not wired")
}

func TestFlushHandler_UnknownPartition(t *testing.T) {
	s := NewServer(testToken)
	RegisterFlushRoutes(s, nil)
	rr := doFlushPost(t, s, `{"partitions":["garbage"]}`, testToken)
	if rr.Code != http.StatusBadRequest {
		t.Fatalf("unknown partition: status = %d, want 400", rr.Code)
	}
}

func TestFlushHandler_EmptyPartitions(t *testing.T) {
	s := NewServer(testToken)
	RegisterFlushRoutes(s, nil)
	rr := doFlushPost(t, s, `{"partitions":[]}`, testToken)
	assertFlushStatus(t, rr, http.StatusBadRequest, "partitions is required")
}

func TestFlushHandler_NilCache_409(t *testing.T) {
	s := NewServer(testToken)
	RegisterFlushRoutes(s, nil)
	rr := doFlushPost(t, s, `{"partitions":["warm"]}`, testToken)
	assertFlushStatus(t, rr, http.StatusConflict, "warm cache is not configured")
}

func TestFlushHandler_BadToken_401(t *testing.T) {
	s := NewServer(testToken)
	RegisterFlushRoutes(s, nil)

	rr := doFlushPost(t, s, `{"partitions":["warm"]}`, "")
	assertFlushStatus(t, rr, http.StatusUnauthorized, "invalid admin token")

	rr = doFlushPost(t, s, `{"partitions":["warm"]}`, "wrong-token")
	assertFlushStatus(t, rr, http.StatusUnauthorized, "invalid admin token")
}

func TestFlushHandler_InvalidJSON(t *testing.T) {
	s := NewServer(testToken)
	RegisterFlushRoutes(s, nil)
	rr := doFlushPost(t, s, `not json`, testToken)
	assertFlushStatus(t, rr, http.StatusBadRequest, "invalid request body")
}

func TestFlushHandler_MultiplePartitions(t *testing.T) {
	s := NewServer(testToken)
	RegisterFlushRoutes(s, nil)
	rr := doFlushPost(t, s, `{"partitions":["warm","prefix"]}`, testToken)
	assertFlushStatus(t, rr, http.StatusBadRequest, "prefix partition is pin-managed; use unpin")
}

func TestFlushHandler_SingleflightReentry(t *testing.T) {
	fake := &fakeWarmFlusher{blockCh: make(chan struct{})}
	s := NewServer(testToken)
	RegisterFlushRoutes(s, fake)

	var wg sync.WaitGroup
	results := make([]int, 2)

	wg.Add(2)
	for i := 0; i < 2; i++ {
		go func(idx int) {
			defer wg.Done()
			rr := doFlushPost(t, s, `{"partitions":["warm"]}`, testToken)
			results[idx] = rr.Code
		}(i)
	}

	time.Sleep(100 * time.Millisecond)
	wg.Wait()

	for i, code := range results {
		if code != http.StatusAccepted {
			t.Fatalf("response[%d] = %d, want 202", i, code)
		}
	}

	close(fake.blockCh)
	time.Sleep(100 * time.Millisecond)

	if calls := fake.calls.Load(); calls != 1 {
		t.Fatalf("Flush calls after singleflight: want 1, got %d", calls)
	}
}
