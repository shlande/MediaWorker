package adminapi

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

// ---------------------------------------------------------------------------
// TestPromClient_QueryScalar
// ---------------------------------------------------------------------------

func TestPromClient_QueryScalar_Vector(t *testing.T) {
	// Given: a Prometheus API that returns a valid vector response
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprint(w, `{
			"status": "success",
			"data": {
				"resultType": "vector",
				"result": [{"value": [1740000000, "0.8542"]}]
			}
		}`)
	}))
	defer srv.Close()

	c := NewPromClient(srv.URL)

	// When: QueryScalar is called with a valid PromQL
	val, ok, err := c.QueryScalar(context.Background(), `sum(rate(edge_cache_hit_total[5m]))`)

	// Then: the value is parsed correctly
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !ok {
		t.Fatal("expected ok=true for vector response")
	}
	if val != 0.8542 {
		t.Errorf("expected 0.8542, got %f", val)
	}
}

func TestPromClient_QueryScalar_EmptyResult(t *testing.T) {
	// Given: a Prometheus API that returns an empty vector result
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprint(w, `{
			"status": "success",
			"data": {
				"resultType": "vector",
				"result": []
			}
		}`)
	}))
	defer srv.Close()

	c := NewPromClient(srv.URL)

	// When: QueryScalar is called
	val, ok, err := c.QueryScalar(context.Background(), `sum(rate(edge_cache_hit_total[5m]))`)

	// Then: it returns (0, false, nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if ok {
		t.Fatal("expected ok=false for empty result")
	}
	if val != 0 {
		t.Errorf("expected val=0, got %f", val)
	}
}

func TestPromClient_QueryScalar_NonVector(t *testing.T) {
	// Given: a Prometheus API that returns a "matrix" resultType (query_range)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprint(w, `{
			"status": "success",
			"data": {
				"resultType": "matrix",
				"result": [{"values": [[1740000000, "0.5"]]}]
			}
		}`)
	}))
	defer srv.Close()

	c := NewPromClient(srv.URL)

	// When: QueryScalar gets a non-vector resultType
	val, ok, err := c.QueryScalar(context.Background(), `rate(edge_cache_hit_total[5m])`)

	// Then: it returns (0, false, nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if ok {
		t.Fatal("expected ok=false for non-vector resultType")
	}
	if val != 0 {
		t.Errorf("expected val=0, got %f", val)
	}
}

func TestPromClient_QueryScalar_5xx(t *testing.T) {
	// Given: a Prometheus endpoint that returns 503
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusServiceUnavailable)
		fmt.Fprint(w, "Service Unavailable")
	}))
	defer srv.Close()

	c := NewPromClient(srv.URL)

	// When: QueryScalar is called
	_, _, err := c.QueryScalar(context.Background(), `up`)

	// Then: it returns an error
	if err == nil {
		t.Fatal("expected error for 5xx response")
	}
}

func TestPromClient_QueryScalar_Disabled(t *testing.T) {
	// Given: a PromClient with empty baseURL
	c := NewPromClient("")

	// When: QueryScalar is called
	val, ok, err := c.QueryScalar(context.Background(), `up`)

	// Then: it returns (0, false, nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if ok {
		t.Fatal("expected ok=false when disabled")
	}
	if val != 0 {
		t.Errorf("expected val=0, got %f", val)
	}
}

func TestPromClient_QueryScalar_Timeout(t *testing.T) {
	// Given: a Prometheus endpoint that sleeps longer than the 5s timeout
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		time.Sleep(6 * time.Second)
	}))
	defer srv.Close()

	c := NewPromClient(srv.URL)

	// When: QueryScalar is called
	start := time.Now()
	_, _, err := c.QueryScalar(context.Background(), `up`)
	elapsed := time.Since(start)

	// Then: it returns an error within reasonable time (< 6s, no hang)
	if err == nil {
		t.Fatal("expected timeout error")
	}
	if elapsed > 6*time.Second {
		t.Fatalf("test hung: elapsed=%v", elapsed)
	}
	// httptest.Server doesn't enforce client timeouts — the client-side
	// http.Client.Timeout=5s kicks in. We verify the error exists and
	// we didn't hang.
	t.Logf("timeout test: elapsed=%v, err=%v", elapsed, err)
}

// ---------------------------------------------------------------------------
// TestPromClient_PreCanned
// ---------------------------------------------------------------------------

func TestPromClient_CacheHitRate(t *testing.T) {
	// Given: a Prometheus API returning a valid vector for cache hit rate
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprint(w, `{
			"status": "success",
			"data": {
				"resultType": "vector",
				"result": [{"value": [1740000000, "0.87"]}]
			}
		}`)
	}))
	defer srv.Close()

	c := NewPromClient(srv.URL)

	// When: CacheHitRate is called
	val, ok, err := c.CacheHitRate(context.Background())

	// Then: the value is parsed
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !ok {
		t.Fatal("expected ok=true")
	}
	if val != 0.87 {
		t.Errorf("expected 0.87, got %f", val)
	}
}

func TestPromClient_TTFBP95(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprint(w, `{
			"status": "success",
			"data": {
				"resultType": "vector",
				"result": [{"value": [1740000000, "1.234"]}]
			}
		}`)
	}))
	defer srv.Close()

	c := NewPromClient(srv.URL)
	val, ok, err := c.TTFBP95(context.Background())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !ok {
		t.Fatal("expected ok=true")
	}
	if val != 1.234 {
		t.Errorf("expected 1.234, got %f", val)
	}
}

func TestPromClient_BackhaulBandwidthBps(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprint(w, `{
			"status": "success",
			"data": {
				"resultType": "vector",
				"result": [{"value": [1740000000, "8000000"]}]
			}
		}`)
	}))
	defer srv.Close()

	c := NewPromClient(srv.URL)
	val, ok, err := c.BackhaulBandwidthBps(context.Background())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !ok {
		t.Fatal("expected ok=true")
	}
	if val != 8000000 {
		t.Errorf("expected 8000000, got %f", val)
	}
}

func TestPromClient_Enabled(t *testing.T) {
	t.Run("enabled", func(t *testing.T) {
		c := NewPromClient("http://localhost:9090")
		if !c.Enabled() {
			t.Fatal("expected Enabled=true with non-empty baseURL")
		}
	})
	t.Run("disabled", func(t *testing.T) {
		c := NewPromClient("")
		if c.Enabled() {
			t.Fatal("expected Enabled=false with empty baseURL")
		}
	})
}
