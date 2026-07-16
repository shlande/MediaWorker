package monitor

import (
	"context"
	"net/http"
	"time"
)

// MetricsServer wraps an HTTP server that exposes Prometheus metrics on /metrics.
// Storage-layer code should instantiate one per process (e.g. alongside the L4 node).
type MetricsServer struct {
	server *http.Server
}

// NewMetricsServer creates an http.Server that serves the given StorageMetrics
// on /metrics. The addr is a listen address like ":2112".
func NewMetricsServer(addr string, m *StorageMetrics) *MetricsServer {
	mux := http.NewServeMux()
	mux.Handle("/metrics", m.HTTPHandler())

	return &MetricsServer{
		server: &http.Server{
			Addr:         addr,
			Handler:      mux,
			ReadTimeout:  5 * time.Second,
			WriteTimeout: 10 * time.Second,
			IdleTimeout:  15 * time.Second,
		},
	}
}

// Start begins listening. Blocks until ctx is cancelled or the server fails.
func (s *MetricsServer) Start(ctx context.Context) error {
	errCh := make(chan error, 1)
	go func() {
		if err := s.server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			errCh <- err
		}
	}()

	select {
	case <-ctx.Done():
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		return s.server.Shutdown(shutdownCtx)
	case err := <-errCh:
		return err
	}
}

// Addr returns the configured listen address.
func (s *MetricsServer) Addr() string {
	return s.server.Addr
}