package reporter

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"sync"
	"testing"
	"time"

	"github.com/libp2p/go-libp2p/core/peer"

	"github.com/shlande/mediaworker/internal/types"
)

// lockedBuffer is a thread-safe bytes.Buffer for capturing slog output while
// the reporter goroutine logs concurrently.
type lockedBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *lockedBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *lockedBuffer) count(sub []byte) int {
	b.mu.Lock()
	defer b.mu.Unlock()
	return bytes.Count(b.buf.Bytes(), sub)
}

func (b *lockedBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}

// sendCall records one SendToControlPlane invocation.
type sendCall struct {
	at        time.Time
	cp        peer.ID
	eventType string
	payload   any
}

// mockSender is a thread-safe recording stub for the send seam.
type mockSender struct {
	mu      sync.Mutex
	calls   []sendCall
	failErr error // non-nil: every send returns this error
}

func (m *mockSender) send(_ context.Context, cp peer.ID, eventType string, payload any) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.calls = append(m.calls, sendCall{at: time.Now(), cp: cp, eventType: eventType, payload: payload})
	return m.failErr
}

func (m *mockSender) count() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return len(m.calls)
}

func (m *mockSender) snapshot() []sendCall {
	m.mu.Lock()
	defer m.mu.Unlock()
	return append([]sendCall(nil), m.calls...)
}

// waitForCalls polls until the mock records at least n sends or the deadline
// expires, returning the elapsed time at the moment the n-th send was seen.
func waitForCalls(m *mockSender, n int, timeout time.Duration) (elapsed time.Duration, ok bool) {
	start := time.Now()
	deadline := start.Add(timeout)
	for time.Now().Before(deadline) {
		if m.count() >= n {
			return time.Since(start), true
		}
		time.Sleep(2 * time.Millisecond)
	}
	return time.Since(start), false
}

func fullCollect() func() types.NodeStatusReport {
	return func() types.NodeStatusReport {
		return types.NodeStatusReport{
			NodeID: "12D3KooWNode",
			PeerID: "12D3KooWNode",
			Capabilities: types.NodeCapabilities{
				Edge:          true,
				L4Backhaul:    true,
				RelayProvider: false,
				PeerICP:       true,
			},
			PrefixSpace:       types.PartitionStatus{TotalBytes: 100, UsedBytes: 40, BlobCount: 7},
			WarmSpace:         types.PartitionStatus{TotalBytes: 200, UsedBytes: 90, BlobCount: 11},
			Healthy:           true,
			LastUpdate:        1_700_000_000,
			Region:            "cn-shanghai",
			Version:           "v1.2.3",
			StartedAt:         1_699_999_000,
			ConnCount:         5,
			ColdSpace:         nil,
			JWTRefreshFail24h: 3,
		}
	}
}

// TestReporter_Run_SendsPeriodicallyAtInterval verifies that with a 50ms
// interval the reporter sends at least twice within a bounded window.
func TestReporter_Run_SendsPeriodicallyAtInterval(t *testing.T) {
	// Given: a reporter with a 50ms cadence and a recording send stub
	mock := &mockSender{}
	r := NewReporter(Config{CP: "12D3KooWCP", Interval: 50 * time.Millisecond, Collect: fullCollect()})
	r.send = mock.send

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go r.Run(ctx)

	// When: we wait for two sends
	// Then: both arrive within a bounded window (expected ~100ms; the 500ms
	// ceiling leaves CI headroom while still catching a broken 30s default)
	elapsed, ok := waitForCalls(mock, 2, 500*time.Millisecond)
	if !ok {
		t.Fatalf("expected >=2 sends within 500ms, got %d", mock.count())
	}
	calls := mock.snapshot()
	if gap := calls[1].at.Sub(calls[0].at); gap < 30*time.Millisecond {
		t.Fatalf("sends bursting: gap between sends %v < 30ms (interval not honoured)", gap)
	}
	t.Logf("2 sends observed after %v", elapsed)
}

// TestReporter_Run_SendsCollectedReportVerbatim verifies the payload pushed to
// the CP is exactly the report produced by collect, with every field intact.
func TestReporter_Run_SendsCollectedReportVerbatim(t *testing.T) {
	// Given: a collect that returns a fully-populated report
	mock := &mockSender{}
	r := NewReporter(Config{CP: "12D3KooWCP", Interval: 20 * time.Millisecond, Collect: fullCollect()})
	r.send = mock.send

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go r.Run(ctx)

	// When: one send lands
	if _, ok := waitForCalls(mock, 1, 2*time.Second); !ok {
		t.Fatal("no send observed")
	}

	// Then: event type, CP target, and every report field match collect's output
	call := mock.snapshot()[0]
	if call.eventType != EventType {
		t.Fatalf("event type: want %q, got %q", EventType, call.eventType)
	}
	if call.cp != peer.ID("12D3KooWCP") {
		t.Fatalf("cp target: want 12D3KooWCP, got %s", call.cp)
	}
	report, ok := call.payload.(types.NodeStatusReport)
	if !ok {
		t.Fatalf("payload type: want types.NodeStatusReport, got %T", call.payload)
	}
	want := fullCollect()()
	if report.NodeID != want.NodeID ||
		report.PeerID != want.PeerID ||
		report.Capabilities != want.Capabilities ||
		report.PrefixSpace != want.PrefixSpace ||
		report.WarmSpace != want.WarmSpace ||
		report.Healthy != want.Healthy ||
		report.LastUpdate != want.LastUpdate ||
		report.Region != want.Region ||
		report.Version != want.Version ||
		report.StartedAt != want.StartedAt ||
		report.ConnCount != want.ConnCount ||
		report.ColdSpace != nil ||
		report.JWTRefreshFail24h != want.JWTRefreshFail24h {
		t.Fatalf("report mismatch:\n got %+v\nwant %+v", report, want)
	}
}

// TestReporter_Run_SendFailureWarnsAndSurvives verifies that when the CP is
// unreachable (send returns error) the failure is Warn-logged and the
// goroutine survives to send again on the next cycle.
func TestReporter_Run_SendFailureWarnsAndSurvives(t *testing.T) {
	// Given: a send stub that always fails (CP unreachable)
	mock := &mockSender{failErr: errors.New("dial backoff")}
	logBuf := &lockedBuffer{}
	logger := slog.New(slog.NewTextHandler(logBuf, nil))
	r := NewReporter(Config{CP: "12D3KooWCP", Interval: 30 * time.Millisecond, Collect: fullCollect(), Logger: logger})
	r.send = mock.send

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan struct{})
	go func() { r.Run(ctx); close(done) }()

	// When: multiple cycles elapse with every send failing
	// Then: the loop keeps attempting (>=3 sends) and Warns accumulate
	if _, ok := waitForCalls(mock, 3, 2*time.Second); !ok {
		t.Fatalf("goroutine did not survive send failures: got %d sends", mock.count())
	}
	warnDeadline := time.Now().Add(2 * time.Second)
	for logBuf.count([]byte("send failed")) < 3 && time.Now().Before(warnDeadline) {
		time.Sleep(2 * time.Millisecond)
	}
	if got := logBuf.count([]byte("send failed")); got < 3 {
		t.Fatalf("expected >=3 send-failure Warns, got %d; log: %s", got, logBuf.String())
	}

	// And: cancelling the context stops the loop cleanly
	cancel()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("Run did not return after context cancel")
	}
}

// TestReporter_Run_StopsOnContextCancel verifies no sends occur after cancel.
func TestReporter_Run_StopsOnContextCancel(t *testing.T) {
	// Given: a running reporter
	mock := &mockSender{}
	r := NewReporter(Config{CP: "12D3KooWCP", Interval: 20 * time.Millisecond, Collect: fullCollect()})
	r.send = mock.send

	ctx, cancel := context.WithCancel(context.Background())
	go r.Run(ctx)
	if _, ok := waitForCalls(mock, 1, 2*time.Second); !ok {
		t.Fatal("no initial send observed")
	}

	// When: the context is cancelled
	cancel()
	time.Sleep(30 * time.Millisecond) // let the loop observe cancellation
	stable := mock.count()
	time.Sleep(100 * time.Millisecond) // several intervals

	// Then: no further sends are recorded
	if got := mock.count(); got != stable {
		t.Fatalf("sends continued after cancel: %d -> %d", stable, got)
	}
}

// TestNewReporter_Defaults verifies interval/collect/logger normalization.
func TestNewReporter_Defaults(t *testing.T) {
	// Given: a zero-value Config (no client, no interval, no collect)
	r := NewReporter(Config{})

	// Then: interval defaults to 30s and collect/logger are non-nil
	if r.interval != DefaultInterval {
		t.Fatalf("interval: want %v, got %v", DefaultInterval, r.interval)
	}
	if r.collect == nil {
		t.Fatal("collect must default to a zero-report function")
	}
	if got := r.collect(); got != (types.NodeStatusReport{}) {
		t.Fatalf("default collect must return zero report, got %+v", got)
	}
	if r.logger == nil {
		t.Fatal("logger must default to slog.Default()")
	}
}
