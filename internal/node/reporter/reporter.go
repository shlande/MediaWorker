// Package reporter implements the node's periodic NODE_STATUS_REPORT loop.
// Every interval (default 30s) the Reporter collects a types.NodeStatusReport
// via the injected collect function and pushes it to the control plane over
// the /edge/control/1.0.0 reverse channel (nodesync.Client.SendToControlPlane).
// A failed send is logged at Warn and NOT retried — the next cycle covers it.
package reporter

import (
	"context"
	"log/slog"
	"time"

	"github.com/libp2p/go-libp2p/core/peer"

	nodesync "github.com/shlande/mediaworker/internal/node/syncbroadcaster"
	"github.com/shlande/mediaworker/internal/types"
)

// EventType is the wire event type for node status reports. The control-plane
// broadcaster dispatches on this string (see syncbroadcaster_test.go's
// TestReverseChannel_NodeStatusReport).
const EventType = "NODE_STATUS_REPORT"

// DefaultInterval is the cadence used when Config.Interval is zero/negative.
const DefaultInterval = 30 * time.Second

// sendFunc is the dispatch seam — identical signature to
// nodesync.Client.SendToControlPlane. Production wiring defaults it to the
// client's method; tests substitute a recording stub.
type sendFunc func(ctx context.Context, targetCP peer.ID, eventType string, payload any) error

// Reporter periodically collects a NodeStatusReport and sends it to the
// control plane. Run it in its own goroutine; it stops when ctx is cancelled.
type Reporter struct {
	client   *nodesync.Client
	cp       peer.ID
	interval time.Duration
	collect  func() types.NodeStatusReport
	logger   *slog.Logger
	send     sendFunc
}

// Config carries NewReporter's construction inputs.
type Config struct {
	// Client is the control-channel client used to reach the CP (required in
	// production; tests may leave nil and set the send seam directly).
	Client *nodesync.Client
	// CP is the control plane's libp2p peer ID (stream target).
	CP peer.ID
	// Interval is the report cadence; <= 0 selects DefaultInterval.
	Interval time.Duration
	// Collect builds the report each cycle. Nil installs a zero-report
	// placeholder so Run can never panic on a missing collector.
	Collect func() types.NodeStatusReport
	// Logger receives send-failure Warns; nil selects slog.Default().
	Logger *slog.Logger
}

// NewReporter constructs a Reporter from cfg.
func NewReporter(cfg Config) *Reporter {
	r := &Reporter{
		client:   cfg.Client,
		cp:       cfg.CP,
		interval: cfg.Interval,
		collect:  cfg.Collect,
		logger:   cfg.Logger,
	}
	if r.interval <= 0 {
		r.interval = DefaultInterval
	}
	if r.collect == nil {
		r.collect = func() types.NodeStatusReport { return types.NodeStatusReport{} }
	}
	if r.logger == nil {
		r.logger = slog.Default()
	}
	if r.client != nil {
		r.send = r.client.SendToControlPlane
	}
	return r
}

// Run collects and sends a NodeStatusReport every interval until ctx is
// cancelled. Send failures are Warn-logged and the loop continues — the next
// cycle is the retry. The first send fires after one full interval.
func (r *Reporter) Run(ctx context.Context) {
	ticker := time.NewTicker(r.interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			report := r.collect()
			if r.send == nil {
				r.logger.Warn("node status report skipped: no control-channel client")
				continue
			}
			if err := r.send(ctx, r.cp, EventType, report); err != nil {
				r.logger.Warn("node status report send failed (next cycle covers)", "err", err)
			}
		}
	}
}
