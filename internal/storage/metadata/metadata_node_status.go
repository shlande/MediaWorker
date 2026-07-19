package metadata

import (
	"context"
	"fmt"
	"time"
)

// ─── node_status_history accessors ─────────────────────────────────────────────
//
// Persisted history of edge-node status reports (migration 016). Written by the
// control plane on each NodeStatusReport it consumes; read back by the node
// detail admin API ("recent N reports"). Pruned per-peer so the table stays
// bounded.

// NodeStatusHistoryRow is one row of the node_status_history table.
// Pointer fields map to nullable columns: nil means "not reported".
type NodeStatusHistoryRow struct {
	ID          int64     // BIGSERIAL, zero on insert
	PeerID      string    // peer that emitted the report
	NodeID      *string   // optional node identifier
	Healthy     bool      // self-reported health flag
	PrefixUsed  *int64    // prefix cache bytes used
	PrefixTotal *int64    // prefix cache bytes total
	WarmUsed    *int64    // warm cache bytes used
	WarmTotal   *int64    // warm cache bytes total
	ConnCount   *int32    // active connection count
	Region      *string   // configured node region
	Version     *string   // node binary version
	ReportedAt  time.Time // node-side report timestamp
	ReceivedAt  time.Time // CP-side insert timestamp, zero on insert (DB default now())
}

// InsertNodeStatusHistory appends one status report row for a peer.
// received_at is set by the database default (now()).
func (c *PGMetadataClient) InsertNodeStatusHistory(ctx context.Context, row NodeStatusHistoryRow) error {
	_, err := c.db.ExecContext(ctx,
		`INSERT INTO node_status_history
		   (peer_id, node_id, healthy, prefix_used, prefix_total, warm_used, warm_total, conn_count, region, version, reported_at)
		 VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)`,
		row.PeerID, row.NodeID, row.Healthy,
		row.PrefixUsed, row.PrefixTotal, row.WarmUsed, row.WarmTotal,
		row.ConnCount, row.Region, row.Version, row.ReportedAt,
	)
	if err != nil {
		return fmt.Errorf("metadata: insert node status history %q: %w", row.PeerID, err)
	}
	return nil
}

// GetNodeStatusHistory returns up to limit most recent rows for a peer,
// newest first (received_at DESC). An empty result is a nil-error empty slice.
func (c *PGMetadataClient) GetNodeStatusHistory(ctx context.Context, peerID string, limit int) ([]NodeStatusHistoryRow, error) {
	rows, err := c.db.QueryContext(ctx,
		`SELECT id, peer_id, node_id, healthy, prefix_used, prefix_total, warm_used, warm_total, conn_count, region, version, reported_at, received_at
		 FROM node_status_history WHERE peer_id = $1 ORDER BY received_at DESC LIMIT $2`,
		peerID, limit,
	)
	if err != nil {
		return nil, fmt.Errorf("metadata: get node status history %q: %w", peerID, err)
	}
	defer func() { _ = rows.Close() }()

	var out []NodeStatusHistoryRow
	for rows.Next() {
		var r NodeStatusHistoryRow
		if err := rows.Scan(
			&r.ID, &r.PeerID, &r.NodeID, &r.Healthy,
			&r.PrefixUsed, &r.PrefixTotal, &r.WarmUsed, &r.WarmTotal,
			&r.ConnCount, &r.Region, &r.Version, &r.ReportedAt, &r.ReceivedAt,
		); err != nil {
			return nil, fmt.Errorf("metadata: scan node status history row: %w", err)
		}
		out = append(out, r)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("metadata: iterate node status history: %w", err)
	}
	return out, nil
}

// PruneNodeStatusHistory deletes all but the keep most recent rows for a peer.
// keep is caller-supplied; rows are ranked by received_at DESC.
func (c *PGMetadataClient) PruneNodeStatusHistory(ctx context.Context, peerID string, keep int) error {
	_, err := c.db.ExecContext(ctx,
		`DELETE FROM node_status_history
		 WHERE peer_id = $1 AND id NOT IN (
		   SELECT id FROM node_status_history WHERE peer_id = $1 ORDER BY received_at DESC LIMIT $2
		 )`,
		peerID, keep,
	)
	if err != nil {
		return fmt.Errorf("metadata: prune node status history %q: %w", peerID, err)
	}
	return nil
}
