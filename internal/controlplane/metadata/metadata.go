// Package metadata provides a PostgreSQL-backed implementation of the
// pinstrategy.MetadataClient interface. It queries the content, video_popularity,
// and blob_location tables directly via database/sql with the lib/pq driver.
package metadata

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	_ "github.com/lib/pq"

	"github.com/shlande/mediaworker/internal/controlplane/pinstrategy"
	"github.com/shlande/mediaworker/internal/types"
)

// MetadataWriter is the interface for persisting content metadata and health
// states to the metadata service. Implemented by PGMetadataClient.
type MetadataWriter interface {
	WriteContentMeta(ctx context.Context, content types.ContentMeta, blobs []types.BlobDescriptor, locations []types.BlobLocation) error
	ReportAccountHealth(ctx context.Context, vendor types.Vendor, accountID string, state types.HealthState) error
	GetAccountHealths(ctx context.Context, vendor types.Vendor) ([]AccountHealth, error)
}

// AccountHealth is a persisted account health record from account_health table.
type AccountHealth struct {
	Vendor    string
	AccountID string
	State     string
	LastCheck time.Time
	LatencyMs int
	ErrorMsg  string
	BanUntil  *time.Time
}

// PGMetadataClient satisfies pinstrategy.MetadataClient by querying PostgreSQL.
// It also implements MetadataWriter for write operations.
type PGMetadataClient struct {
	db *sql.DB
}

// NewPGMetadataClient opens a PostgreSQL connection and returns a ready-to-use
// client. Callers must Close() the client when done.
func NewPGMetadataClient(dsn string) (*PGMetadataClient, error) {
	db, err := sql.Open("postgres", dsn)
	if err != nil {
		return nil, fmt.Errorf("metadata: open postgres: %w", err)
	}
	db.SetMaxOpenConns(10)
	if err := db.Ping(); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("metadata: ping postgres: %w", err)
	}
	return &PGMetadataClient{db: db}, nil
}

// Close releases the underlying database connection pool.
func (c *PGMetadataClient) Close() error {
	return c.db.Close()
}

// GetContentMeta retrieves content metadata by content_id.
// Returns a wrapped sql.ErrNoRows when the row is not found.
func (c *PGMetadataClient) GetContentMeta(contentID string) (*types.ContentMeta, error) {
	row := c.db.QueryRow(
		`SELECT content_id, content_type, type_metadata FROM content WHERE content_id = $1`,
		contentID,
	)
	var cm types.ContentMeta
	if err := row.Scan(&cm.ContentID, &cm.ContentType, &cm.TypeMetadata); err != nil {
		return nil, fmt.Errorf("metadata: get content meta %q: %w", contentID, err)
	}
	return &cm, nil
}

// GetTopContents returns the top N contents sorted by 24-hour window popularity
// descending. Each entry pairs a ContentMeta with its popularity score.
func (c *PGMetadataClient) GetTopContents(ctx context.Context, limit int) ([]pinstrategy.TopContent, error) {
	rows, err := c.db.QueryContext(ctx,
		`SELECT c.content_id, c.content_type, c.type_metadata, v.window_24h
		 FROM content c
		 JOIN video_popularity v ON c.content_id = v.content_id
		 ORDER BY v.window_24h DESC
		 LIMIT $1`, limit,
	)
	if err != nil {
		return nil, fmt.Errorf("metadata: get top contents: %w", err)
	}
	defer rows.Close()

	var out []pinstrategy.TopContent
	for rows.Next() {
		var tc pinstrategy.TopContent
		if err := rows.Scan(&tc.ContentMeta.ContentID, &tc.ContentMeta.ContentType, &tc.ContentMeta.TypeMetadata, &tc.Popularity); err != nil {
			return nil, fmt.Errorf("metadata: scan top content row: %w", err)
		}
		out = append(out, tc)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("metadata: iterate top contents: %w", err)
	}
	return out, nil
}

// WriteContentMeta writes content metadata, blob indices, and blob locations in
// a single transaction. All locations are committed atomically.
func (c *PGMetadataClient) WriteContentMeta(ctx context.Context, content types.ContentMeta, blobs []types.BlobDescriptor, locations []types.BlobLocation) (err error) {
	tx, err := c.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("metadata: begin tx: %w", err)
	}
	defer func() {
		if err != nil {
			_ = tx.Rollback()
		}
	}()

	if _, err = tx.ExecContext(ctx,
		`INSERT INTO content (content_id, content_type, type_metadata) VALUES ($1, $2, $3)`,
		content.ContentID, content.ContentType, content.TypeMetadata,
	); err != nil {
		return fmt.Errorf("metadata: insert content: %w", err)
	}

	for _, b := range blobs {
		if _, err = tx.ExecContext(ctx,
			`INSERT INTO blob_index (content_id, blob_hash, role, sort_order, size_bytes, checksum) VALUES ($1, $2, $3, $4, $5, $6)`,
			content.ContentID, b.BlobHash, b.BlobType, b.SortOrder, b.Size, b.BlobHash,
		); err != nil {
			return fmt.Errorf("metadata: insert blob_index %q: %w", b.BlobHash, err)
		}
	}

	for _, loc := range locations {
		if _, err = tx.ExecContext(ctx,
			`INSERT INTO blob_location (content_id, blob_hash, vendor, account_id, file_id) VALUES ($1, $2, $3, $4, $5)`,
			loc.ContentID, loc.BlobHash, loc.Vendor, loc.AccountID, loc.FileID,
		); err != nil {
			return fmt.Errorf("metadata: insert blob_location %q/%s: %w", loc.BlobHash, loc.Vendor, err)
		}
	}

	if err = tx.Commit(); err != nil {
		return fmt.Errorf("metadata: commit tx: %w", err)
	}
	return nil
}

// ReportAccountHealth upserts an account health record.
func (c *PGMetadataClient) ReportAccountHealth(ctx context.Context, vendor types.Vendor, accountID string, state types.HealthState) error {
	var banUntil *time.Time
	if state.State == "banned" {
		now := time.Now()
		banUntil = &now
	}
	_, err := c.db.ExecContext(ctx,
		`INSERT INTO account_health (vendor, account_id, state, last_check, latency_ms, error_msg, ban_until)
		 VALUES ($1, $2, $3, $4, $5, $6, $7)
		 ON CONFLICT (vendor, account_id) DO UPDATE SET
		   state = $3, last_check = $4, latency_ms = $5, error_msg = $6, ban_until = $7`,
		string(vendor), accountID, state.State, state.LastCheck, int(state.Latency.Milliseconds()), state.ErrorMsg, banUntil,
	)
	if err != nil {
		return fmt.Errorf("metadata: report account health %s/%s: %w", vendor, accountID, err)
	}
	return nil
}

// GetAccountHealths returns all account health records for the given vendor.
func (c *PGMetadataClient) GetAccountHealths(ctx context.Context, vendor types.Vendor) ([]AccountHealth, error) {
	rows, err := c.db.QueryContext(ctx,
		`SELECT vendor, account_id, state, last_check, latency_ms, error_msg, ban_until FROM account_health WHERE vendor = $1`,
		string(vendor),
	)
	if err != nil {
		return nil, fmt.Errorf("metadata: get account healths %q: %w", vendor, err)
	}
	defer rows.Close()

	var out []AccountHealth
	for rows.Next() {
		var h AccountHealth
		if err := rows.Scan(&h.Vendor, &h.AccountID, &h.State, &h.LastCheck, &h.LatencyMs, &h.ErrorMsg, &h.BanUntil); err != nil {
			return nil, fmt.Errorf("metadata: scan account health row: %w", err)
		}
		out = append(out, h)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("metadata: iterate account healths: %w", err)
	}
	return out, nil
}

// GetSegmentLocations returns all storage locations for a given blob hash.
func (c *PGMetadataClient) GetSegmentLocations(blobHash string) ([]types.BlobLocation, error) {
	rows, err := c.db.Query(
		`SELECT content_id, blob_hash, vendor, account_id, file_id FROM blob_location WHERE blob_hash = $1`,
		blobHash,
	)
	if err != nil {
		return nil, fmt.Errorf("metadata: get segment locations %q: %w", blobHash, err)
	}
	defer rows.Close()

	var out []types.BlobLocation
	for rows.Next() {
		var loc types.BlobLocation
		if err := rows.Scan(&loc.ContentID, &loc.BlobHash, &loc.Vendor, &loc.AccountID, &loc.FileID); err != nil {
			return nil, fmt.Errorf("metadata: scan blob location row: %w", err)
		}
		out = append(out, loc)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("metadata: iterate blob locations: %w", err)
	}
	return out, nil
}

// GetPopularity24h returns the 24-hour popularity window for a blob/content.
// Returns 0.0 when the row does not exist or on any error, matching the
// interface contract that errors are silently absorbed.
func (c *PGMetadataClient) GetPopularity24h(blobHash string) float64 {
	var pop int64
	err := c.db.QueryRow(
		`SELECT window_24h FROM video_popularity WHERE content_id = $1`,
		blobHash,
	).Scan(&pop)
	if err != nil {
		return 0.0
	}
	return float64(pop)
}
