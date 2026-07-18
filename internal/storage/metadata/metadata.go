// Package metadata provides a PostgreSQL-backed implementation of the
// BlobStoreClient and ContentMetaClient interfaces. It queries the blob,
// content_blob, blob_location, content_popularity, and content tables
// directly via database/sql with the lib/pq driver.
//
// allow: SIZE_OK — single cohesive unit: PGMetadataClient + its interface contracts
// (BlobStoreClient, ContentMetaClient, PopularityClient, MetadataWriter).
// Splitting would scatter tightly-coupled interface definitions.
package metadata

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"time"

	_ "github.com/lib/pq"

	"github.com/shlande/mediaworker/internal/types"
)

// ─── Interfaces ────────────────────────────────────────────────────────────────

// BlobStoreClient is the storage-layer content-addressed client.
// Implemented by *PGMetadataClient.
type BlobStoreClient interface {
	// GetBlobLocations queries blob's K-redundant storage locations.
	// Origin hot path: single key lookup by blob_hash.
	GetBlobLocations(ctx context.Context, blobHash string) ([]types.BlobLocation, error)
	// WriteBlob batch-inserts blob descriptors (ON CONFLICT DO NOTHING dedup).
	// Called within an ingest transaction.
	WriteBlob(ctx context.Context, tx *sql.Tx, blobs []types.BlobDescriptor) error
	// WriteBlobLocations batch-inserts blob locations.
	// Called within an ingest transaction.
	WriteBlobLocations(ctx context.Context, tx *sql.Tx, locations []types.BlobLocation) error
}

// ContentMetaClient is the metadata orchestration-layer client.
// Implemented by *PGMetadataClient.
type ContentMetaClient interface {
	// GetContentMeta retrieves content primary table metadata.
	GetContentMeta(ctx context.Context, contentID string) (*types.ContentMeta, error)
	// GetContentBlobs queries all blobs + arrangement info for a content (JOIN blob + content_blob).
	GetContentBlobs(ctx context.Context, contentID string) ([]types.BlobDescriptor, []types.BlobRole, error)
	// GetTopContents returns top-N popular content (JOIN content_popularity).
	GetTopContents(ctx context.Context, limit int) ([]TopContent, error)
	// GetPopularity24h returns the 24h popularity window for a content.
	GetPopularity24h(ctx context.Context, contentID string) float64
	// WriteContentMeta writes content + content_blob rows.
	// Called within an ingest transaction.
	WriteContentMeta(ctx context.Context, tx *sql.Tx, content types.ContentMeta, blobs []types.BlobDescriptor, roles []types.BlobRole) error
}

// PopularityClient is the popularity query interface, split from ContentMetaClient
// so PinOrchestrator can inject a narrower dependency.
type PopularityClient interface {
	GetTopContents(ctx context.Context, limit int) ([]TopContent, error)
	GetPopularity24h(ctx context.Context, contentID string) float64
}

// MetadataWriter is the interface for persisting content metadata and health
// states to the metadata service. Implemented by PGMetadataClient.
type MetadataWriter interface {
	WriteIngestTransaction(ctx context.Context, content types.ContentMeta, blobs []types.BlobDescriptor, roles []types.BlobRole, locations []types.BlobLocation) error
	ReportAccountHealth(ctx context.Context, vendor types.Vendor, accountID string, state types.HealthState) error
	GetAccountHealths(ctx context.Context, vendor types.Vendor) ([]AccountHealth, error)
}

// ─── Domain types ──────────────────────────────────────────────────────────────

// TopContent pairs content metadata with its 24-hour popularity for rebalancing.
type TopContent struct {
	ContentMeta types.ContentMeta
	Popularity  int64
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

// ─── Compile-time interface checks ─────────────────────────────────────────────

var (
	_ BlobStoreClient  = (*PGMetadataClient)(nil)
	_ ContentMetaClient = (*PGMetadataClient)(nil)
	_ PopularityClient  = (*PGMetadataClient)(nil)
	_ MetadataWriter    = (*PGMetadataClient)(nil)
)

// ─── Client implementation ─────────────────────────────────────────────────────

// PGMetadataClient implements BlobStoreClient, ContentMetaClient, PopularityClient,
// and MetadataWriter by querying PostgreSQL.
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
	if err := MigrateAll(db); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("metadata: run migrations: %w", err)
	}
	return &PGMetadataClient{db: db}, nil
}

// Close releases the underlying database connection pool.
func (c *PGMetadataClient) Close() error {
	return c.db.Close()
}

// ─── ContentMetaClient methods ─────────────────────────────────────────────────

// GetContentMeta retrieves content metadata by content_id.
// Returns a wrapped sql.ErrNoRows when the row is not found.
func (c *PGMetadataClient) GetContentMeta(ctx context.Context, contentID string) (*types.ContentMeta, error) {
	row := c.db.QueryRowContext(ctx,
		`SELECT content_id, content_type, type_metadata FROM content WHERE content_id = $1`,
		contentID,
	)
	var cm types.ContentMeta
	if err := row.Scan(&cm.ContentID, &cm.ContentType, &cm.TypeMetadata); err != nil {
		return nil, fmt.Errorf("metadata: get content meta %q: %w", contentID, err)
	}
	return &cm, nil
}

// GetContentBlobs queries all blobs and their arrangement roles for a content.
// JOINs blob (content-addressed) with content_blob (arrangement layer).
func (c *PGMetadataClient) GetContentBlobs(ctx context.Context, contentID string) ([]types.BlobDescriptor, []types.BlobRole, error) {
	rows, err := c.db.QueryContext(ctx,
		`SELECT b.blob_hash, b.blob_type, b.size_bytes,
		        cb.role, cb.sort_order, cb.business_meta
		 FROM content_blob cb
		 JOIN blob b ON b.blob_hash = cb.blob_hash
		 WHERE cb.content_id = $1
		 ORDER BY cb.role, cb.sort_order`,
		contentID,
	)
	if err != nil {
		return nil, nil, fmt.Errorf("metadata: get content blobs %q: %w", contentID, err)
	}
	defer rows.Close()

	var (
		blobs []types.BlobDescriptor
		roles []types.BlobRole
	)
	for rows.Next() {
		var (
			bd types.BlobDescriptor
			br types.BlobRole
			businessMetaRaw []byte
		)
		if err := rows.Scan(&bd.BlobHash, &bd.BlobType, &bd.Size,
			&br.Role, &br.SortOrder, &businessMetaRaw); err != nil {
			return nil, nil, fmt.Errorf("metadata: scan content blob row: %w", err)
		}
		br.BlobHash = bd.BlobHash
		if len(businessMetaRaw) > 0 {
			if err := json.Unmarshal(businessMetaRaw, &br.BusinessMeta); err != nil {
				return nil, nil, fmt.Errorf("metadata: unmarshal business_meta: %w", err)
			}
		}
		blobs = append(blobs, bd)
		roles = append(roles, br)
	}
	if err := rows.Err(); err != nil {
		return nil, nil, fmt.Errorf("metadata: iterate content blobs: %w", err)
	}
	return blobs, roles, nil
}

// WriteContentMeta inserts content and content_blob rows within an existing
// transaction. The tx parameter is managed by WriteIngestTransaction.
func (c *PGMetadataClient) WriteContentMeta(ctx context.Context, tx *sql.Tx, content types.ContentMeta, blobs []types.BlobDescriptor, roles []types.BlobRole) error {
	if _, err := tx.ExecContext(ctx,
		`INSERT INTO content (content_id, content_type, type_metadata) VALUES ($1, $2, $3)`,
		content.ContentID, content.ContentType, content.TypeMetadata,
	); err != nil {
		return fmt.Errorf("metadata: insert content: %w", err)
	}

	// Build role index: blobHash -> BlobRole
	roleIndex := make(map[string]types.BlobRole, len(roles))
	for _, r := range roles {
		roleIndex[r.BlobHash] = r
	}

	for _, b := range blobs {
		role, ok := roleIndex[b.BlobHash]
		if !ok {
			role = types.BlobRole{BlobHash: b.BlobHash, Role: b.BlobType, SortOrder: 0}
		}
		metaJSON, err := json.Marshal(role.BusinessMeta)
		if err != nil {
			return fmt.Errorf("metadata: marshal business_meta for blob %q: %w", b.BlobHash, err)
		}
		if _, err := tx.ExecContext(ctx,
			`INSERT INTO content_blob (content_id, blob_hash, role, sort_order, business_meta) VALUES ($1, $2, $3, $4, $5)`,
			content.ContentID, b.BlobHash, role.Role, role.SortOrder, metaJSON,
		); err != nil {
			return fmt.Errorf("metadata: insert content_blob %q: %w", b.BlobHash, err)
		}
	}
	return nil
}

// ─── PopularityClient methods ──────────────────────────────────────────────────

// GetTopContents returns the top N contents sorted by 24-hour window popularity
// descending. Each entry pairs a ContentMeta with its popularity score.
func (c *PGMetadataClient) GetTopContents(ctx context.Context, limit int) ([]TopContent, error) {
	rows, err := c.db.QueryContext(ctx,
		`SELECT c.content_id, c.content_type, c.type_metadata, p.window_24h
		 FROM content c
		 JOIN content_popularity p ON c.content_id = p.content_id
		 ORDER BY p.window_24h DESC
		 LIMIT $1`, limit,
	)
	if err != nil {
		return nil, fmt.Errorf("metadata: get top contents: %w", err)
	}
	defer rows.Close()

	var out []TopContent
	for rows.Next() {
		var tc TopContent
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

// GetPopularity24h returns the 24-hour popularity window for a content.
// Returns 0.0 when the row does not exist or on any error.
func (c *PGMetadataClient) GetPopularity24h(ctx context.Context, contentID string) float64 {
	var pop int64
	err := c.db.QueryRowContext(ctx,
		`SELECT window_24h FROM content_popularity WHERE content_id = $1`,
		contentID,
	).Scan(&pop)
	if err != nil {
		return 0.0
	}
	return float64(pop)
}

// ─── BlobStoreClient methods ───────────────────────────────────────────────────

// GetBlobLocations returns all storage locations for a given blob hash.
func (c *PGMetadataClient) GetBlobLocations(ctx context.Context, blobHash string) ([]types.BlobLocation, error) {
	rows, err := c.db.QueryContext(ctx,
		`SELECT blob_hash, backend_id, file_id FROM blob_location WHERE blob_hash = $1`,
		blobHash,
	)
	if err != nil {
		return nil, fmt.Errorf("metadata: get blob locations %q: %w", blobHash, err)
	}
	defer rows.Close()

	var out []types.BlobLocation
	for rows.Next() {
		var loc types.BlobLocation
		if err := rows.Scan(&loc.BlobHash, &loc.BackendID, &loc.FileID); err != nil {
			return nil, fmt.Errorf("metadata: scan blob location row: %w", err)
		}
		out = append(out, loc)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("metadata: iterate blob locations: %w", err)
	}
	return out, nil
}

// WriteBlob batch-inserts blob descriptors with ON CONFLICT DO NOTHING.
// Called within an ingest transaction.
func (c *PGMetadataClient) WriteBlob(ctx context.Context, tx *sql.Tx, blobs []types.BlobDescriptor) error {
	for _, b := range blobs {
		if _, err := tx.ExecContext(ctx,
			`INSERT INTO blob (blob_hash, blob_type, size_bytes) VALUES ($1, $2, $3) ON CONFLICT (blob_hash) DO NOTHING`,
			b.BlobHash, b.BlobType, b.Size,
		); err != nil {
			return fmt.Errorf("metadata: insert blob %q: %w", b.BlobHash, err)
		}
	}
	return nil
}

// WriteBlobLocations batch-inserts blob locations.
// Called within an ingest transaction.
func (c *PGMetadataClient) WriteBlobLocations(ctx context.Context, tx *sql.Tx, locations []types.BlobLocation) error {
	for _, loc := range locations {
		if _, err := tx.ExecContext(ctx,
			`INSERT INTO blob_location (blob_hash, backend_id, file_id) VALUES ($1, $2, $3) ON CONFLICT (blob_hash, backend_id) DO NOTHING`,
			loc.BlobHash, loc.BackendID, loc.FileID,
		); err != nil {
			return fmt.Errorf("metadata: insert blob_location %q/%s: %w", loc.BlobHash, loc.BackendID, err)
		}
	}
	return nil
}

// ─── MetadataWriter methods ────────────────────────────────────────────────────

// WriteIngestTransaction executes a single PG transaction spanning all 4 ingest tables:
// blob (content-addressed) → blob_location (K redundancy) → content → content_blob (arrangement).
// Any step failure triggers ROLLBACK; full success COMMITs.
func (c *PGMetadataClient) WriteIngestTransaction(
	ctx context.Context,
	content types.ContentMeta,
	blobs []types.BlobDescriptor,
	roles []types.BlobRole,
	locations []types.BlobLocation,
) (err error) {
	tx, err := c.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("metadata: begin ingest tx: %w", err)
	}
	defer func() {
		if err != nil {
			_ = tx.Rollback()
		}
	}()

	// 1. blob primary table (ON CONFLICT DO NOTHING cross-content dedup)
	if err = c.WriteBlob(ctx, tx, blobs); err != nil {
		return fmt.Errorf("metadata: write blob: %w", err)
	}

	// 2. blob_location (K redundant positions)
	if err = c.WriteBlobLocations(ctx, tx, locations); err != nil {
		return fmt.Errorf("metadata: write blob locations: %w", err)
	}

	// 3. content + content_blob (orchestration layer)
	if err = c.WriteContentMeta(ctx, tx, content, blobs, roles); err != nil {
		return fmt.Errorf("metadata: write content meta: %w", err)
	}

	if err = tx.Commit(); err != nil {
		return fmt.Errorf("metadata: commit ingest tx: %w", err)
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
