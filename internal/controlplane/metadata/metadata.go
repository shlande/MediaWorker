// Package metadata provides a PostgreSQL-backed implementation of the
// pinstrategy.MetadataClient interface. It queries the content, video_popularity,
// and blob_location tables directly via database/sql with the lib/pq driver.
package metadata

import (
	"context"
	"database/sql"
	"fmt"

	_ "github.com/lib/pq"

	"github.com/shlande/mediaworker/internal/controlplane/pinstrategy"
	"github.com/shlande/mediaworker/internal/types"
)

// PGMetadataClient satisfies pinstrategy.MetadataClient by querying PostgreSQL.
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

// GetSegmentLocations returns all storage locations for a given blob hash.
func (c *PGMetadataClient) GetSegmentLocations(blobHash string) ([]types.BlobLocation, error) {
	rows, err := c.db.Query(
		`SELECT blob_hash, vendor, account_id, file_id FROM blob_location WHERE blob_hash = $1`,
		blobHash,
	)
	if err != nil {
		return nil, fmt.Errorf("metadata: get segment locations %q: %w", blobHash, err)
	}
	defer rows.Close()

	var out []types.BlobLocation
	for rows.Next() {
		var loc types.BlobLocation
		if err := rows.Scan(&loc.BlobHash, &loc.Vendor, &loc.AccountID, &loc.FileID); err != nil {
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
