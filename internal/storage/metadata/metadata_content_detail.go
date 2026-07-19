package metadata

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	"github.com/shlande/mediaworker/internal/types"
)

// ─── Admin content detail read-model (ui-admin-apis todo 21) ────────────────
//
// GetContentDetail assembles the admin contents/{id} view in three steps:
//
//	(a) meta      — GetContentMeta (includes title/deleted_at from todo 7)
//	(b) blobs     — GetContentBlobs, descriptor + role merged per entry
//	(c) locations — every blob_location row of the content's blobs, with
//	                backend_id ("vendor:account_id", migration 010) split in
//	                SQL and LEFT JOINed to account_health for its state.
//
// Soft-deleted contents (deleted_at NOT NULL) are STILL returned: the API
// layer marks them pending_delete rather than answering 410. Replica
// aggregation is deliberately absent — ListContents (todo 14) owns it.

// ErrContentNotFound is returned by GetContentDetail when the content row
// does not exist. API layers map it to 404.
var ErrContentNotFound = errors.New("metadata: content not found")

// AdminContentDetail is the full admin view of one content.
type AdminContentDetail struct {
	Meta      *types.ContentMeta     `json:"meta"`
	Blobs     []AdminContentBlob     `json:"blobs"`
	Locations []AdminContentLocation `json:"locations"`
}

// AdminContentBlob merges the content-addressed descriptor (hash/type/size)
// with the arrangement layer (role/sort_order/business_meta).
type AdminContentBlob struct {
	Hash         string         `json:"hash"`
	Role         string         `json:"role"`
	SortOrder    int            `json:"sort_order"`
	BusinessMeta map[string]any `json:"business_meta,omitempty"`
	Size         int64          `json:"size"`
	BlobType     string         `json:"blob_type"`
}

// AdminContentLocation is one physical replica of a blob on a backend.
// AccountHealth is nil when the backend has no account_health row yet
// (awaiting first probe — same null semantics as the accounts list).
type AdminContentLocation struct {
	BlobHash      string  `json:"blob_hash"`
	BackendID     string  `json:"backend_id"` // "vendor:account_id"
	FileID        string  `json:"file_id"`
	AccountHealth *string `json:"account_health"`
}

// GetContentDetail returns the admin detail view for contentID, or an error
// wrapping ErrContentNotFound when the content does not exist.
func (c *PGMetadataClient) GetContentDetail(ctx context.Context, contentID string) (*AdminContentDetail, error) {
	meta, err := c.GetContentMeta(ctx, contentID)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, fmt.Errorf("metadata: content %q: %w", contentID, ErrContentNotFound)
		}
		return nil, fmt.Errorf("metadata: get content detail %q: %w", contentID, err)
	}

	blobs, roles, err := c.GetContentBlobs(ctx, contentID)
	if err != nil {
		return nil, fmt.Errorf("metadata: get content detail blobs %q: %w", contentID, err)
	}

	locations, err := c.getContentLocations(ctx, contentID)
	if err != nil {
		return nil, err
	}

	return &AdminContentDetail{
		Meta:      meta,
		Blobs:     mergeBlobsAndRoles(blobs, roles),
		Locations: locations,
	}, nil
}

// mergeBlobsAndRoles zips the parallel slices returned by GetContentBlobs
// (same row order, same length) into AdminContentBlob entries.
func mergeBlobsAndRoles(blobs []types.BlobDescriptor, roles []types.BlobRole) []AdminContentBlob {
	out := make([]AdminContentBlob, 0, len(blobs))
	for i, bd := range blobs {
		out = append(out, AdminContentBlob{
			Hash:         bd.BlobHash,
			BlobType:     bd.BlobType,
			Size:         bd.Size,
			Role:         roles[i].Role,
			SortOrder:    roles[i].SortOrder,
			BusinessMeta: roles[i].BusinessMeta,
		})
	}
	return out
}

// getContentLocations lists every blob_location row belonging to the
// content's blobs. backend_id is stored as "vendor:account_id" (migration
// 010 comment, types.BlobLocation.BackendID, internal/ingest adapters) and
// split in SQL so account_health can be LEFT JOINed on (vendor, account_id);
// a missing health row leaves state NULL → AccountHealth nil.
func (c *PGMetadataClient) getContentLocations(ctx context.Context, contentID string) ([]AdminContentLocation, error) {
	rows, err := c.db.QueryContext(ctx,
		`SELECT cb.blob_hash, bl.backend_id, bl.file_id, ah.state
		 FROM content_blob cb
		 JOIN blob_location bl ON bl.blob_hash = cb.blob_hash
		 LEFT JOIN account_health ah
		   ON ah.vendor = split_part(bl.backend_id, ':', 1)
		  AND ah.account_id = split_part(bl.backend_id, ':', 2)
		 WHERE cb.content_id = $1
		 ORDER BY cb.blob_hash, bl.backend_id`,
		contentID,
	)
	if err != nil {
		return nil, fmt.Errorf("metadata: get content locations %q: %w", contentID, err)
	}
	defer func() { _ = rows.Close() }()

	var out []AdminContentLocation
	for rows.Next() {
		var (
			loc   AdminContentLocation
			state sql.NullString
		)
		if err := rows.Scan(&loc.BlobHash, &loc.BackendID, &loc.FileID, &state); err != nil {
			return nil, fmt.Errorf("metadata: scan content location row: %w", err)
		}
		if state.Valid {
			s := state.String
			loc.AccountHealth = &s
		}
		out = append(out, loc)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("metadata: iterate content locations: %w", err)
	}
	return out, nil
}
