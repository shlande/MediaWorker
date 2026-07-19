package metadata

import (
	"context"
	"fmt"
	"strings"
	"time"
)

// ─── Admin contents read-model (ui-admin-apis todo 14) ───────────────────────
//
// Read-only list query for the admin contents page. Per content row it
// aggregates blob_count/total_bytes (content_blob JOIN blob), the weakest
// replica count (MIN of per-blob blob_location counts — the weakest blob
// determines content health), and the 24h popularity window. Soft-deleted
// contents (deleted_at IS NOT NULL, migration 017) are excluded.
//
// replicas.want (=K, ingest redundancy) is NOT computed here: the caller
// (todo 28 API layer) knows K and merges it into the response as
// replicas:{have, want}, so K never appears in this SQL — hardcoded or
// otherwise. pin_node_count is likewise merged at the API layer from the
// E5 dispatch bookkeeping (todo 16), not from this query.

// AdminContentRow is one row of the admin contents list.
type AdminContentRow struct {
	ContentID    string    `json:"content_id"`
	Title        string    `json:"title"` // SQL NULL -> ""
	ContentType  string    `json:"content_type"`
	TotalBytes   int64     `json:"total_bytes"`
	BlobCount    int       `json:"blob_count"`
	ReplicasHave int       `json:"replicas_have"` // MIN per-blob location count; 0 when blobless
	Window24h    int64     `json:"window_24h"`
	CreatedAt    time.Time `json:"created_at"`
}

// ListContentsQuery parameterizes ListContents. Page is 1-based.
type ListContentsQuery struct {
	Sort     string // "popularity" | "created_at"; empty or unknown -> created_at DESC
	Type     string // content_type filter; empty = all types
	Page     int    // 1-based page number; <1 -> 1
	PageSize int    // rows per page; <1 -> defaultListContentsPageSize
}

// defaultListContentsPageSize is the fallback page size when the caller
// passes PageSize < 1.
const defaultListContentsPageSize = 20

// ListContents returns one page of the admin contents list plus the total
// number of matching (non-deleted) rows across all pages. Sort and column
// names are whitelist-mapped to fixed SQL fragments; only filter values and
// pagination numbers travel as bind parameters.
func (c *PGMetadataClient) ListContents(ctx context.Context, q ListContentsQuery) ([]AdminContentRow, int, error) {
	page := q.Page
	if page < 1 {
		page = 1
	}
	pageSize := q.PageSize
	if pageSize < 1 {
		pageSize = defaultListContentsPageSize
	}

	var filterArgs []any
	var typeFilter string
	if q.Type != "" {
		filterArgs = append(filterArgs, q.Type)
		typeFilter = ` AND c.content_type = $1`
	}

	countQuery := `SELECT COUNT(*) FROM content c WHERE ` + listContentsWhere(typeFilter)
	var total int64
	if err := c.db.QueryRowContext(ctx, countQuery, filterArgs...).Scan(&total); err != nil {
		return nil, 0, fmt.Errorf("metadata: count admin contents: %w", err)
	}

	orderBy := ` ORDER BY c.created_at DESC, c.content_id`
	if q.Sort == "popularity" {
		orderBy = ` ORDER BY COALESCE(p.window_24h, 0) DESC, c.created_at DESC, c.content_id`
	}

	query := `SELECT c.content_id, COALESCE(c.title, ''), c.content_type,
       bs.total_bytes, bs.blob_count,
       COALESCE(rc.replicas_have, 0),
       COALESCE(p.window_24h, 0),
       c.created_at
FROM content c
LEFT JOIN content_popularity p ON p.content_id = c.content_id
LEFT JOIN LATERAL (
    SELECT COUNT(*) AS blob_count, COALESCE(SUM(b.size_bytes), 0) AS total_bytes
    FROM content_blob cb
    JOIN blob b ON b.blob_hash = cb.blob_hash
    WHERE cb.content_id = c.content_id
) bs ON TRUE
LEFT JOIN LATERAL (
    SELECT MIN(pb.cnt) AS replicas_have
    FROM (
        SELECT COUNT(bl.backend_id) AS cnt
        FROM content_blob cb
        LEFT JOIN blob_location bl ON bl.blob_hash = cb.blob_hash
        WHERE cb.content_id = c.content_id
        GROUP BY cb.blob_hash
    ) pb
) rc ON TRUE
WHERE ` + listContentsWhere(typeFilter) + orderBy

	args := append([]any(nil), filterArgs...)
	args = append(args, pageSize, (page-1)*pageSize)
	query += fmt.Sprintf(` LIMIT $%d OFFSET $%d`, len(args)-1, len(args))

	rows, err := c.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, 0, fmt.Errorf("metadata: list admin contents: %w", err)
	}
	defer func() { _ = rows.Close() }()

	var out []AdminContentRow
	for rows.Next() {
		var r AdminContentRow
		var blobCount, replicasHave int64
		if err := rows.Scan(&r.ContentID, &r.Title, &r.ContentType, &r.TotalBytes, &blobCount, &replicasHave, &r.Window24h, &r.CreatedAt); err != nil {
			return nil, 0, fmt.Errorf("metadata: scan admin content row: %w", err)
		}
		r.BlobCount = int(blobCount)
		r.ReplicasHave = int(replicasHave)
		out = append(out, r)
	}
	if err := rows.Err(); err != nil {
		return nil, 0, fmt.Errorf("metadata: iterate admin contents: %w", err)
	}
	return out, int(total), nil
}

// listContentsWhere renders the shared WHERE fragment for the list and count
// queries. Kept unexported and single-purpose; both queries MUST stay in sync
// on the soft-delete filter and the type filter.
func listContentsWhere(typeFilter string) string {
	var sb strings.Builder
	sb.WriteString(`c.deleted_at IS NULL`)
	sb.WriteString(typeFilter)
	return sb.String()
}
