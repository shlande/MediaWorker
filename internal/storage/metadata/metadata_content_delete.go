package metadata

import (
	"context"
	"fmt"
)

// SoftDeleteContent marks a content as deleted (content.deleted_at = now()) and
// unlinks its arrangement rows (content_blob) in a single transaction. The blob
// rows themselves are NOT touched — orphaned blobs are the janitor's job
// (gc.Collector.MarkOrphans). Idempotent: a repeat delete matches zero rows
// and returns nil.
func (c *PGMetadataClient) SoftDeleteContent(ctx context.Context, contentID string) (err error) {
	tx, err := c.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("metadata: begin soft-delete tx: %w", err)
	}
	defer func() {
		if err != nil {
			_ = tx.Rollback()
		}
	}()

	if _, err = tx.ExecContext(ctx,
		`UPDATE content SET deleted_at = now() WHERE content_id = $1 AND deleted_at IS NULL`,
		contentID,
	); err != nil {
		return fmt.Errorf("metadata: soft-delete content %q: %w", contentID, err)
	}

	if _, err = tx.ExecContext(ctx,
		`DELETE FROM content_blob WHERE content_id = $1`,
		contentID,
	); err != nil {
		return fmt.Errorf("metadata: unlink content_blob for %q: %w", contentID, err)
	}

	if err = tx.Commit(); err != nil {
		return fmt.Errorf("metadata: commit soft-delete tx: %w", err)
	}
	return nil
}
