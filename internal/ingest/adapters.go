// Package ingest defines the generic content ingestion pipeline. This file
// provides adapter types that bridge external packages (accountpool, PG) into
// the ingest interfaces without introducing import cycles.
package ingest

import (
	"context"
	"fmt"
	"io"
	"log/slog"

	"github.com/shlande/mediaworker/internal/types"
)

// ─── AccountPool → BackendPool / BackendUploader adapter ──────────────

// UploadableAccount represents a single account ready for upload. It is the
// minimal representation the adapter needs from the account pool.
type UploadableAccount struct {
	BackendID string // "vendor:account_id"
	PutFunc   func(ctx context.Context, blobHash string, reader io.Reader, size int64) (fileID string, err error)
}

// AccountSelector abstracts AccountPool.SelectK so the adapter can select
// K healthy accounts for upload.
type AccountSelector interface {
	SelectKForUpload(ctx context.Context, k int) ([]*UploadableAccount, error)
}

// AccountPoolBackend adapts an AccountSelector to the BackendPool and
// BackendUploader interfaces.
type AccountPoolBackend struct {
	selector AccountSelector
	k        int // default redundancy
}

// NewAccountPoolBackend wraps an AccountSelector as a BackendPool.
func NewAccountPoolBackend(selector AccountSelector, k int) *AccountPoolBackend {
	return &AccountPoolBackend{selector: selector, k: k}
}

// SelectKForUpload satisfies BackendPool. Each selected UploadableAccount
// is wrapped as a BackendUploader.
func (b *AccountPoolBackend) SelectKForUpload(k int) ([]BackendUploader, error) {
	if k <= 0 {
		k = b.k
	}
	accounts, err := b.selector.SelectKForUpload(context.Background(), k)
	if err != nil {
		return nil, fmt.Errorf("select accounts for upload: %w", err)
	}
	out := make([]BackendUploader, len(accounts))
	for i, acct := range accounts {
		out[i] = &accountBackend{backendID: acct.BackendID, putFunc: acct.PutFunc}
	}
	return out, nil
}

// accountBackend wraps a single UploadableAccount as BackendUploader.
type accountBackend struct {
	backendID string
	putFunc   func(ctx context.Context, blobHash string, reader io.Reader, size int64) (fileID string, err error)
}

// Put satisfies BackendUploader by delegating to the account's PutFunc.
func (a *accountBackend) Put(ctx context.Context, blobHash string, reader io.Reader, size int64) (BackendLocation, error) {
	fileID, err := a.putFunc(ctx, blobHash, reader, size)
	if err != nil {
		return BackendLocation{}, fmt.Errorf("upload to %s: %w", a.backendID, err)
	}
	return BackendLocation{
		BackendID: a.backendID,
		FileID:    fileID,
	}, nil
}

// ─── Log-only EventPublisher ───────────────────────────────────────────

// LogPublisher is a no-op EventPublisher that logs ingestion events as
// structured slog records. The standalone ingest-worker does not have a
// libp2p SyncBroadcaster connection — event forwarding will be added when
// the worker joins the sync mesh.
type LogPublisher struct{}

// NewLogPublisher returns a LogPublisher.
func NewLogPublisher() *LogPublisher { return &LogPublisher{} }

// Publish logs the ContentIngestedEvent at info level.
func (p *LogPublisher) Publish(evt types.ContentIngestedEvent) {
	slog.Info("content-ingested",
		"content_id", evt.ContentID,
		"content_type", evt.ContentType,
		"blob_count", len(evt.Blobs),
		"role_count", len(evt.Roles),
		"timestamp", evt.Timestamp,
	)
}
