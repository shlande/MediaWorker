// Package ingest defines the generic content ingestion pipeline: content-type
// aware preprocessing (ContentIngester), redundant blob upload (BackendPool,
// BackendUploader), metadata transaction storage (BlobStoreWriter), and event
// publication (EventPublisher).
//
// It deliberately does not import internal/controlplane or internal/storage to
// avoid import cycles — the interfaces declared here are satisfied by those
// packages via implicit structural typing.
package ingest

import (
	"context"
	"io"

	"github.com/shlande/mediaworker/internal/types"
)

// ─── Preprocessing (per-content-type) ────────────────────────────────

// ContentIngester is a per-content-type preprocessor. Each concrete type
// (dash_video, image, document, ...) implements this interface and registers
// with the IngestPipeline.
type ContentIngester interface {
	ContentType() string
	Process(ctx context.Context, input io.Reader, opts ProcessOptions) (*ProcessResult, error)
}

// ProcessOptions carries optional parameters supplied by the caller.
type ProcessOptions struct {
	ContentID string
	Metadata  map[string]string // caller-provided key-value metadata
}

// ProcessResult is the output of a ContentIngester.Process call. It is a
// pure-data struct: no methods, no side effects.
type ProcessResult struct {
	ContentID    string
	ContentType  string
	Blobs        []types.BlobDescriptor
	Roles        []types.BlobRole
	TypeMetadata []byte // content-type-specific metadata (MPD XML, EXIF, etc.)

	// BlobFiles maps blob_hash → absolute path to the temporary file. These
	// files are transient — they exist only for the lifetime of the Ingest()
	// call and MUST NOT be persisted. The pipeline reads from them during
	// upload and the caller (or the Process implementation) cleans them up.
	BlobFiles map[string]string
}

// ─── Storage (blob + content metadata) ───────────────────────────────

// BlobStoreWriter is the metadata-transaction interface for persisting blob
// records, their locations, and the content orchestration metadata. This
// interface mirrors the shape of
// storage/metadata.PGMetadataClient.WriteIngestTransaction but lives in
// the ingest package to break the import cycle.
type BlobStoreWriter interface {
	WriteIngestTransaction(
		ctx context.Context,
		content types.ContentMeta,
		blobs []types.BlobDescriptor,
		roles []types.BlobRole,
		locations []types.BlobLocation,
	) error
}

// ─── Backend abstraction (cloud-drive upload) ────────────────────────

// BackendLocation identifies a file on a specific cloud-drive backend.
type BackendLocation struct {
	BackendID string // "vendor:account_id", e.g. "115:acct_03"
	FileID    string
}

// BackendUploader places a single blob onto a backend. The blobHash is the
// SHA-256 content hash (used for content-addressed path construction).
type BackendUploader interface {
	Put(ctx context.Context, blobHash string, reader io.Reader, size int64) (BackendLocation, error)
}

// BackendPool selects K backends for redundant upload.
type BackendPool interface {
	SelectKForUpload(k int) ([]BackendUploader, error)
}

// ─── Event publication ───────────────────────────────────────────────

// EventPublisher broadcasts content-ingestion events to other domains
// (distribution, policy, etc.).
type EventPublisher interface {
	Publish(evt types.ContentIngestedEvent)
}
