package ingest

import (
	"context"
	"fmt"
	"io"
	"os"
	"time"

	"github.com/shlande/mediaworker/internal/types"
	"golang.org/x/sync/errgroup"
)

// IngestPipeline is the generic content ingestion orchestrator. It routes
// incoming content to the registered ContentIngester, uploads all resulting
// blobs redundantly, writes metadata in a single transaction, and publishes
// the ingestion event asynchronously.
type IngestPipeline struct {
	ingesters  map[string]ContentIngester
	backends   BackendPool
	blobStore  BlobStoreWriter
	eventBus   EventPublisher
	redundancy int
}

// NewIngestPipeline returns a ready-to-use IngestPipeline.
// redundancy is the target K for redundant blob uploads;
// values ≤ 0 are normalized to 2.
func NewIngestPipeline(backends BackendPool, blobStore BlobStoreWriter, eventBus EventPublisher, redundancy int) *IngestPipeline {
	if redundancy <= 0 {
		redundancy = 2
	}
	return &IngestPipeline{
		ingesters:  make(map[string]ContentIngester),
		backends:   backends,
		blobStore:  blobStore,
		eventBus:   eventBus,
		redundancy: redundancy,
	}
}

// RegisterIngester adds a content-type-specific ingester, keyed by its
// ContentType() value. Re-registering the same type overwrites the previous
// handler.
func (p *IngestPipeline) RegisterIngester(ingester ContentIngester) {
	p.ingesters[ingester.ContentType()] = ingester
}

// Ingest processes raw input for the given content type. It returns the final
// content ID on success.
//
// Flow: route → Process → uploadAllBlobs → WriteIngestTransaction → async Publish.
func (p *IngestPipeline) Ingest(
	ctx context.Context,
	contentType string,
	input io.Reader,
	opts ProcessOptions,
) (contentID string, err error) {
	ingester, ok := p.ingesters[contentType]
	if !ok {
		return "", fmt.Errorf("unsupported content type: %s", contentType)
	}

	result, err := ingester.Process(ctx, input, opts)
	if err != nil {
		return "", fmt.Errorf("process: %w", err)
	}
	if result.WorkDir != "" {
		defer os.RemoveAll(result.WorkDir)
	}

	// Upload every blob redundantly (K = p.redundancy).
	locations, err := p.uploadAllBlobs(ctx, result.Blobs, result.BlobFiles, p.redundancy)
	if err != nil {
		return "", fmt.Errorf("blob upload: %w", err)
	}

	// Write metadata in a single transaction.
	content := types.ContentMeta{
		ContentID:    result.ContentID,
		ContentType:  result.ContentType,
		TypeMetadata: result.TypeMetadata,
	}
	if err := p.blobStore.WriteIngestTransaction(ctx, content, result.Blobs, result.Roles, locations); err != nil {
		return "", err
	}

	// Publish the event asynchronously — never blocks the caller.
	go p.eventBus.Publish(types.ContentIngestedEvent{
		ContentID:   result.ContentID,
		ContentType: result.ContentType,
		Blobs:       result.Blobs,
		Roles:       result.Roles,
		Timestamp:   time.Now().Unix(),
	})

	return result.ContentID, nil
}

// uploadAllBlobs concurrently uploads every blob. It tolerates partial
// failures per blob as long as at least one backend succeeds. Returns
// a flat slice of all successful BlobLocation records.
func (p *IngestPipeline) uploadAllBlobs(
	ctx context.Context,
	blobs []types.BlobDescriptor,
	blobFiles map[string]string,
	k int,
) ([]types.BlobLocation, error) {
	g, ctx := errgroup.WithContext(ctx)
	g.SetLimit(10)

	type idxLoc struct {
		idx  int
		locs []types.BlobLocation
	}
	results := make(chan idxLoc, len(blobs))

	for idx, blob := range blobs {
		idx, blob := idx, blob
		g.Go(func() error {
			filePath, ok := blobFiles[blob.BlobHash]
			if !ok {
				return fmt.Errorf("blob %s: no file path in BlobFiles", blob.BlobHash)
			}
			locs, err := p.uploadBlobToK(ctx, blob, filePath, k)
			if err != nil {
				return fmt.Errorf("blob %s: %w", blob.BlobHash, err)
			}
			results <- idxLoc{idx: idx, locs: locs}
			return nil
		})
	}

	if err := g.Wait(); err != nil {
		return nil, err
	}
	close(results)

	// Reassemble in original order.
	allLocations := make([]types.BlobLocation, 0, len(blobs)*k)
	ordered := make([][]types.BlobLocation, len(blobs))
	for r := range results {
		ordered[r.idx] = r.locs
	}
	for _, locs := range ordered {
		allLocations = append(allLocations, locs...)
	}
	return allLocations, nil
}

// uploadBlobToK redundantly uploads a single blob to K backends. At least one
// backend MUST succeed; the first K that succeed are returned. Zero-value
// locations (from failed backends) are filtered out.
func (p *IngestPipeline) uploadBlobToK(
	ctx context.Context,
	blob types.BlobDescriptor,
	filePath string,
	k int,
) ([]types.BlobLocation, error) {
	backends, err := p.backends.SelectKForUpload(k)
	if err != nil {
		return nil, fmt.Errorf("select backends: %w", err)
	}

	g, ctx := errgroup.WithContext(ctx)
	locs := make([]BackendLocation, len(backends))

	for i, be := range backends {
		i, be := i, be
		g.Go(func() error {
			f, err := os.Open(filePath)
			if err != nil {
				return fmt.Errorf("open %s: %w", filePath, err)
			}
			defer func() { _ = f.Close() }()

			loc, err := be.Put(ctx, blob.BlobHash, f, blob.Size)
			if err != nil {
				return err
			}
			locs[i] = loc
			return nil
		})
	}

	if werr := g.Wait(); werr != nil {
		// Tolerate partial failure — at least one backend must succeed.
		successful := filterNonZero(locs)
		if len(successful) == 0 {
			return nil, fmt.Errorf("all %d backends failed for blob %s: %w", len(backends), blob.BlobHash, werr)
		}
		return backendToBlobLocations(successful, blob.BlobHash), nil
	}

	return backendToBlobLocations(filterNonZero(locs), blob.BlobHash), nil
}

// ─── helpers ──────────────────────────────────────────────────────────

func filterNonZero(locs []BackendLocation) []BackendLocation {
	out := make([]BackendLocation, 0, len(locs))
	for _, l := range locs {
		if l.BackendID != "" || l.FileID != "" {
			out = append(out, l)
		}
	}
	return out
}

func backendToBlobLocations(bls []BackendLocation, blobHash string) []types.BlobLocation {
	out := make([]types.BlobLocation, len(bls))
	for i, l := range bls {
		out[i] = types.BlobLocation{
			BlobHash:  blobHash,
			BackendID: l.BackendID,
			FileID:    l.FileID,
		}
	}
	return out
}
