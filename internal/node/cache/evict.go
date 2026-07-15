package cache

import (
	"errors"
	"sort"
	"time"
)

// ─── Domain types for eviction ───

// VideoMeta groups a video's blob with metadata used for content-aware eviction.
type VideoMeta struct {
	BlobHash   string
	Popularity float64
	Segments   []*SegmentMeta
}

// SegmentMeta is per-segment cache metadata used during eviction decisions.
type SegmentMeta struct {
	BlobHash           string
	Bitrate            int
	Size               int64
	LastAccess         time.Time
	LastFetchLatencyMs int64
}

// ─── Injection points (not yet implemented by other todos) ───

// PinChecker reports whether a blob hash is pinned and should not be evicted.
type PinChecker func(blobHash string) bool

// PopSource returns a slice of VideoMeta sorted by popularity (ascending is preferred by eviction).
type PopSource func() []*VideoMeta

// ─── Errors ───

// ErrCacheFull is returned when eviction cannot free any space.
var ErrCacheFull = errors.New("cache: no evictable segment found, cache is full")

// ─── Content-aware LRU eviction ───

const highLatencyThresholdMs = 200

// Evict selects and removes one segment from the warm cache using content-aware LRU:
//  1. Iterate videos by ascending popularity (lowest pop first).
//  2. Within each video, iterate segments by descending bitrate (highest first).
//  3. Skip pinned segments (PinChecker returns true).
//  4. Skip segments whose LastFetchLatencyMs > highLatencyThresholdMs (high-latency protection).
//  5. Otherwise evict the first eligible segment.
//
// If all segments are high-latency, fallback to evicting the one with the lowest latency.
// The caller (warm.go) handles the disk deletion and index cleanup of the returned segment.
func Evict(
	pinChecker PinChecker,
	popSource PopSource,
	index *MemoryIndex,
) (*SegmentMeta, error) {
	videos := popSource()
	if len(videos) == 0 {
		return nil, ErrCacheFull
	}

	// Sort by popularity ascending (lowest pop first) for eviction.
	sort.Slice(videos, func(i, j int) bool {
		return videos[i].Popularity < videos[j].Popularity
	})

	var highLatencyCandidates []*SegmentMeta

	for _, v := range videos {
		// Sort segments by bitrate descending (highest first).
		segs := make([]*SegmentMeta, len(v.Segments))
		copy(segs, v.Segments)
		sort.Slice(segs, func(i, j int) bool {
			return segs[i].Bitrate > segs[j].Bitrate
		})

		for _, seg := range segs {
			// Verify segment is still in the index.
			entry, ok := index.Get(seg.BlobHash)
			if !ok {
				continue
			}

			// Skip pinned segments.
			if entry.IsPrefix || pinChecker(seg.BlobHash) {
				continue
			}

			// High-latency protection: defer to fallback collector.
			if seg.LastFetchLatencyMs > highLatencyThresholdMs {
				highLatencyCandidates = append(highLatencyCandidates, seg)
				continue
			}

			return seg, nil
		}
	}

	// Fallback: all segments are high-latency. Evict the one with the lowest latency.
	if len(highLatencyCandidates) > 0 {
		sort.Slice(highLatencyCandidates, func(i, j int) bool {
			return highLatencyCandidates[i].LastFetchLatencyMs < highLatencyCandidates[j].LastFetchLatencyMs
		})
		return highLatencyCandidates[0], nil
	}

	return nil, ErrCacheFull
}
