package gossippop

import (
	"sync"
	"time"
)

// ─── LocalPopularity: per-node sliding-window request counters ───

// LocalPopularity tracks per-blob request counts over a 6-minute sliding window.
// Nodes snapshot their window every 30 seconds and publish the counts to
// GossipSub for weighted merge by peers.
//
// The window is composed of 6 buckets, each covering 1 minute. On every Hit()
// or Snapshot() call, the window is rotated if 1 minute has elapsed since the
// last rotation, discarding the oldest bucket.
type LocalPopularity struct {
	mu       sync.RWMutex
	counters map[string]*SlidingWindow // key = blob_hash
}

// SlidingWindow is a 6-bucket rolling window where each bucket accumulates
// request counts over 1 minute. The sum of all buckets gives the per-blob
// popularity over the last 6 minutes.
type SlidingWindow struct {
	Buckets    [6]int64  // 6 x 1 min = 6 min sliding window
	CurIdx     int
	LastRotate time.Time
}

// NewLocalPopularity returns an initialised LocalPopularity with an empty
// counter map.
func NewLocalPopularity() *LocalPopularity {
	return &LocalPopularity{
		counters: make(map[string]*SlidingWindow),
	}
}

// Hit increments the current bucket for the given blob hash. If no window
// exists for the blob, a new one is created anchored at time.Now().
func (lp *LocalPopularity) Hit(blobHash string) {
	lp.mu.Lock()
	defer lp.mu.Unlock()

	w, ok := lp.counters[blobHash]
	if !ok {
		w = &SlidingWindow{LastRotate: time.Now()}
		lp.counters[blobHash] = w
	}
	w.rotateIfNeeded()
	w.Buckets[w.CurIdx]++
}

// Snapshot returns a copy of the current per-blob counts (sum of all 6
// buckets). The window is rotated first if needed.
func (lp *LocalPopularity) Snapshot() map[string]int64 {
	lp.mu.Lock()
	defer lp.mu.Unlock()

	result := make(map[string]int64, len(lp.counters))
	for hash, w := range lp.counters {
		w.rotateIfNeeded()
		var sum int64
		for _, b := range w.Buckets {
			sum += b
		}
		if sum > 0 {
			result[hash] = sum
		}
	}
	return result
}

// rotateIfNeeded advances the window if 1 minute has elapsed since the last
// rotation, zeroing stale buckets.
func (w *SlidingWindow) rotateIfNeeded() {
	elapsed := time.Since(w.LastRotate)
	if elapsed < time.Minute {
		return
	}

	// Each full minute that elapsed rotates one bucket, capped at 6.
	steps := int(elapsed.Minutes())
	if steps > 6 {
		steps = 6
	}

	for i := 0; i < steps; i++ {
		w.CurIdx = (w.CurIdx + 1) % 6
		w.Buckets[w.CurIdx] = 0
	}
	w.LastRotate = w.LastRotate.Add(time.Duration(steps) * time.Minute)
}
