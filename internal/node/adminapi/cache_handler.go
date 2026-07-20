package adminapi

import (
	"net/http"

	"github.com/shlande/mediaworker/internal/types"
)

// PinSpaceQuerier is the narrow subset of *pinstore.PinStore used by
// the cache handler. Tests satisfy it with a mock; production wires the
// real PinStore.
type PinSpaceQuerier interface {
	QuerySpace() types.PinSpaceInfo
}

// WarmCacheReader is the narrow subset of *cache.WarmCache used by the
// cache handler. Tests satisfy it with a mock; production wires the real
// WarmCache.
type WarmCacheReader interface {
	Usage() (used, total int64)
	Count() int
	Evictions1h() int
}

// cachePartition is the per-partition response for prefix and warm.
type cachePartition struct {
	Total     int64 `json:"total"`
	Used      int64 `json:"used"`
	BlobCount int64 `json:"blob_count"`
}

// cacheResponse is the GET /v1/cache response body.
type cacheResponse struct {
	Prefix          *cachePartition `json:"prefix"`
	Warm            *cachePartition `json:"warm"`
	Cold            any             `json:"cold"`
	EvictionCounter evictionCounter `json:"eviction_counters"`
}

type evictionCounter struct {
	Warm1h int `json:"warm_1h"`
	Cold1h int `json:"cold_1h"`
}

// prefixPartition builds a cachePartition from PinSpaceInfo. total = available + used
// so it reflects the underlying storage capacity (pin store tracks available, not max).
func prefixPartition(pi types.PinSpaceInfo) *cachePartition {
	return &cachePartition{
		Total:     pi.AvailableBytes + pi.TotalPinnedSize,
		Used:      pi.TotalPinnedSize,
		BlobCount: int64(pi.PinnedCount),
	}
}

// RegisterCacheRoutes mounts GET /v1/cache on srv. pinStore and warmCache
// may be nil — the corresponding partition field is emitted as null. Per
// orchestrator decision D1, this function does not edit main.go; todo 49
// consolidates all node-admin route mounts.
func RegisterCacheRoutes(srv *Server, pinStore PinSpaceQuerier, warmCache WarmCacheReader) {
	srv.Handle("GET /v1/cache", handleCache(pinStore, warmCache))
}

// handleCache 返回各缓存分区用量与驱逐计数。
//
//	@Summary		缓存状态
//	@Description	返回 prefix（PinStore）、warm（WarmCache）分区用量、blob 数与驱逐计数器。
//	@Tags			node-admin
//	@Produce		json
//	@Success		200	{object}	cacheResponse
//	@Security		AdminToken
//	@Router			/v1/cache [get]
func handleCache(pinStore PinSpaceQuerier, warmCache WarmCacheReader) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var prefix *cachePartition
		if pinStore != nil {
			prefix = prefixPartition(pinStore.QuerySpace())
		}

		var warm *cachePartition
		var warm1h int
		if warmCache != nil {
			used, total := warmCache.Usage()
			warm = &cachePartition{
				Total:     total,
				Used:      used,
				BlobCount: int64(warmCache.Count()),
			}
			warm1h = warmCache.Evictions1h()
		}

		WriteJSON(w, http.StatusOK, cacheResponse{
			Prefix:          prefix,
			Warm:            warm,
			Cold:            nil, // cold cache unwired — no cold storage layer at this time
			EvictionCounter: evictionCounter{Warm1h: warm1h, Cold1h: 0},
		})
	}
}
