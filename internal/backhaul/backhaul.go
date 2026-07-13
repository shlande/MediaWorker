package backhaul

import (
	"context"

	"golang.org/x/sync/singleflight"
)

type BackhaulManager struct {
	cache CacheReader

	sfGroup singleflight.Group

	dataPlane  DataPlane
	icpFetcher ICPFetcher
	l4Fetcher  L4Fetcher
}

type ICPFetcher interface {
	FetchFromPeer(ctx context.Context, blobHash string) (interface{}, bool, error)
}

type L4Fetcher interface {
	FetchFromL4Node(ctx context.Context, blobHash string) (interface{}, error)
}

type CacheReader interface {
	Get(blobHash string) ([]byte, bool)
}

func NewBackhaulManager(
	cache CacheReader,
	dataPlane DataPlane,
	icpFetcher ICPFetcher,
	l4Fetcher L4Fetcher,
) *BackhaulManager {
	return &BackhaulManager{
		cache:      cache,
		dataPlane:  dataPlane,
		icpFetcher: icpFetcher,
		l4Fetcher:  l4Fetcher,
	}
}

func (bm *BackhaulManager) BackhaulUtilization() float64 {
	return 0
}
