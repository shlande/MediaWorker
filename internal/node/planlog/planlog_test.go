package planlog

import (
	"sync"
	"testing"
	"time"

	"github.com/shlande/mediaworker/internal/types"
)

func rec(seq uint64) Record {
	return Record{Seq: seq, ReceivedAt: time.Unix(int64(seq), 0), Pins: 1, Unpins: 0, Applied: true}
}

// Given 51 records added to a capacity-50 ring, when reading back, then the
// oldest record is evicted and exactly 50 remain.
func TestLog_CapacityEvictsOldest(t *testing.T) {
	l := New()
	for seq := uint64(0); seq <= 50; seq++ {
		l.Add(rec(seq))
	}

	got := l.Recent(100)
	if len(got) != 50 {
		t.Fatalf("len = %d, want 50 (oldest evicted)", len(got))
	}
	if got[49].Seq != 1 {
		t.Fatalf("oldest retained seq = %d, want 1 (seq 0 evicted)", got[49].Seq)
	}
}

// Given several records, when reading Recent, then the order is newest first.
func TestLog_RecentNewestFirst(t *testing.T) {
	l := New()
	for seq := uint64(1); seq <= 3; seq++ {
		l.Add(rec(seq))
	}

	got := l.Recent(10)
	if len(got) != 3 {
		t.Fatalf("len = %d, want 3", len(got))
	}
	for i, want := range []uint64{3, 2, 1} {
		if got[i].Seq != want {
			t.Fatalf("got[%d].Seq = %d, want %d (newest first)", i, got[i].Seq, want)
		}
	}
}

// Given more stored records than the limit, when reading Recent, then only
// the newest `limit` records are returned.
func TestLog_RecentHonoursLimit(t *testing.T) {
	l := New()
	for seq := uint64(1); seq <= 10; seq++ {
		l.Add(rec(seq))
	}

	got := l.Recent(3)
	if len(got) != 3 {
		t.Fatalf("len = %d, want 3", len(got))
	}
	if got[0].Seq != 10 || got[2].Seq != 8 {
		t.Fatalf("got seqs %d..%d, want 10..8", got[0].Seq, got[2].Seq)
	}
}

// Given a limit larger than the capacity, when reading Recent, then at most
// 50 records come back (the ring cannot hold more).
func TestLog_LimitAboveCapacityReturnsAll(t *testing.T) {
	l := New()
	for seq := uint64(1); seq <= 51; seq++ {
		l.Add(rec(seq))
	}

	if got := l.Recent(999); len(got) != 50 {
		t.Fatalf("len = %d, want 50", len(got))
	}
}

// Given boundary limits, when reading Recent, then limit<=0 returns all
// stored records and an empty ring returns an empty slice.
func TestLog_RecentBoundaryLimits(t *testing.T) {
	l := New()
	if got := l.Recent(10); len(got) != 0 {
		t.Fatalf("empty ring: len = %d, want 0", len(got))
	}

	for seq := uint64(1); seq <= 5; seq++ {
		l.Add(rec(seq))
	}
	if got := l.Recent(0); len(got) != 5 {
		t.Fatalf("limit=0: len = %d, want 5 (all stored)", len(got))
	}
	if got := l.Recent(-1); len(got) != 5 {
		t.Fatalf("limit=-1: len = %d, want 5 (all stored)", len(got))
	}
}

// Given concurrent Add and Recent, when run under -race, then no data race
// fires and the log stays consistent.
func TestLog_ConcurrentAddRecent(t *testing.T) {
	l := New()
	var wg sync.WaitGroup
	for w := 0; w < 4; w++ {
		wg.Add(2)
		go func(base uint64) {
			defer wg.Done()
			for i := uint64(0); i < 200; i++ {
				l.Add(rec(base + i))
			}
		}(uint64(w * 1000))
		go func() {
			defer wg.Done()
			for i := 0; i < 200; i++ {
				_ = l.Recent(10)
			}
		}()
	}
	wg.Wait()

	if got := l.Recent(100); len(got) != 50 {
		t.Fatalf("len = %d, want 50 after 800 adds", len(got))
	}
}

// Given plans in both wire shapes, when counting, then PinBlobMetas (new CP)
// is authoritative over legacy PinBlobs and unpins always total.
func TestCounts_MetasAuthoritativeOverLegacy(t *testing.T) {
	plan := types.PinPlan{
		Updates: []types.PinUpdate{
			{ // new-CP shape: metas win over the legacy list
				PinBlobs:     []string{"legacy-ignored"},
				PinBlobMetas: []types.PinBlobMeta{{BlobHash: "a"}, {BlobHash: "b"}, {BlobHash: "c"}},
				UnpinBlobs:   []string{"u1"},
			},
			{ // legacy shape: plain lists counted
				PinBlobs:   []string{"p1", "p2"},
				UnpinBlobs: []string{"u2", "u3", "u4"},
			},
		},
	}

	pins, unpins := Counts(plan)
	if pins != 5 {
		t.Fatalf("pins = %d, want 5 (3 metas + 2 legacy)", pins)
	}
	if unpins != 4 {
		t.Fatalf("unpins = %d, want 4", unpins)
	}
}

// Given an empty plan, when counting, then both totals are zero.
func TestCounts_EmptyPlan(t *testing.T) {
	pins, unpins := Counts(types.PinPlan{})
	if pins != 0 || unpins != 0 {
		t.Fatalf("Counts = (%d, %d), want (0, 0)", pins, unpins)
	}
}
