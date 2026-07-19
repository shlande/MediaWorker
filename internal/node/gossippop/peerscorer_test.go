package gossippop

import (
	"testing"

	"github.com/shlande/mediaworker/internal/types"
)

// Given a scorer with no recorded scores, When GraylistedCount is read,
// Then it is 0.
func TestGraylistedCount_ZeroInitially(t *testing.T) {
	s := NewPeerScorer()
	if got := s.GraylistedCount(); got != 0 {
		t.Errorf("GraylistedCount() = %d, want 0", got)
	}
}

// Given peers driven below the threshold via RecordMisbehavior, When
// GraylistedCount is read, Then exactly the below-threshold peers count.
func TestGraylistedCount_CountsBelowThreshold(t *testing.T) {
	s := NewPeerScorer()
	// -5 * 4 = -20 → at threshold (<=) → counts.
	bad1 := types.PeerId("peer-bad-1")
	for i := 0; i < 4; i++ {
		s.RecordMisbehavior(bad1, MisbehaviorInvalidSig)
	}
	// -5 * 5 = -25 → below threshold → counts.
	bad2 := types.PeerId("peer-bad-2")
	for i := 0; i < 5; i++ {
		s.RecordMisbehavior(bad2, MisbehaviorPoisonedHeat)
	}
	// -5 * 3 = -15 → above threshold → does NOT count.
	ok := types.PeerId("peer-ok")
	for i := 0; i < 3; i++ {
		s.RecordMisbehavior(ok, MisbehaviorRateLimitViolation)
	}

	if got := s.GraylistedCount(); got != 2 {
		t.Errorf("GraylistedCount() = %d, want 2 (bad1 at -20, bad2 at -25; ok at -15)", got)
	}
}

// Given a peer that crossed the threshold and later recovered above it, When
// GraylistedCount is read, Then the recovered peer is excluded (live-score
// semantics, even though the sticky graylisted set still contains it).
func TestGraylistedCount_RecoveredPeerExcluded(t *testing.T) {
	s := NewPeerScorer()
	p := types.PeerId("peer-recover")
	for i := 0; i < 4; i++ {
		s.RecordMisbehavior(p, MisbehaviorInvalidSig) // -20 → graylisted
	}
	if !s.IsGraylisted(p) {
		t.Fatal("precondition: peer should be in the sticky graylisted set")
	}
	s.RecordICPSuccess(p) // -19.5 → above threshold

	if got := s.GraylistedCount(); got != 0 {
		t.Errorf("GraylistedCount() = %d, want 0 (score recovered to -19.5)", got)
	}
	if !s.IsGraylisted(p) {
		t.Error("IsGraylisted should stay true (sticky set) — the two views diverge by design")
	}
}
