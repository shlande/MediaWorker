package gossippop

import (
	"log/slog"
	"sync"

	"github.com/libp2p/go-libp2p/core/peer"

	"github.com/shlande/mediaworker/internal/types"
)

// ─── GossipSub Score Constants ───

const (
	// GraylistThreshold is the score below which peers are rejected by
	// ConnectionGater and excluded from the hash ring.
	GraylistThreshold = -20.0
	// PublishThreshold is the score below which flood-publish is suppressed.
	PublishThreshold = -10.0
	// GossipThreshold is the score below which IHAVE/IWANT is suppressed.
	GossipThreshold = -5.0
	// OpportunisticGraftThreshold is the mesh median below which high-score
	// peers are opportunistically grafted.
	OpportunisticGraftThreshold = 5.0
	// MinTrustedWeight is the minimum cumulative source score required
	// before a merged popularity entry is considered trustworthy.
	MinTrustedWeight = 5.0
)

// MisbehaviorKind classifies the type of protocol violation a peer committed.
type MisbehaviorKind int

const (
	// MisbehaviorInvalidSig indicates an Ed25519 signature verification failure.
	MisbehaviorInvalidSig MisbehaviorKind = iota
	// MisbehaviorPoisonedHeat indicates the peer reported a heat value that
	// deviates >5x from the majority.
	MisbehaviorPoisonedHeat
	// MisbehaviorRateLimitViolation indicates the peer exceeded connection limits.
	MisbehaviorRateLimitViolation
)

// PeerScorer maintains application-level reputation scores for each peer.
// It is the source of truth for GossipSub's AppSpecificScore callback.
//
// Scores start at 0 (neutral). Positive events increase the score (capped
// at +10), negative events decrease it. Peers dropping below GraylistThreshold
// are marked as graylisted.
type PeerScorer struct {
	scores     sync.Map // map[types.PeerId]float64
	graylisted sync.Map // map[types.PeerId]struct{}
	logger     *slog.Logger
}

func NewPeerScorer() *PeerScorer {
	return &PeerScorer{
		logger: slog.Default().With("component", "peer_scorer"),
	}
}

// AppSpecificScore is the GossipSub callback that returns the stored score for
// the given peer. Unknown peers return 0 (neutral).
func (s *PeerScorer) AppSpecificScore(p peer.ID) float64 {
	v, ok := s.scores.Load(types.PeerId(p.String()))
	if !ok {
		return 0
	}
	return v.(float64)
}

// GetScore returns the current score for the given peer, or 0 if unknown.
func (s *PeerScorer) GetScore(p types.PeerId) float64 {
	v, ok := s.scores.Load(p)
	if !ok {
		return 0
	}
	return v.(float64)
}

// RecordICPSuccess adds 0.5 per successful ICP delivery, capped at +10.
func (s *PeerScorer) RecordICPSuccess(p types.PeerId) {
	s.adjust(p, min(0.5, 10.0-s.current(p)))
}

// RecordICPTimeout subtracts 1.0 per ICP timeout/failure.
func (s *PeerScorer) RecordICPTimeout(p types.PeerId) {
	s.adjust(p, -1.0)
}

// RecordBandwidthContributed adds bytes/1e9 (1 point per GB transferred).
func (s *PeerScorer) RecordBandwidthContributed(p types.PeerId, bytes int64) {
	s.adjust(p, float64(bytes)/1e9)
}

// RecordMisbehavior penalizes the peer by 5.0 points. If the resulting score
// falls to or below GraylistThreshold, the peer is graylisted.
func (s *PeerScorer) RecordMisbehavior(p types.PeerId, kind MisbehaviorKind) {
	s.adjust(p, -5.0)
	newScore := s.current(p)
	if newScore <= GraylistThreshold {
		s.markGraylisted(p)
		s.logger.Warn("peer graylisted due to misbehavior",
			"peer", p, "kind", kind, "score", newScore, "threshold", GraylistThreshold)
	} else {
		s.logger.Debug("peer penalized for misbehavior",
			"peer", p, "kind", kind, "score", newScore)
	}
}

// IsGraylisted returns true if the peer has been graylisted.
func (s *PeerScorer) IsGraylisted(p types.PeerId) bool {
	_, ok := s.graylisted.Load(p)
	return ok
}

// markGraylisted records that the peer is below GraylistThreshold.
func (s *PeerScorer) markGraylisted(p types.PeerId) {
	s.graylisted.Store(p, struct{}{})
}

// ─── internal helpers ───

// current returns the stored score without an atomic load/store dance; the
// caller must hold whatever coordination it needs.
func (s *PeerScorer) current(p types.PeerId) float64 {
	v, ok := s.scores.Load(p)
	if !ok {
		return 0
	}
	return v.(float64)
}

// adjust atomically applies delta to the peer's score.
func (s *PeerScorer) adjust(p types.PeerId, delta float64) {
	// sync.Map does not offer an atomic update, so we do a load-add-store
	// loop. This is fine because PeerScorer scores are eventually consistent
	// per peer; concurrent adjustments from different goroutines on the same
	// peer are rare and the lost-update window is acceptable.
	for {
		old := s.current(p)
		new := old + delta
		if old == 0 && delta == 0 {
			return
		}
		if old == 0 {
			if _, loaded := s.scores.LoadOrStore(p, new); !loaded {
				return
			}
			continue
		}
		if s.scores.CompareAndSwap(p, old, new) {
			return
		}
	}
}
