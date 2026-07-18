package gossippop

import (
	"context"
	"crypto/ed25519"
	"encoding/json"
	"log/slog"
	"sync"
	"time"

	pubsub "github.com/libp2p/go-libp2p-pubsub"
	"github.com/libp2p/go-libp2p/core/peer"

	"github.com/shlande/mediaworker/internal/types"
)

// GossipSub topic name for edge popularity synchronization.
const PopularityTopic = "edge-popularity-v1"

// ─── PopularityUpdate message ───

// PopularityUpdate is the GossipSub message published by each edge node every
// 30 seconds. It carries a snapshot of the local sliding-window counters,
// signed with the node's Ed25519 private key.
type PopularityUpdate struct {
	PeerID    types.PeerId     `json:"peer_id"`
	Timestamp int64            `json:"timestamp"`
	Counts    map[string]int64 `json:"counts"`
	Sig       []byte           `json:"sig"`
}

// payloadForSigning returns the JSON bytes of the update fields that are
// covered by the Ed25519 signature (PeerID, Timestamp, Counts).
func (u *PopularityUpdate) payloadForSigning() ([]byte, error) {
	return json.Marshal(struct {
		PeerID    types.PeerId     `json:"peer_id"`
		Timestamp int64            `json:"timestamp"`
		Counts    map[string]int64 `json:"counts"`
	}{
		PeerID:    u.PeerID,
		Timestamp: u.Timestamp,
		Counts:    u.Counts,
	})
}

// ─── MergedPopularity ───

// MergedEntry holds the weighted-average popularity for a single blob hash
// as reported by multiple peers. The entry is considered trustworthy when
// TotalWeight > MinTrustedWeight.
type MergedEntry struct {
	WeightedHeat float64
	TotalWeight  float64
	LastUpdate   int64
}

// MergedPopularity maintains the weighted-merge view of blob popularity
// reported by peers via GossipSub. Each update is weighted by the source
// peer's reputation score.
type MergedPopularity struct {
	mu      sync.RWMutex
	entries map[string]*MergedEntry // key = blob_hash
}

// NewMergedPopularity returns an initialised MergedPopularity.
func NewMergedPopularity() *MergedPopularity {
	return &MergedPopularity{
		entries: make(map[string]*MergedEntry),
	}
}

// OnPopularityUpdate processes a GossipSub popularity update from a remote
// peer. It verifies the signature, checks the source peer's score, and
// performs a weighted merge into the local view.
//
// The scorer is used to look up the source peer's reputation; updates from
// peers scoring below GraylistThreshold are dropped.
//
// The host is used to extract the Ed25519 public key from the peer.ID for
// signature verification.
func (mp *MergedPopularity) OnPopularityUpdate(
	update *PopularityUpdate,
	sourceScore float64,
	pubKey ed25519.PublicKey,
) error {
	// 1. Verify signature.
	payload, err := update.payloadForSigning()
	if err != nil {
		return err
	}
	if !ed25519.Verify(pubKey, payload, update.Sig) {
		return errInvalidSig
	}

	// 2. Drop updates from greylisted peers.
	if sourceScore <= GraylistThreshold {
		return errScoreTooLow
	}

	// 3. Weighted merge.
	mp.mu.Lock()
	defer mp.mu.Unlock()

	for blobHash, count := range update.Counts {
		entry, ok := mp.entries[blobHash]
		if !ok {
			entry = &MergedEntry{}
			mp.entries[blobHash] = entry
		}
		// Incremental weighted average:
		//   new_heat = (old_w * old_heat + src_score * count) / (old_w + src_score)
		entry.WeightedHeat = (entry.TotalWeight*entry.WeightedHeat + sourceScore*float64(count)) /
			(entry.TotalWeight + sourceScore)
		entry.TotalWeight += sourceScore
		entry.LastUpdate = update.Timestamp
	}
	return nil
}

// getVideoPopularity returns the weighted heat for a blob hash from the
// merged gossip view. If the entry's TotalWeight <= MinTrustedWeight the
// result is not considered trustworthy and 0 is returned (fallback to PG
// query, which is a mock in this implementation).
func (mp *MergedPopularity) getVideoPopularity(blobHash string) float64 {
	mp.mu.RLock()
	defer mp.mu.RUnlock()

	entry, ok := mp.entries[blobHash]
	if !ok || entry.TotalWeight < MinTrustedWeight {
		return 0 // fallback to PG; mock returns 0
	}
	return entry.WeightedHeat
}

// Snapshot returns a copy of the merged popularity view containing only
// entries whose TotalWeight >= MinTrustedWeight (the same trust threshold used
// by getVideoPopularity). The returned map is safe for the caller to mutate.
//
// This is the bridge between the GossipSub-driven MergedPopularity and the
// cache eviction PopSource: edge main wires a closure that converts this map
// into []*cache.VideoMeta so that WarmCache.Evict can rank blobs by gossip
// heat instead of falling back to PG.
func (mp *MergedPopularity) Snapshot() map[string]float64 {
	mp.mu.RLock()
	defer mp.mu.RUnlock()

	out := make(map[string]float64, len(mp.entries))
	for hash, entry := range mp.entries {
		if entry.TotalWeight < MinTrustedWeight {
			continue
		}
		out[hash] = entry.WeightedHeat
	}
	return out
}

// ─── Publish helper ───

// PublishPopularity is a periodic background goroutine that snapshots the
// local popularity window, signs it with the node's Ed25519 private key,
// and publishes the update to the given GossipSub topic.
//
// Parameters:
//   - ctx: controls the goroutine lifetime
//   - topic: the pubsub topic handle to publish on
//   - localPop: the node's LocalPopularity sliding window
//   - peerID: the node's PeerId
//   - privKey: the node's Ed25519 private key for signing updates
func PublishPopularity(
	ctx context.Context,
	topic *pubsub.Topic,
	localPop *LocalPopularity,
	peerID types.PeerId,
	privKey ed25519.PrivateKey,
) {
	logger := slog.Default().With("component", "gossippop_publisher", "peer_id", peerID)
	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			snapshot := localPop.Snapshot()
			if len(snapshot) == 0 {
				logger.Debug("skipping publish: empty snapshot")
				continue
			}

			update := PopularityUpdate{
				PeerID:    peerID,
				Timestamp: time.Now().Unix(),
				Counts:    snapshot,
			}

			payload, err := update.payloadForSigning()
			if err != nil {
				logger.Debug("skipping publish: payload signing failed", "err", err)
				continue
			}
			update.Sig = ed25519.Sign(privKey, payload)

			data, err := json.Marshal(update)
			if err != nil {
				logger.Debug("skipping publish: marshal failed", "err", err)
				continue
			}

			if err := topic.Publish(ctx, data); err != nil {
				logger.Debug("publish failed", "err", err, "blobs", len(snapshot))
			} else {
				logger.Debug("published popularity update", "blobs", len(snapshot))
			}
		}
	}
}

// ─── Handler (subscriber side) ───

// PeerEntryLookup is the narrow read-only view of a peer entry store that
// HandlePopularityMessage consults to reject heat from stale or unknown peers.
//
// The concrete implementation is *peerstore.PeerEntryStore (Get returns
// (types.PeerStoreEntry, bool); Stale=true signals JWT-expired or evicted
// peers). The interface lives here so gossippop does not import
// internal/node/peerstore — mirroring the inline-interface pattern used by
// the host adapter parameter below.
//
// A nil PeerEntryLookup is treated as "trust everyone": the guard is skipped,
// preserving backward compatibility for assembly paths without a PeerEntryStore.
type PeerEntryLookup interface {
	// StaleOrUnknown reports whether peerID is absent from the local store
	// (unknown) or marked stale (JWT expired / evicted). Returns true in
	// either case so the caller can discard the heat update.
	StaleOrUnknown(peerID types.PeerId) bool
}

// HandlePopularityMessage decodes an incoming GossipSub message, verifies the
// signature, checks the source score, and merges into the local view.
//
// Defense layers (order matters, all are logged at Debug on rejection):
//  1. Trust guard: peerEntryStore.StaleOrUnknown → discard heat from stale
//     or unknown peers (T19). nil peerEntryStore skips this guard.
//  2. Signature verification (OnPopularityUpdate:86-93).
//  3. GraylistThreshold floor (OnPopularityUpdate:96).
//  4. MinTrustedWeight output gate (Snapshot / getVideoPopularity:124-133).
//  5. Incremental weighted-average is naturally immune to zero-score /
//     zero-weight injection (OnPopularityUpdate:110-114).
func HandlePopularityMessage(
	mp *MergedPopularity,
	scorer *PeerScorer,
	msg *pubsub.Message,
	h interface {
		Peerstore() interface {
			PubKey(peer.ID) ed25519.PublicKey
		}
	},
	peerEntryStore PeerEntryLookup,
) {
	logger := slog.Default().With("component", "gossippop_receiver", "from", msg.ReceivedFrom.ShortString())

	var update PopularityUpdate
	if err := json.Unmarshal(msg.Data, &update); err != nil {
		logger.Debug("dropped message: unmarshal failed", "err", err, "bytes", len(msg.Data))
		return
	}

	// 1. Trust guard: reject heat from stale or unknown peers.
	if peerEntryStore != nil {
		if peerEntryStore.StaleOrUnknown(types.PeerId(msg.ReceivedFrom.String())) {
			logger.Debug("dropped popularity update: peer stale or unknown",
				"peer", msg.ReceivedFrom.ShortString(), "blobs", len(update.Counts))
			return
		}
	}

	sourceScore := scorer.GetScore(types.PeerId(msg.ReceivedFrom.String()))

	pubKey := h.Peerstore().PubKey(msg.ReceivedFrom)
	if pubKey == nil {
		logger.Debug("dropped message: no public key for peer")
		return
	}

	if err := mp.OnPopularityUpdate(&update, sourceScore, pubKey); err != nil {
		logger.Debug("dropped popularity update",
			"err", err, "source_score", sourceScore, "blobs", len(update.Counts))
	} else {
		logger.Debug("merged popularity update",
			"source_score", sourceScore, "blobs", len(update.Counts))
	}
}

// ─── Errors ───

var (
	errInvalidSig  = &PopularityError{kind: "invalid_signature"}
	errScoreTooLow = &PopularityError{kind: "score_too_low"}
)

// PopularityError is returned when a popularity update is rejected.
type PopularityError struct {
	kind string
}

func (e *PopularityError) Error() string {
	return "gossippop: " + e.kind
}
