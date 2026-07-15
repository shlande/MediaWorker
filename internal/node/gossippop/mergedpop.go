package gossippop

import (
	"context"
	"crypto/ed25519"
	"encoding/json"
	"sync"
	"time"

	"github.com/libp2p/go-libp2p/core/peer"
	pubsub "github.com/libp2p/go-libp2p-pubsub"

	"github.com/shlande/mediaworker/internal/types"
)

// GossipSub topic name for edge popularity synchronization.
const PopularityTopic = "edge-popularity-v1"

// ─── PopularityUpdate message ───

// PopularityUpdate is the GossipSub message published by each edge node every
// 30 seconds. It carries a snapshot of the local sliding-window counters,
// signed with the node's Ed25519 private key.
type PopularityUpdate struct {
	PeerID    types.PeerId `json:"peer_id"`
	Timestamp int64        `json:"timestamp"`
	Counts    map[string]int64 `json:"counts"`
	Sig       []byte       `json:"sig"`
}

// payloadForSigning returns the JSON bytes of the update fields that are
// covered by the Ed25519 signature (PeerID, Timestamp, Counts).
func (u *PopularityUpdate) payloadForSigning() ([]byte, error) {
	return json.Marshal(struct {
		PeerID    types.PeerId   `json:"peer_id"`
		Timestamp int64          `json:"timestamp"`
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
	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			snapshot := localPop.Snapshot()
			if len(snapshot) == 0 {
				continue // nothing to publish
			}

			update := PopularityUpdate{
				PeerID:    peerID,
				Timestamp: time.Now().Unix(),
				Counts:    snapshot,
			}

			payload, err := update.payloadForSigning()
			if err != nil {
				continue
			}
			update.Sig = ed25519.Sign(privKey, payload)

			data, err := json.Marshal(update)
			if err != nil {
				continue
			}
			topic.Publish(ctx, data) // failure is silent
		}
	}
}

// ─── Handler (subscriber side) ───

// HandlePopularityMessage decodes an incoming GossipSub message, verifies the
// signature, checks the source score, and merges into the local view.
func HandlePopularityMessage(
	mp *MergedPopularity,
	scorer *PeerScorer,
	msg *pubsub.Message,
	h interface {
		Peerstore() interface {
			PubKey(peer.ID) ed25519.PublicKey
		}
	},
) {
	var update PopularityUpdate
	if err := json.Unmarshal(msg.Data, &update); err != nil {
		return
	}

	sourceScore := scorer.GetScore(types.PeerId(msg.ReceivedFrom.String()))

	// Extract Ed25519 public key from the publisher's peer.ID.
	// In tests this comes from the peerstore via the host.
	pubKey := h.Peerstore().PubKey(msg.ReceivedFrom)
	if pubKey == nil {
		return
	}

	// Ignore return error; invalid sigs are handled inside OnPopularityUpdate.
	_ = mp.OnPopularityUpdate(&update, sourceScore, pubKey)
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
