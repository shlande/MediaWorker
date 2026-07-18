// Package gossippop implements GossipSub-based peer exchange, heartbeat,
// and weighted popularity synchronisation across edge nodes.
//
// # Architecture
//
// Each edge node maintains:
//   - LocalPopularity: a 6-minute sliding window of per-blob request counts
//   - PeerScorer: application-level reputation scores for known peers
//   - MergedPopularity: a weighted-merge view built from GossipSub updates
//
// Every 30 seconds, each node snapshots its LocalPopularity, signs the
// snapshot with its Ed25519 key, and publishes it to the "edge-popularity-v1"
// GossipSub topic. Receiving nodes verify the signature, look up the source
// peer's score, and merge the counts using a weighted average formula.
//
// # Poisoning defence
//
// Peer scores gate trust: updates from peers scoring below GraylistThreshold
// (-10.0) are dropped. A single malicious peer reporting inflated counts
// has negligible impact if its score is low. Sybil attacks are mitigated by
// GossipSub's IPColocationFactor penalty.
//
// # Testing
//
// Test hosts are created with libp2phost.NewEdgeHost (PSK, TCP only) and
// Pre-seeded with high scores via PeerScorer.RecordICPSuccess (11+ calls to
// exceed MinTrustedWeight of 5.0).
package gossippop

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	pubsub "github.com/libp2p/go-libp2p-pubsub"
	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/peer"
)

// NewGossipSub creates a GossipSub service with peer scoring enabled.
// The AppSpecificScore function is wired to the provided PeerScorer.
//
// Parameters:
//   - ctx: lifecycle context
//   - h: the libp2p host to attach GossipSub to
//   - scorer: the PeerScorer providing AppSpecificScore
func NewGossipSub(ctx context.Context, h host.Host, scorer *PeerScorer) (*pubsub.PubSub, error) {
	scoreParams := &pubsub.PeerScoreParams{
		// Per-topic scoring: none for now — all scoring is app-specific.
		Topics: make(map[string]*pubsub.TopicScoreParams),

		// App-specific scoring: the PeerScorer's reputation.
		AppSpecificScore:  scorer.AppSpecificScore,
		AppSpecificWeight: 1.0,

		// IP colocation penalty: punish Sybil nodes sharing an IP.
		IPColocationFactorWeight:    -10.0,
		IPColocationFactorThreshold: 5,

		// Decay: scores slowly revert to 0 for disconnected peers.
		DecayInterval: 12 * time.Second,
		DecayToZero:   0.01,
		RetainScore:   10 * time.Minute,
	}

	scoreThresholds := &pubsub.PeerScoreThresholds{
		GossipThreshold:             GossipThreshold,             // -5.0
		PublishThreshold:            PublishThreshold,            // -20.0
		GraylistThreshold:           GraylistThreshold,           // -10.0
		AcceptPXThreshold:           10,                          // PX safety margin
		OpportunisticGraftThreshold: OpportunisticGraftThreshold, // 5.0
	}

	ps, err := pubsub.NewGossipSub(ctx, h,
		pubsub.WithPeerScore(scoreParams, scoreThresholds),
		pubsub.WithGossipSubParams(func() pubsub.GossipSubParams {
			params := pubsub.DefaultGossipSubParams()
			params.D = 6
			params.Dlo = 2
			params.Dhi = 8
			params.Dout = 1
			params.HeartbeatInterval = 300 * time.Millisecond
			return params
		}()),
	)
	if err != nil {
		return nil, fmt.Errorf("new gossipsub: %w", err)
	}

	return ps, nil
}

// JoinTopic subscribes to the popularity topic and returns the topic handle.
func JoinTopic(ctx context.Context, ps *pubsub.PubSub, topicName string) (*pubsub.Topic, error) {
	topic, err := ps.Join(topicName)
	if err != nil {
		return nil, fmt.Errorf("join topic %q: %w", topicName, err)
	}
	slog.Default().With("component", "gossippop").Info("joined gossipsub topic", "topic", topicName)
	return topic, nil
}

// Ed25519PubKey extracts the Ed25519 public key bytes from a libp2p peer.ID.
// Returns nil if the peer.ID does not embed an Ed25519 key.
func Ed25519PubKey(id peer.ID) []byte {
	// peer.ID embeds the public key if the peer was created with
	// peer.IDFromPublicKey; otherwise the key is unknown.
	pub, err := id.ExtractPublicKey()
	if err != nil {
		return nil
	}
	raw, err := pub.Raw()
	if err != nil {
		return nil
	}
	return raw
}
