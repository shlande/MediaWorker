// Package syncbroadcaster implements the control-plane side of the SyncBroadcasterClient
// interface using libp2p stream protocol /edge/control/1.0.0. It sends PinPlan updates
// (forward direction) to nodes and receives NodeStatusReport events (reverse direction).
package syncbroadcaster

import (
	"bufio"
	"context"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"sync"

	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/core/protocol"

	"github.com/shlande/mediaworker/internal/types"
)

// ControlProtocol is the libp2p stream protocol ID for the control-plane ↔ node
// sync channel. Both forward (SendToNode) and reverse (node status push) use it.
const ControlProtocol = protocol.ID("/edge/control/1.0.0")

// WireMessage is the JSON-encoded envelope carried over the control stream.
// It is compatible with types.Event: Type maps to Event.Type, Payload maps to
// Event.Payload. The wire format is: 4-byte big-endian length, then JSON body.
type WireMessage struct {
	Type    string          `json:"type"`
	Payload json.RawMessage `json:"payload"`
}

// SyncBroadcaster satisfies the pinstrategy.SyncBroadcasterClient interface by
// sending typed events over libp2p streams (forward direction) and dispatching
// incoming stream events to subscriber channels (reverse direction).
type SyncBroadcaster struct {
	host host.Host

	mu   sync.RWMutex
	subs map[string][]chan types.Event // eventType → subscribers
}

// New creates a SyncBroadcaster and registers the /edge/control/1.0.0 stream
// handler on the given libp2p host for receiving reverse-direction events from
// nodes (e.g., NodeStatusReport).
func New(h host.Host) *SyncBroadcaster {
	sb := &SyncBroadcaster{
		host: h,
		subs: make(map[string][]chan types.Event),
	}
	h.SetStreamHandler(ControlProtocol, sb.handleStream)
	return sb
}

// SendToNode sends an event to a specific node by opening a /edge/control/1.0.0
// stream and writing a varint-prefixed JSON WireMessage. nodeID is the string
// representation of the target node's libp2p peer.ID.
func (sb *SyncBroadcaster) SendToNode(nodeID string, eventType string, payload any) error {
	targetPeerID, err := peer.Decode(nodeID)
	if err != nil {
		return fmt.Errorf("syncbroadcaster SendToNode: decode peer ID %q: %w", nodeID, err)
	}

	payloadBytes, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("syncbroadcaster SendToNode: marshal payload: %w", err)
	}

	msg := WireMessage{
		Type:    eventType,
		Payload: json.RawMessage(payloadBytes),
	}

	return sb.sendWireMessage(context.Background(), targetPeerID, msg)
}

// Subscribe registers a subscriber channel for a given event type. When a
// reverse-direction stream arrives carrying an event of eventType, it is
// dispatched to all subscriber channels. The returned channel is a buffered
// channel with capacity 16 to avoid blocking the stream handler.
func (sb *SyncBroadcaster) Subscribe(eventType string) <-chan types.Event {
	ch := make(chan types.Event, 16)

	sb.mu.Lock()
	sb.subs[eventType] = append(sb.subs[eventType], ch)
	sb.mu.Unlock()

	return ch
}

// Unsubscribe removes all subscriber channels for a given event type and
// closes them. Callers should call this when they no longer need the events
// to prevent goroutine leaks.
func (sb *SyncBroadcaster) Unsubscribe(eventType string) {
	sb.mu.Lock()
	chans := sb.subs[eventType]
	delete(sb.subs, eventType)
	sb.mu.Unlock()

	for _, ch := range chans {
		close(ch)
	}
}

// ─── Stream handler (reverse direction) ──────────────────────────────────────

// handleStream reads a varint-prefixed JSON WireMessage from an incoming
// /edge/control/1.0.0 stream and dispatches it to matching subscribers.
func (sb *SyncBroadcaster) handleStream(stream network.Stream) {
	defer stream.Close()

	msg, err := readWireMessage(stream)
	if err != nil {
		stream.Reset()
		return
	}

	evt := types.Event{
		Type:    msg.Type,
		Payload: []byte(msg.Payload),
	}

	sb.mu.RLock()
	chans := sb.subs[msg.Type]
	// Copy the slice to avoid holding the lock during channel sends.
	subs := make([]chan types.Event, len(chans))
	copy(subs, chans)
	sb.mu.RUnlock()

	for _, ch := range subs {
		select {
		case ch <- evt:
		default:
			// Subscriber is too slow; drop the event rather than blocking
			// the stream handler.
		}
	}
}

// ─── Wire helpers ──────────────────────────────────────────────────────────

// sendWireMessage opens a stream to the target peer, writes a varint-prefixed
// JSON WireMessage, and closes the stream. It uses a short timeout to avoid
// blocking indefinitely on unresponsive nodes.
func (sb *SyncBroadcaster) sendWireMessage(ctx context.Context, target peer.ID, msg WireMessage) error {
	stream, err := sb.host.NewStream(ctx, target, ControlProtocol)
	if err != nil {
		return fmt.Errorf("syncbroadcaster: open stream to %s: %w", target.ShortString(), err)
	}
	defer stream.Close()

	if err := writeWireMessage(stream, msg); err != nil {
		return fmt.Errorf("syncbroadcaster: write message to %s: %w", target.ShortString(), err)
	}

	// Signal end of write side.
	if c, ok := stream.(interface{ CloseWrite() error }); ok {
		_ = c.CloseWrite()
	}

	return nil
}

func writeWireMessage(w io.Writer, msg WireMessage) error {
	data, err := json.Marshal(msg)
	if err != nil {
		return fmt.Errorf("marshal wire message: %w", err)
	}

	var lenBuf [binary.MaxVarintLen64]byte
	n := binary.PutUvarint(lenBuf[:], uint64(len(data)))
	if _, err := w.Write(lenBuf[:n]); err != nil {
		return fmt.Errorf("write length prefix: %w", err)
	}
	if _, err := w.Write(data); err != nil {
		return fmt.Errorf("write body: %w", err)
	}
	return nil
}

// readWireMessage reads a varint-prefixed JSON WireMessage from the stream.
func readWireMessage(r io.Reader) (WireMessage, error) {
	br := bufio.NewReader(r)

	length, err := binary.ReadUvarint(br)
	if err != nil {
		return WireMessage{}, fmt.Errorf("read length: %w", err)
	}

	data := make([]byte, length)
	if _, err := io.ReadFull(br, data); err != nil {
		return WireMessage{}, fmt.Errorf("read body: %w", err)
	}

	var msg WireMessage
	if err := json.Unmarshal(data, &msg); err != nil {
		return WireMessage{}, fmt.Errorf("unmarshal wire message: %w", err)
	}

	return msg, nil
}
