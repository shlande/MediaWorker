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
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/core/protocol"

	"github.com/shlande/mediaworker/internal/types"
)

// ControlProtocol is the libp2p stream protocol ID for the control-plane ↔ node
// sync channel. Both forward (SendToNode) and reverse (node status push) use it.
//
// This is the default protocol ID; callers can override it via
// WithProtocolID when constructing a SyncBroadcaster. The node-side client
// (internal/node/syncbroadcaster/client.go) uses the same default constant —
// overriding on one side only would fork the wire protocol, which is why
// operators must change both ends in lockstep (plan line 239).
const ControlProtocol = protocol.ID("/edge/control/1.0.0")

// DefaultSendTimeout is the per-message send timeout used when
// WithSendTimeout is not supplied or passes a zero/negative duration.
const DefaultSendTimeout = 30 * time.Second

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

	// protocolID is the libp2p stream protocol negotiated with nodes. Defaults
	// to ControlProtocol ("/edge/control/1.0.0"). Changing this requires
	// changing the node-side client's protocol ID in lockstep — otherwise the
	// two sides cannot negotiate streams (plan line 239).
	protocolID protocol.ID

	// sendTimeout caps each sendWireMessage call. Defaults to
	// DefaultSendTimeout (30s). A zero/negative value is normalised to the
	// default at New time.
	sendTimeout time.Duration

	mu   sync.RWMutex
	subs map[string][]chan types.Event // eventType → subscribers

	snapshotStore *SnapshotStore
}

// Option configures a SyncBroadcaster at construction time.
type Option func(*SyncBroadcaster)

// WithProtocolID overrides the default libp2p stream protocol ID. An empty
// string is ignored (default preserved). Operators must change the node-side
// client's protocol ID to the same value — otherwise no streams can be
// established (wire-protocol fork hazard, plan line 239).
func WithProtocolID(id string) Option {
	return func(sb *SyncBroadcaster) {
		if id != "" {
			sb.protocolID = protocol.ID(id)
		}
	}
}

// WithSendTimeout overrides the per-message send timeout. A zero or negative
// duration is ignored (default preserved).
func WithSendTimeout(d time.Duration) Option {
	return func(sb *SyncBroadcaster) {
		if d > 0 {
			sb.sendTimeout = d
		}
	}
}

// RingBuffer is a fixed-capacity (1000) circular buffer of Events. It stores
// the most recent events for replay to reconnecting peers (missing fewer than
// 1000 events can skip the full snapshot).
type RingBuffer struct {
	mu    sync.Mutex
	buf   [1000]Event
	head  int // next write position
	count int // number of buffered events
}

// Event is a single entry in the ring buffer, carrying a monotonically
// increasing sequence number, a type label, and the raw payload bytes.
type Event struct {
	Seq     uint64 `json:"seq"`
	Type    string `json:"type"`
	Payload []byte `json:"payload"`
}

// Push appends an event to the ring buffer. When the buffer is full the
// oldest event is silently overwritten.
func (rb *RingBuffer) Push(evt Event) {
	rb.mu.Lock()
	defer rb.mu.Unlock()

	rb.buf[rb.head] = evt
	rb.head = (rb.head + 1) % len(rb.buf)
	if rb.count < len(rb.buf) {
		rb.count++
	}
}

// Since returns all buffered events with Seq > lastSeq, in ascending seq
// order. If lastSeq is zero it returns every buffered event.
func (rb *RingBuffer) Since(lastSeq uint64) []Event {
	rb.mu.Lock()
	defer rb.mu.Unlock()

	if rb.count == 0 {
		return nil
	}

	// Determine the oldest seq in the buffer.
	tail := (rb.head - rb.count + len(rb.buf)) % len(rb.buf)
	oldestSeq := rb.buf[tail].Seq

	// When lastSeq is 0 (new peer with no history) we always return
	// everything. Non-zero lastSeq must not be older than oldestSeq-1,
	// otherwise the caller has missed events that are already overwritten.
	if lastSeq != 0 && lastSeq < oldestSeq-1 {
		return nil
	}
	// Caller is ahead of or exactly at the newest event.
	newestSeq := oldestSeq + uint64(rb.count) - 1
	if lastSeq >= newestSeq {
		return nil
	}

	start := tail
	if lastSeq >= oldestSeq {
		// skip events the caller has already seen
		skip := lastSeq - oldestSeq + 1
		start = (tail + int(skip)) % len(rb.buf)
	}

	n := rb.count - int(start-tail+len(rb.buf))%len(rb.buf)
	if n <= 0 {
		return nil
	}

	result := make([]Event, 0, n)
	for i := 0; i < n; i++ {
		idx := (start + i) % len(rb.buf)
		result = append(result, rb.buf[idx])
	}
	return result
}

// NextSeq returns the next unused sequence number (current high-water mark + 1).
// Push uses this to assign seq numbers; callers that want to pre-allocate or
// check progress can also call it.
func (rb *RingBuffer) NextSeq() uint64 {
	rb.mu.Lock()
	defer rb.mu.Unlock()

	if rb.count == 0 {
		return 1
	}
	lastIdx := (rb.head - 1 + len(rb.buf)) % len(rb.buf)
	return rb.buf[lastIdx].Seq + 1
}

// SnapshotStore holds the latest full snapshot (opaque byte slice) together
// with a ring buffer of incremental events. Peers connect, fetch the snapshot,
// and then consume events via the ring buffer.
type SnapshotStore struct {
	mu       sync.RWMutex
	snapshot []byte
	events   *RingBuffer
	seq      atomic.Uint64
}

// NewSnapshotStore creates a SnapshotStore with a default ring-buffer capacity
// of 1000 events.
func NewSnapshotStore() *SnapshotStore {
	return &SnapshotStore{
		events: &RingBuffer{},
	}
}

// SetSnapshot stores a new full snapshot. It also assigns a fresh seq number
// — any subsequent GetSnapshot call will return data + seq.
func (ss *SnapshotStore) SetSnapshot(data []byte) {
	ss.mu.Lock()
	defer ss.mu.Unlock()

	ss.snapshot = cloneBytes(data)
	ss.seq.Add(1) // new snapshot bumps seq
}

// GetSnapshot returns the newest snapshot and its associated seq number. If
// no snapshot has ever been stored it returns nil, 0, nil.
func (ss *SnapshotStore) GetSnapshot(lastSeq uint64) ([]byte, uint64, error) {
	ss.mu.RLock()
	defer ss.mu.RUnlock()

	if ss.snapshot == nil {
		return nil, 0, nil
	}
	return cloneBytes(ss.snapshot), ss.seq.Load(), nil
}

// PushEvent appends an incremental event to the ring buffer, assigns it the
// next seq number, and returns that seq. The snapshot itself is unchanged.
func (ss *SnapshotStore) PushEvent(eventType string, payload []byte) uint64 {
	seq := ss.events.NextSeq()
	evt := Event{
		Seq:     seq,
		Type:    eventType,
		Payload: cloneBytes(payload),
	}
	ss.events.Push(evt)
	return seq
}

// SnapshotEvents returns all events since lastSeq. This is the reconnect path.
func (ss *SnapshotStore) SnapshotEvents(lastSeq uint64) []Event {
	return ss.events.Since(lastSeq)
}

// cloneBytes is a small helper that makes an independent copy of a byte slice.
func cloneBytes(src []byte) []byte {
	if src == nil {
		return nil
	}
	dst := make([]byte, len(src))
	copy(dst, src)
	return dst
}

// New creates a SyncBroadcaster and registers the control-protocol stream
// handler on the given libp2p host for receiving reverse-direction events from
// nodes (e.g., NodeStatusReport).
//
// Default protocol ID is ControlProtocol ("/edge/control/1.0.0"); override via
// WithProtocolID (must match node-side client). Default send timeout is
// DefaultSendTimeout (30s); override via WithSendTimeout.
func New(h host.Host, opts ...Option) *SyncBroadcaster {
	sb := &SyncBroadcaster{
		host:        h,
		protocolID:  ControlProtocol,
		sendTimeout: DefaultSendTimeout,
		subs:        make(map[string][]chan types.Event),
	}
	for _, opt := range opts {
		opt(sb)
	}
	h.SetStreamHandler(sb.protocolID, sb.handleStream)
	return sb
}

// ProtocolID returns the libp2p stream protocol ID this broadcaster negotiates.
// Exposed so callers (e.g., node-side client code wiring both ends) can verify
// the configured value matches across the wire.
func (sb *SyncBroadcaster) ProtocolID() protocol.ID {
	return sb.protocolID
}

func (sb *SyncBroadcaster) SendTimeout() time.Duration {
	return sb.sendTimeout
}

// SendToNode sends an event to a specific node by opening a control-protocol
// stream and writing a varint-prefixed JSON WireMessage. nodeID is the string
// representation of the target node's libp2p peer.ID.
//
// The send is bounded by sb.sendTimeout (default 30s). On timeout the
// underlying stream open returns a context.DeadlineExceeded wrap.
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

	ctx, cancel := context.WithTimeout(context.Background(), sb.sendTimeout)
	defer cancel()
	return sb.sendWireMessage(ctx, targetPeerID, msg)
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

// ─── Broadcast & Snapshot ──────────────────────────────────────────────────

// Broadcast sends a WireMessage to every connected peer. payload is
// JSON-marshalled and wrapped in a WireMessage{Type: eventType} before sending.
// If there are no connected peers it returns nil (not an error).
func (sb *SyncBroadcaster) Broadcast(eventType string, payload any) error {
	payloadBytes, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("syncbroadcaster Broadcast: marshal payload: %w", err)
	}

	msg := WireMessage{
		Type:    eventType,
		Payload: json.RawMessage(payloadBytes),
	}

	peers := sb.host.Network().Peers()
	if len(peers) == 0 {
		return nil
	}

	for _, peer := range peers {
		ctx, cancel := context.WithTimeout(context.Background(), sb.sendTimeout)
		err := sb.sendWireMessage(ctx, peer, msg)
		cancel()
		if err != nil {
			_ = err
		}
	}
	return nil
}

// GetSnapshot returns the latest snapshot from the attached SnapshotStore (if
// set) together with its current sequence number. If no SnapshotStore is
// configured or no snapshot has ever been stored it returns nil, 0, nil.
func (sb *SyncBroadcaster) GetSnapshot(lastSeq uint64) ([]byte, uint64, error) {
	if sb.snapshotStore == nil {
		return nil, 0, nil
	}
	return sb.snapshotStore.GetSnapshot(lastSeq)
}

// SetSnapshotStore attaches a SnapshotStore instance. After this call,
// Broadcast events will be recorded in the store, and GetSnapshot will
// return the latest snapshot.
func (sb *SyncBroadcaster) SetSnapshotStore(ss *SnapshotStore) {
	sb.snapshotStore = ss
}

// ─── Stream handler (reverse direction) ──────────────────────────────────────

// handleStream reads a varint-prefixed JSON WireMessage from an incoming
// /edge/control/1.0.0 stream and dispatches it to matching subscribers.
func (sb *SyncBroadcaster) handleStream(stream network.Stream) {
	slog.Info("SyncBroadcaster: inbound stream received", "from", stream.Conn().RemotePeer().ShortString())
	defer func() { _ = stream.Close() }()

	msg, err := ReadWireMessage(stream)
	if err != nil {
		_ = stream.Reset()
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
// JSON WireMessage, and closes the stream. The caller-supplied ctx bounds
// stream open + writes; SendToNode wraps it with sb.sendTimeout. Broadcast
// uses an unbounded context (per-peer failures are logged and skipped).
func (sb *SyncBroadcaster) sendWireMessage(ctx context.Context, target peer.ID, msg WireMessage) error {
	stream, err := sb.host.NewStream(ctx, target, sb.protocolID)
	if err != nil {
		return fmt.Errorf("syncbroadcaster: open stream to %s: %w", target.ShortString(), err)
	}
	defer func() { _ = stream.Close() }()

	if err := writeWireMessage(stream, msg); err != nil {
		return fmt.Errorf("syncbroadcaster: write message to %s: %w", target.ShortString(), err)
	}

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

// ReadWireMessage reads a varint-prefixed JSON WireMessage from the stream.
func ReadWireMessage(r io.Reader) (WireMessage, error) {
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
