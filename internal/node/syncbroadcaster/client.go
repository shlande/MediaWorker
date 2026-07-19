// Package syncbroadcaster provides the node-side client for the /edge/control/1.0.0
// libp2p stream protocol. It receives PinPlan events from the control plane and
// sends NodeStatusReport events back (reverse direction).
package syncbroadcaster

import (
	"bufio"
	"context"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"

	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/core/protocol"

	"github.com/shlande/mediaworker/internal/types"
)

// ControlProtocol is the libp2p stream protocol ID for the control-plane ↔
// node sync channel. This is the default; operators that override the
// control-plane side via WithProtocolID must override the node side to the
// same value — otherwise no streams can be negotiated (plan line 239).
const ControlProtocol = protocol.ID("/edge/control/1.0.0")

// WireMessage is the JSON envelope shared between the control-plane broadcaster
// and node-side client. Wire format: varint-prefixed JSON body.
type WireMessage struct {
	Type    string          `json:"type"`
	Payload json.RawMessage `json:"payload"`
}

// OnPinPlan is the callback invoked when a PinPlan is received from the control
// plane. The node-side handler decodes the payload into types.PinPlan.
type OnPinPlan func(plan types.PinPlan)

// OnEvent is the callback invoked when a generic event (not a PinPlan) is
// received from the control plane. The payload is the raw WireMessage payload.
type OnEvent func(event types.Event)

// Client is the node-side handler for the /edge/control/1.0.0 stream protocol.
// It registers a stream handler that receives PinPlan events and provides a
// SendToControlPlane method for pushing NodeStatusReport back to the CPS.
type Client struct {
	host       host.Host
	protocolID protocol.ID
	onPlan     OnPinPlan
	onEvent    OnEvent
}

// Option configures a Client at construction time.
type Option func(*Client)

// WithProtocolID overrides the default libp2p stream protocol ID. An empty
// string is ignored (default preserved). Must match the control-plane side's
// configured protocol ID — otherwise no streams can be negotiated (plan line 239).
func WithProtocolID(id string) Option {
	return func(c *Client) {
		if id != "" {
			c.protocolID = protocol.ID(id)
		}
	}
}

// NewClient creates a node-side control-channel client and registers the
// /edge/control/1.0.0 stream handler on the given host.
// onPlan is called for PIN_PLAN_UPDATE events; onEvent is called for all
// other recognized events. Either may be nil to skip that dispatch.
func NewClient(h host.Host, onPlan OnPinPlan, onEvent OnEvent, opts ...Option) *Client {
	c := &Client{
		host:       h,
		protocolID: ControlProtocol,
		onPlan:     onPlan,
		onEvent:    onEvent,
	}
	for _, opt := range opts {
		opt(c)
	}
	h.SetStreamHandler(c.protocolID, c.handleStream)
	return c
}

// ProtocolID returns the libp2p stream protocol ID this client negotiates.
// Exposed for cross-side verification (CP-side broadcaster must match).
func (c *Client) ProtocolID() protocol.ID {
	return c.protocolID
}

// SendToControlPlane sends an event (e.g., NodeStatusReport) to the control
// plane by opening a stream to targetCP, writing a varint-prefixed WireMessage,
// and closing the write side.
func (c *Client) SendToControlPlane(ctx context.Context, targetCP peer.ID, eventType string, payload any) error {
	payloadBytes, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("node/syncbroadcaster: marshal %s payload: %w", eventType, err)
	}

	msg := WireMessage{
		Type:    eventType,
		Payload: payloadBytes,
	}

	stream, err := c.host.NewStream(ctx, targetCP, c.protocolID)
	if err != nil {
		return fmt.Errorf("node/syncbroadcaster: open stream to CP %s: %w", targetCP.ShortString(), err)
	}
	defer func() { _ = stream.Close() }()

	data, err := json.Marshal(msg)
	if err != nil {
		return fmt.Errorf("node/syncbroadcaster: marshal wire message: %w", err)
	}

	var lenBuf [binary.MaxVarintLen64]byte
	n := binary.PutUvarint(lenBuf[:], uint64(len(data)))
	if _, err := stream.Write(lenBuf[:n]); err != nil {
		return fmt.Errorf("node/syncbroadcaster: write length: %w", err)
	}
	if _, err := stream.Write(data); err != nil {
		return fmt.Errorf("node/syncbroadcaster: write body: %w", err)
	}

	if cw, ok := stream.(interface{ CloseWrite() error }); ok {
		_ = cw.CloseWrite()
	}

	return nil
}

func (c *Client) handleStream(stream network.Stream) {
	defer func() { _ = stream.Close() }()

	br := bufio.NewReader(stream)

	length, err := binary.ReadUvarint(br)
	if err != nil {
		_ = stream.Reset()
		return
	}

	data := make([]byte, length)
	if _, err := io.ReadFull(br, data); err != nil {
		_ = stream.Reset()
		return
	}

	var msg WireMessage
	if err := json.Unmarshal(data, &msg); err != nil {
		_ = stream.Reset()
		return
	}

	if msg.Type == "PIN_PLAN_UPDATE" && c.onPlan != nil {
		var plan types.PinPlan
		if err := json.Unmarshal(msg.Payload, &plan); err != nil {
			_ = stream.Reset()
			return
		}
		c.onPlan(plan)
		return
	}

	if c.onEvent != nil {
		c.onEvent(types.Event{
			Type:    msg.Type,
			Payload: []byte(msg.Payload),
		})
	}
}
