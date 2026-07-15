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

// Client is the node-side handler for the /edge/control/1.0.0 stream protocol.
// It registers a stream handler that receives PinPlan events and provides a
// SendToControlPlane method for pushing NodeStatusReport back to the CPS.
type Client struct {
	host    host.Host
	onPlan  OnPinPlan
}

// NewClient creates a node-side control-channel client and registers the
// /edge/control/1.0.0 stream handler on the given host.
func NewClient(h host.Host, onPlan OnPinPlan) *Client {
	c := &Client{
		host:   h,
		onPlan: onPlan,
	}
	h.SetStreamHandler(ControlProtocol, c.handleStream)
	return c
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

	stream, err := c.host.NewStream(ctx, targetCP, ControlProtocol)
	if err != nil {
		return fmt.Errorf("node/syncbroadcaster: open stream to CP %s: %w", targetCP.ShortString(), err)
	}
	defer stream.Close()

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
	defer stream.Close()

	br := bufio.NewReader(stream)

	length, err := binary.ReadUvarint(br)
	if err != nil {
		stream.Reset()
		return
	}

	data := make([]byte, length)
	if _, err := io.ReadFull(br, data); err != nil {
		stream.Reset()
		return
	}

	var msg WireMessage
	if err := json.Unmarshal(data, &msg); err != nil {
		stream.Reset()
		return
	}

	if msg.Type == "PIN_PLAN_UPDATE" && c.onPlan != nil {
		var plan types.PinPlan
		if err := json.Unmarshal(msg.Payload, &plan); err != nil {
			stream.Reset()
			return
		}
		c.onPlan(plan)
	}
}
