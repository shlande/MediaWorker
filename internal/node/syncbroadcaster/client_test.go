package syncbroadcaster

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"io"
	"sync"
	"testing"
	"time"

	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/peer"

	"github.com/shlande/mediaworker/internal/shared/identity"
	"github.com/shlande/mediaworker/internal/node/libp2phost"
	"github.com/shlande/mediaworker/internal/types"
)

func genTestIdentity(t *testing.T) *identity.NodeIdentity {
	t.Helper()
	keyFile := t.TempDir() + "/ed25519.key"
	id, err := identity.LoadOrGenerateIdentity(keyFile)
	if err != nil {
		t.Fatalf("gen identity: %v", err)
	}
	return id
}

func spawnTwoHosts(t *testing.T) (cpHost host.Host, nodeHost host.Host, nodePeer peer.ID, cleanup func()) {
	t.Helper()

	cpID := genTestIdentity(t)
	cpH, err := libp2phost.NewEdgeHost(cpID, []string{"/ip4/127.0.0.1/tcp/0"}, nil, nil)
	if err != nil {
		t.Fatalf("cp host: %v", err)
	}

	nodeID := genTestIdentity(t)
	nodeH, err := libp2phost.NewEdgeHost(nodeID, []string{"/ip4/127.0.0.1/tcp/0"}, nil, nil)
	if err != nil {
		cpH.Close()
		t.Fatalf("node host: %v", err)
	}

	cpH.Peerstore().AddAddrs(nodeH.ID(), nodeH.Addrs(), time.Hour)
	nodeH.Peerstore().AddAddrs(cpH.ID(), cpH.Addrs(), time.Hour)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := cpH.Connect(ctx, peer.AddrInfo{ID: nodeH.ID(), Addrs: nodeH.Addrs()}); err != nil {
		cpH.Close()
		nodeH.Close()
		t.Fatalf("connect: %v", err)
	}

	cleanup = func() {
		cpH.Close()
		nodeH.Close()
	}
	return cpH, nodeH, nodeH.ID(), cleanup
}

func writeWireMessage(w io.Writer, msg WireMessage) error {
	data, err := json.Marshal(msg)
	if err != nil {
		return err
	}
	var lenBuf [binary.MaxVarintLen64]byte
	n := binary.PutUvarint(lenBuf[:], uint64(len(data)))
	if _, err := w.Write(lenBuf[:n]); err != nil {
		return err
	}
	_, err = w.Write(data)
	return err
}

func cpSendEvent(t *testing.T, cpHost host.Host, nodePeer peer.ID, eventType string, payload any) {
	t.Helper()
	payloadBytes, err := json.Marshal(payload)
	if err != nil {
		t.Fatalf("marshal payload: %v", err)
	}
	msg := WireMessage{
		Type:    eventType,
		Payload: payloadBytes,
	}
	stream, err := cpHost.NewStream(context.Background(), nodePeer, ControlProtocol)
	if err != nil {
		t.Fatalf("open stream: %v", err)
	}
	defer stream.Close()
	if err := writeWireMessage(stream, msg); err != nil {
		t.Fatalf("write wire message: %v", err)
	}
	if c, ok := stream.(interface{ CloseWrite() error }); ok {
		_ = c.CloseWrite()
	}
}

func TestOnEvent_CredentialUpdate(t *testing.T) {
	cpHost, nodeHost, nodePeer, cleanup := spawnTwoHosts(t)
	defer cleanup()

	eventCh := make(chan types.Event, 1)
	_ = NewClient(nodeHost, nil, func(evt types.Event) {
		eventCh <- evt
	})

	time.Sleep(100 * time.Millisecond)

	payload := map[string]string{"vendor": "115", "account_id": "acct_03"}
	cpSendEvent(t, cpHost, nodePeer, types.EventCredentialUpdate, payload)

	select {
	case evt := <-eventCh:
		if evt.Type != types.EventCredentialUpdate {
			t.Fatalf("expected type %q, got %q", types.EventCredentialUpdate, evt.Type)
		}
		if len(evt.Payload) == 0 {
			t.Fatal("expected non-empty payload")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for CREDENTIAL_UPDATE event")
	}
}

func TestOnEvent_Ban(t *testing.T) {
	cpHost, nodeHost, nodePeer, cleanup := spawnTwoHosts(t)
	defer cleanup()

	eventCh := make(chan types.Event, 1)
	_ = NewClient(nodeHost, nil, func(evt types.Event) {
		eventCh <- evt
	})

	time.Sleep(100 * time.Millisecond)

	cpSendEvent(t, cpHost, nodePeer, types.EventBan, map[string]string{"key": "115:acct_03"})

	select {
	case evt := <-eventCh:
		if evt.Type != types.EventBan {
			t.Fatalf("expected type %q, got %q", types.EventBan, evt.Type)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for BAN event")
	}
}

func TestOnEvent_PinPlanUpdate_StillWorksViaOnPlan(t *testing.T) {
	cpHost, nodeHost, nodePeer, cleanup := spawnTwoHosts(t)
	defer cleanup()

	planCh := make(chan types.PinPlan, 1)
	_ = NewClient(nodeHost, func(plan types.PinPlan) {
		planCh <- plan
	}, nil)

	time.Sleep(100 * time.Millisecond)

	plan := types.PinPlan{
		Seq:        99,
		TargetNode: "node-B",
		Updates: []types.PinUpdate{{
			PinBlobs:   []string{"blob1"},
			UnpinBlobs: nil,
		}},
	}
	cpSendEvent(t, cpHost, nodePeer, "PIN_PLAN_UPDATE", plan)

	select {
	case got := <-planCh:
		if got.Seq != 99 {
			t.Fatalf("expected Seq=99, got %d", got.Seq)
		}
		if got.TargetNode != "node-B" {
			t.Fatalf("expected TargetNode=node-B, got %s", got.TargetNode)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for PinPlan via onPlan")
	}
}

func TestOnEvent_UnknownEvent_NoPanicWithNilOnEvent(t *testing.T) {
	cpHost, nodeHost, nodePeer, cleanup := spawnTwoHosts(t)
	defer cleanup()

	_ = NewClient(nodeHost, nil, nil)
	time.Sleep(100 * time.Millisecond)

	cpSendEvent(t, cpHost, nodePeer, "SOME_UNKNOWN_EVENT", "some data")
}

func TestOnEvent_UnknownEvent_DeliveredToOnEvent(t *testing.T) {
	cpHost, nodeHost, nodePeer, cleanup := spawnTwoHosts(t)
	defer cleanup()

	var mu sync.Mutex
	var received *types.Event
	_ = NewClient(nodeHost, nil, func(evt types.Event) {
		mu.Lock()
		received = &evt
		mu.Unlock()
	})

	time.Sleep(100 * time.Millisecond)

	cpSendEvent(t, cpHost, nodePeer, "UNRECOGNIZED_EVENT", map[string]string{"foo": "bar"})

	time.Sleep(500 * time.Millisecond)

	mu.Lock()
	if received == nil {
		mu.Unlock()
		t.Fatal("expected unknown event to be delivered to onEvent, got nil")
	}
	if received.Type != "UNRECOGNIZED_EVENT" {
		t.Fatalf("expected type UNRECOGNIZED_EVENT, got %q", received.Type)
	}
	mu.Unlock()
}