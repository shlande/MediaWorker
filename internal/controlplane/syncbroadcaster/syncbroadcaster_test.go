package syncbroadcaster_test

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/peer"

	"github.com/shlande/mediaworker/internal/shared/identity"

	sb "github.com/shlande/mediaworker/internal/controlplane/syncbroadcaster"
	"github.com/shlande/mediaworker/internal/node/libp2phost"
	sbnode "github.com/shlande/mediaworker/internal/node/syncbroadcaster"
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

func TestForwardChannel_PinPlanUpdate(t *testing.T) {
	cpHost, nodeHost, nodePeer, cleanup := spawnTwoHosts(t)
	defer cleanup()

	received := make(chan types.PinPlan, 1)

	_ = sbnode.NewClient(nodeHost, func(plan types.PinPlan) {
		received <- plan
	})

	broadcaster := sb.New(cpHost)

	plan := types.PinPlan{
		Seq:        42,
		TargetNode: "node-A",
		Updates: []types.PinUpdate{{
			BlobHash:   "blob-hash-1",
			PinBlobs:   []string{"blob1", "blob2"},
			UnpinBlobs: []string{"blob3"},
		}},
	}

	err := broadcaster.SendToNode(nodePeer.String(), "PIN_PLAN_UPDATE", plan)
	if err != nil {
		t.Fatalf("SendToNode: %v", err)
	}

	select {
	case got := <-received:
		if got.Seq != 42 {
			t.Fatalf("expected Seq=42, got %d", got.Seq)
		}
		if got.TargetNode != "node-A" {
			t.Fatalf("expected TargetNode=node-A, got %s", got.TargetNode)
		}
		if len(got.Updates) != 1 {
			t.Fatalf("expected 1 update, got %d", len(got.Updates))
		}
		if got.Updates[0].PinBlobs[0] != "blob1" {
			t.Fatalf("expected blob1, got %s", got.Updates[0].PinBlobs[0])
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for PinPlan")
	}
}

func TestReverseChannel_NodeStatusReport(t *testing.T) {
	cpHost, nodeHost, _, cleanup := spawnTwoHosts(t)
	defer cleanup()

	broadcaster := sb.New(cpHost)
	subCh := broadcaster.Subscribe("NODE_STATUS_REPORT")

	nodeClient := sbnode.NewClient(nodeHost, nil)

	report := types.NodeStatusReport{
		NodeID:  "node-X",
		PeerID:  "peer-id-x",
		Healthy: true,
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	err := nodeClient.SendToControlPlane(ctx, cpHost.ID(), "NODE_STATUS_REPORT", report)
	if err != nil {
		t.Fatalf("SendToControlPlane: %v", err)
	}

	select {
	case evt := <-subCh:
		if evt.Type != "NODE_STATUS_REPORT" {
			t.Fatalf("expected NODE_STATUS_REPORT, got %s", evt.Type)
		}
		var got types.NodeStatusReport
		if err := json.Unmarshal(evt.Payload, &got); err != nil {
			t.Fatalf("unmarshal: %v", err)
		}
		if got.NodeID != "node-X" {
			t.Fatalf("expected node-X, got %s", got.NodeID)
		}
		if !got.Healthy {
			t.Fatal("expected healthy=true")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for NodeStatusReport on subscribe channel")
	}
}

func TestSendToNode_NodeOffline(t *testing.T) {
	cpHost, _, _, cleanup := spawnTwoHosts(t)
	defer cleanup()

	broadcaster := sb.New(cpHost)

	err := broadcaster.SendToNode("12D3KooWDeadBeeFDe4dB33FdE4dB33FDe4dB33F", "PIN_PLAN_UPDATE", nil)
	if err == nil {
		t.Fatal("expected error for offline node, got nil")
	}
	t.Logf("offline node error (expected): %v", err)
}
