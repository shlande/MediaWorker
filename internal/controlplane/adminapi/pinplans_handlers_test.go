package adminapi

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/shlande/mediaworker/internal/controlplane/noderegistry"
	"github.com/shlande/mediaworker/internal/controlplane/pinstrategy"
	"github.com/shlande/mediaworker/internal/types"
)

func TestPinPlans_AckAndOffline(t *testing.T) {
	// Given: 2 records — one targeted at a node in registry (acked), one
	// targeted at an offline/unknown node (pending).
	t1 := time.Date(2026, 1, 15, 12, 0, 0, 0, time.UTC)
	dl := pinstrategy.NewDispatchLog()
	dl.Add(pinstrategy.DispatchRecord{
		Seq:        1,
		TargetNode: "node-online",
		ContentID:  "content-1",
		Pins:       3,
		Trigger:    pinstrategy.TriggerAuto,
		SentAt:     t1,
	})
	dl.Add(pinstrategy.DispatchRecord{
		Seq:        2,
		TargetNode: "node-offline",
		ContentID:  "content-2",
		Pins:       2,
		Unpins:     1,
		Trigger:    pinstrategy.TriggerManual,
		SentAt:     t1,
	})

	reg := noderegistry.NewRegistry()
	reg.UpsertReport(types.NodeStatusReport{
		PeerID: "peer-1",
		NodeID: "node-online",
	})

	secret := []byte("test-secret-key-for-admin-tokens")
	srv := NewServer(secret)
	RegisterPinPlansRoutes(srv, dl, reg)
	ts := httptest.NewServer(srv.mux)
	t.Cleanup(ts.Close)
	token := signAdminToken(t, secret)

	req, _ := http.NewRequest(http.MethodGet, ts.URL+"/v1/admin/pin-plans", nil)
	req.Header.Set("Authorization", "Bearer "+token)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("GET: %v", err)
	}
	defer resp.Body.Close()

	var items []PinPlanItem
	if err := json.NewDecoder(resp.Body).Decode(&items); err != nil {
		t.Fatalf("decode: %v", err)
	}

	if len(items) != 2 {
		t.Fatalf("len(items) = %d, want 2", len(items))
	}

	byNode := map[string]PinPlanItem{}
	for _, it := range items {
		byNode[it.TargetNode] = it
	}

	if it, ok := byNode["node-online"]; !ok {
		t.Error("node-online missing from response")
	} else if it.AckState != ackStateAcked {
		t.Errorf("node-online ack_state = %q, want acked (sent %s ≤ received now)", it.AckState, t1)
	}

	if it, ok := byNode["node-offline"]; !ok {
		t.Error("node-offline missing from response")
	} else if it.AckState != ackStatePending {
		t.Errorf("node-offline ack_state = %q, want pending (not in registry)", it.AckState)
	}
}

func TestPinPlans_SentAtBoundaryAcked(t *testing.T) {
	t1 := time.Date(2026, 1, 15, 12, 0, 0, 0, time.UTC)
	dl := pinstrategy.NewDispatchLog()
	dl.Add(pinstrategy.DispatchRecord{
		Seq:        1,
		TargetNode: "node-X",
		ContentID:  "c1",
		Pins:       1,
		Trigger:    pinstrategy.TriggerAuto,
		SentAt:     t1,
	})

	reg := noderegistry.NewRegistry()
	reg.UpsertReport(types.NodeStatusReport{
		PeerID: "peer-1",
		NodeID: "node-X",
	})

	secret := []byte("test-secret-key-for-admin-tokens")
	srv := NewServer(secret)
	RegisterPinPlansRoutes(srv, dl, reg)
	ts := httptest.NewServer(srv.mux)
	t.Cleanup(ts.Close)
	token := signAdminToken(t, secret)

	req, _ := http.NewRequest(http.MethodGet, ts.URL+"/v1/admin/pin-plans", nil)
	req.Header.Set("Authorization", "Bearer "+token)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("GET: %v", err)
	}
	defer resp.Body.Close()

	var items []PinPlanItem
	json.NewDecoder(resp.Body).Decode(&items)

	if len(items) != 1 {
		t.Fatalf("len(items) = %d, want 1", len(items))
	}
	if items[0].AckState != ackStateAcked {
		t.Errorf("ack_state = %q, want acked (SentAt ≤ ReceivedAt, boundary included)", items[0].AckState)
	}
}

func TestPinPlans_Pagination(t *testing.T) {
	// Given: 5 records, page_size=2.
	dl := pinstrategy.NewDispatchLog()
	for i := range 5 {
		dl.Add(pinstrategy.DispatchRecord{
			Seq:        uint64(i + 1),
			TargetNode: "node-X",
			ContentID:  "c",
			Pins:       1,
			Trigger:    pinstrategy.TriggerAuto,
			SentAt:     time.Date(2026, 1, 15, 12, 0, i, 0, time.UTC),
		})
	}

	reg := noderegistry.NewRegistry()
	reg.UpsertReport(types.NodeStatusReport{
		PeerID: "peer-1",
		NodeID: "node-X",
	})

	secret := []byte("test-secret-key-for-admin-tokens")
	srv := NewServer(secret)
	RegisterPinPlansRoutes(srv, dl, reg)
	ts := httptest.NewServer(srv.mux)
	t.Cleanup(ts.Close)
	token := signAdminToken(t, secret)

	req, _ := http.NewRequest(http.MethodGet, ts.URL+"/v1/admin/pin-plans?page=2&page_size=2", nil)
	req.Header.Set("Authorization", "Bearer "+token)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("GET page 2: %v", err)
	}
	defer resp.Body.Close()

	var items []PinPlanItem
	json.NewDecoder(resp.Body).Decode(&items)

	if len(items) != 2 {
		t.Fatalf("page 2 len = %d, want 2", len(items))
	}
	if items[0].Seq != 3 || items[1].Seq != 2 {
		t.Errorf("page 2 seqs = [%d, %d], want [3, 2]", items[0].Seq, items[1].Seq)
	}

	// Page 3: last page, 1 item.
	req, _ = http.NewRequest(http.MethodGet, ts.URL+"/v1/admin/pin-plans?page=3&page_size=2", nil)
	req.Header.Set("Authorization", "Bearer "+token)
	resp, err = http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("GET page 3: %v", err)
	}
	defer resp.Body.Close()
	json.NewDecoder(resp.Body).Decode(&items)
	if len(items) != 1 || items[0].Seq != 1 {
		t.Errorf("page 3 = {len:%d, seq:%d}, want {1, 1}", len(items), items[0].Seq)
	}

	// Page 4: empty.
	req, _ = http.NewRequest(http.MethodGet, ts.URL+"/v1/admin/pin-plans?page=4&page_size=2", nil)
	req.Header.Set("Authorization", "Bearer "+token)
	resp, err = http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("GET page 4: %v", err)
	}
	defer resp.Body.Close()
	json.NewDecoder(resp.Body).Decode(&items)
	if len(items) != 0 {
		t.Errorf("page 4 len = %d, want 0", len(items))
	}
}

func TestPinPlans_Empty(t *testing.T) {
	dl := pinstrategy.NewDispatchLog()
	reg := noderegistry.NewRegistry()

	secret := []byte("test-secret-key-for-admin-tokens")
	srv := NewServer(secret)
	RegisterPinPlansRoutes(srv, dl, reg)
	ts := httptest.NewServer(srv.mux)
	t.Cleanup(ts.Close)
	token := signAdminToken(t, secret)

	req, _ := http.NewRequest(http.MethodGet, ts.URL+"/v1/admin/pin-plans", nil)
	req.Header.Set("Authorization", "Bearer "+token)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("GET: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Errorf("status = %d, want 200", resp.StatusCode)
	}
	var items []PinPlanItem
	json.NewDecoder(resp.Body).Decode(&items)
	if len(items) != 0 {
		t.Errorf("len(items) = %d, want 0", len(items))
	}
}

func TestPinPlans_NoToken_401(t *testing.T) {
	dl := pinstrategy.NewDispatchLog()
	reg := noderegistry.NewRegistry()

	secret := []byte("test-secret-key-for-admin-tokens")
	srv := NewServer(secret)
	RegisterPinPlansRoutes(srv, dl, reg)
	ts := httptest.NewServer(srv.mux)
	t.Cleanup(ts.Close)

	req, _ := http.NewRequest(http.MethodGet, ts.URL+"/v1/admin/pin-plans", nil)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("GET: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusUnauthorized {
		t.Errorf("status = %d, want 401", resp.StatusCode)
	}
}

func TestPinPlans_ResponseShape(t *testing.T) {
	t1 := time.Date(2026, 1, 15, 12, 0, 0, 0, time.UTC)
	dl := pinstrategy.NewDispatchLog()
	dl.Add(pinstrategy.DispatchRecord{
		Seq:        42,
		TargetNode: "node-abc",
		ContentID:  "content-xyz",
		Pins:       5,
		Unpins:     3,
		Trigger:    pinstrategy.TriggerManual,
		SentAt:     t1,
	})

	reg := noderegistry.NewRegistry()
	reg.UpsertReport(types.NodeStatusReport{
		PeerID: "peer-abc",
		NodeID: "node-abc",
	})

	secret := []byte("test-secret-key-for-admin-tokens")
	srv := NewServer(secret)
	RegisterPinPlansRoutes(srv, dl, reg)
	ts := httptest.NewServer(srv.mux)
	t.Cleanup(ts.Close)
	token := signAdminToken(t, secret)

	req, _ := http.NewRequest(http.MethodGet, ts.URL+"/v1/admin/pin-plans", nil)
	req.Header.Set("Authorization", "Bearer "+token)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("GET: %v", err)
	}
	defer resp.Body.Close()

	var items []PinPlanItem
	json.NewDecoder(resp.Body).Decode(&items)

	if len(items) != 1 {
		t.Fatalf("len(items) = %d, want 1", len(items))
	}
	it := items[0]
	if it.Seq != 42 {
		t.Errorf("seq = %d, want 42", it.Seq)
	}
	if it.TargetNode != "node-abc" {
		t.Errorf("target_node = %q, want node-abc", it.TargetNode)
	}
	if it.ContentID != "content-xyz" {
		t.Errorf("content_id = %q, want content-xyz", it.ContentID)
	}
	if it.Pins != 5 {
		t.Errorf("pins = %d, want 5", it.Pins)
	}
	if it.Unpins != 3 {
		t.Errorf("unpins = %d, want 3", it.Unpins)
	}
	if it.Trigger != pinstrategy.TriggerManual {
		t.Errorf("trigger = %q, want manual", it.Trigger)
	}
	if !it.SentAt.Equal(t1) {
		t.Errorf("sent_at = %v, want %v", it.SentAt, t1)
	}
	if it.AckState != ackStateAcked {
		t.Errorf("ack_state = %q, want acked", it.AckState)
	}
}
