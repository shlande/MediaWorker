package types

import (
	"encoding/json"
	"reflect"
	"strings"
	"testing"
	"time"
)

// roundtrip marshals v to JSON, unmarshals into a new T, and asserts equality.
func roundtrip[T any](t *testing.T, v T) {
	t.Helper()
	data, err := json.Marshal(v)
	if err != nil {
		t.Fatalf("json.Marshal(%T): %v", v, err)
	}
	var got T
	if err := json.Unmarshal(data, &got); err != nil {
		t.Fatalf("json.Unmarshal(%T): %v", got, err)
	}
	if !reflect.DeepEqual(v, got) {
		t.Fatalf("roundtrip mismatch:\n  want %#v\n  got  %#v", v, got)
	}
}

func TestBlobDescriptor_roundtrip(t *testing.T) {
	roundtrip(t, BlobDescriptor{
		BlobHash: "abc123def456",
		BlobType: "media",
		Size:     1048576,
	})
}

func TestBlobRole_roundtrip(t *testing.T) {
	roundtrip(t, BlobRole{
		BlobHash:  "abc123def456",
		Role:      "media",
		SortOrder: 2,
		BusinessMeta: map[string]any{
			"representation_id": "720p",
			"bitrate":           float64(1500000),
		},
	})
}

func TestBlobRole_omit_business_meta(t *testing.T) {
	roundtrip(t, BlobRole{
		BlobHash:  "blob_no_meta",
		Role:      "original",
		SortOrder: 0,
	})
}

func TestContentMeta_roundtrip(t *testing.T) {
	roundtrip(t, ContentMeta{
		ContentID:    "cont_001",
		ContentType:  "dash_video",
		TypeMetadata: []byte(`{"width":1920,"height":1080}`),
	})
}

func TestContentMeta_withTitleAndDeletedAt_roundtrip(t *testing.T) {
	deletedAt := time.Date(2026, 7, 20, 12, 0, 0, 0, time.UTC)
	roundtrip(t, ContentMeta{
		ContentID:    "cont_001b",
		ContentType:  "dash_video",
		TypeMetadata: []byte(`{"width":1920,"height":1080}`),
		Title:        "赛博朋克实况",
		DeletedAt:    &deletedAt,
	})
}

// Given a live ContentMeta (no title, not deleted), When marshalled, Then
// title/deleted_at are omitted from the JSON.
func TestContentMeta_omitsEmptyTitleAndNilDeletedAt(t *testing.T) {
	data, err := json.Marshal(ContentMeta{ContentID: "c1", ContentType: "video"})
	if err != nil {
		t.Fatalf("json.Marshal: %v", err)
	}
	if strings.Contains(string(data), "title") || strings.Contains(string(data), "deleted_at") {
		t.Errorf("live ContentMeta must omit title/deleted_at, got %s", data)
	}
}

func TestContentIngestedEvent_roundtrip(t *testing.T) {
	roundtrip(t, ContentIngestedEvent{
		ContentID:   "cont_002",
		ContentType: "image",
		Blobs: []BlobDescriptor{
			{BlobHash: "blob_a", BlobType: "original", Size: 512000},
			{BlobHash: "blob_b", BlobType: "thumbnail", Size: 32000},
		},
		Roles: []BlobRole{
			{BlobHash: "blob_a", Role: "original", SortOrder: 0},
			{BlobHash: "blob_b", Role: "thumbnail", SortOrder: 1},
		},
		Timestamp: 1700000000,
	})
}

func TestNodeSpaceInfo_roundtrip(t *testing.T) {
	roundtrip(t, NodeSpaceInfo{
		NodeID:         "node_01",
		AvailableBytes: 107374182400,
		PinnedCount:    42,
	})
}

func TestNodePinPlan_roundtrip(t *testing.T) {
	roundtrip(t, NodePinPlan{
		NodeID:     "node_01",
		ContentID:  "cont_003",
		PinBlobs:   []string{"blob_a", "blob_b"},
		UnpinBlobs: []string{"blob_c"},
	})
}

func TestPinPlan_roundtrip(t *testing.T) {
	roundtrip(t, PinPlan{
		Seq:        1,
		TargetNode: "node_02",
		Updates: []PinUpdate{
			{PinBlobs: []string{"blob_x"}, UnpinBlobs: nil},
			{PinBlobs: nil, UnpinBlobs: []string{"blob_z"}},
		},
	})
}

func TestPinUpdate_roundtrip(t *testing.T) {
	roundtrip(t, PinUpdate{
		PinBlobs:   []string{"blob_1", "blob_2"},
		UnpinBlobs: []string{"blob_3"},
	})
}

func TestPinSpaceInfo_roundtrip(t *testing.T) {
	roundtrip(t, PinSpaceInfo{
		AvailableBytes:  10737418240,
		PinnedCount:     7,
		TotalPinnedSize: 5368709120,
	})
}

func TestNodeStatusReport_roundtrip(t *testing.T) {
	roundtrip(t, NodeStatusReport{
		NodeID: "node_l4_01",
		PeerID: PeerId("12D3KooWExample"),
		Capabilities: NodeCapabilities{
			Edge:          false,
			L4Backhaul:    true,
			RelayProvider: true,
			PeerICP:       false,
		},
		PrefixSpace: PartitionStatus{
			TotalBytes: 500000000000,
			UsedBytes:  200000000000,
			BlobCount:  1500,
		},
		WarmSpace: PartitionStatus{
			TotalBytes: 100000000000,
			UsedBytes:  30000000000,
			BlobCount:  200,
		},
		Healthy:    true,
		LastUpdate: 1700000100,
	})
}

// Given a NodeStatusReport with all extended fields set, When marshalled,
// Then the new wire keys appear and the report round-trips exactly.
func TestNodeStatusReport_extendedFields(t *testing.T) {
	cold := PartitionStatus{TotalBytes: 900, UsedBytes: 100, BlobCount: 7}
	rep := NodeStatusReport{
		NodeID:       "node_l4_02",
		PeerID:       PeerId("12D3KooWExt"),
		Capabilities: NodeCapabilities{Edge: true, L4Backhaul: true},
		PrefixSpace:  PartitionStatus{TotalBytes: 1, UsedBytes: 2, BlobCount: 3},
		WarmSpace:    PartitionStatus{TotalBytes: 4, UsedBytes: 5, BlobCount: 6},
		Healthy:      true,
		LastUpdate:   1700000200,

		Region:            "cn",
		Version:           "v0.4.0",
		StartedAt:         1700000000,
		ConnCount:         12,
		ColdSpace:         &cold,
		JWTRefreshFail24h: 2,
	}
	data, err := json.Marshal(rep)
	if err != nil {
		t.Fatalf("json.Marshal: %v", err)
	}
	for _, key := range []string{
		`"region":"cn"`, `"version":"v0.4.0"`, `"started_at":1700000000`,
		`"conn_count":12`, `"cold_space":`, `"jwt_refresh_fail_24h":2`,
	} {
		if !strings.Contains(string(data), key) {
			t.Errorf("extended report JSON missing %s: %s", key, data)
		}
	}
	roundtrip(t, rep)
}

// Given a legacy report JSON without the extended fields, When unmarshalled,
// Then decoding succeeds and every new field stays at its zero value (CP
// compatibility with old node reports).
func TestNodeStatusReport_legacyJSON_decodesToZeroValues(t *testing.T) {
	legacy := `{"node_id":"n1","peer_id":"12D3KooWLegacy","capabilities":{"edge":true,"l4_backhaul":false,"relay_provider":false,"peer_icp":false},"prefix_space":{"total_bytes":1,"used_bytes":2,"blob_count":3},"warm_space":{"total_bytes":4,"used_bytes":5,"blob_count":6},"healthy":true,"last_update":1700000100}`
	var rep NodeStatusReport
	if err := json.Unmarshal([]byte(legacy), &rep); err != nil {
		t.Fatalf("legacy report must decode: %v", err)
	}
	if rep.Region != "" || rep.Version != "" || rep.StartedAt != 0 ||
		rep.ConnCount != 0 || rep.ColdSpace != nil || rep.JWTRefreshFail24h != 0 {
		t.Errorf("extended fields must be zero values, got %+v", rep)
	}
	if rep.NodeID != "n1" || !rep.Healthy || rep.LastUpdate != 1700000100 || rep.PrefixSpace.BlobCount != 3 {
		t.Errorf("legacy fields mangled: %+v", rep)
	}
}

func TestPartitionStatus_roundtrip(t *testing.T) {
	roundtrip(t, PartitionStatus{
		TotalBytes: 1000000,
		UsedBytes:  500000,
		BlobCount:  100,
	})
}

func TestPeerId_type(t *testing.T) {
	const raw = "12D3KooWPeerIdExample"
	var pid PeerId = raw
	data, err := json.Marshal(pid)
	if err != nil {
		t.Fatal(err)
	}
	var got PeerId
	if err := json.Unmarshal(data, &got); err != nil {
		t.Fatal(err)
	}
	if got != pid {
		t.Fatalf("PeerId roundtrip: want %q, got %q", pid, got)
	}
}

func TestNodeCapabilities_roundtrip(t *testing.T) {
	roundtrip(t, NodeCapabilities{
		Edge:          true,
		L4Backhaul:    false,
		RelayProvider: true,
		PeerICP:       false,
	})
}

func TestCapabilityJWT_type(t *testing.T) {
	const raw = "eyJhbGciOiJFZERTQSIsInR5cCI6IkpXVCJ9.eyJleHAiOjE3MDAwMDEyMDB9.signature"
	var jwt CapabilityJWT = raw
	data, err := json.Marshal(jwt)
	if err != nil {
		t.Fatal(err)
	}
	var got CapabilityJWT
	if err := json.Unmarshal(data, &got); err != nil {
		t.Fatal(err)
	}
	if got != jwt {
		t.Fatalf("CapabilityJWT roundtrip: want %q, got %q", jwt, got)
	}
}

func TestNodeJWTPayload_roundtrip(t *testing.T) {
	roundtrip(t, NodeJWTPayload{
		NodeID: "node_l4_01",
		PeerID: PeerId("12D3KooWJWTExample"),
		Capabilities: NodeCapabilities{
			Edge:          true,
			L4Backhaul:    false,
			RelayProvider: false,
			PeerICP:       true,
		},
		BandwidthQuota: 104857600,
		Iat:            1700000000,
		Exp:            1700003600,
	})
}

func TestPSK_type(t *testing.T) {
	raw := []byte{0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0a, 0x0b, 0x0c, 0x0d, 0x0e, 0x0f, 0x10, 0x11, 0x12, 0x13, 0x14, 0x15, 0x16, 0x17, 0x18, 0x19, 0x1a, 0x1b, 0x1c, 0x1d, 0x1e, 0x1f, 0x20}
	var psk PSK = raw
	data, err := json.Marshal(psk)
	if err != nil {
		t.Fatal(err)
	}
	var got PSK
	if err := json.Unmarshal(data, &got); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual([]byte(psk), []byte(got)) {
		t.Fatalf("PSK roundtrip: want %x, got %x", []byte(psk), []byte(got))
	}
}

func TestDHTBootstrapPeer_roundtrip(t *testing.T) {
	roundtrip(t, DHTBootstrapPeer{
		PeerID: PeerId("12D3KooWBootstrap"),
		Addrs:  []string{"/ip4/1.2.3.4/tcp/4001/p2p/12D3KooWBootstrap", "/ip4/1.2.3.4/udp/4001/quic-v1/p2p/12D3KooWBootstrap"},
	})
}

func TestJWTRequest_roundtrip(t *testing.T) {
	roundtrip(t, JWTRequest{
		PeerID:       PeerId("12D3KooWJWTReq"),
		SignedPeerID: []byte{0x30, 0x45, 0x02, 0x21, 0x00, 0xde, 0xad, 0xbe, 0xef},
	})
}

func TestJWTResponse_roundtrip(t *testing.T) {
	roundtrip(t, JWTResponse{
		JWT:           CapabilityJWT("eyJhbGciOiJFZERTQSJ9.eyJleHAiOjE3MDAwMDEyMDB9.sig"),
		RefreshBefore: 300,
	})
}

func TestPeerStoreEntry_roundtrip(t *testing.T) {
	roundtrip(t, PeerStoreEntry{
		PeerID: PeerId("12D3KooWStoreEntry"),
		Addrs:  []string{"/ip4/10.0.0.1/tcp/4001"},
		JWT:    CapabilityJWT("eyJhbGciOiJFZERTQSJ9.payload.sig"),
		Capabilities: NodeCapabilities{
			Edge:          true,
			L4Backhaul:    false,
			RelayProvider: false,
			PeerICP:       true,
		},
		JWTExp:   1700003600,
		LastSeen: 1700003000,
		Score:    12.5,
		Stale:    false,
	})
}

func TestBlobLocation_roundtrip(t *testing.T) {
	roundtrip(t, BlobLocation{
		BlobHash:  "blob_loc_001",
		BackendID: "s3:acct_123",
		FileID:    "file_456",
	})
}

func TestEvent_roundtrip(t *testing.T) {
	roundtrip(t, Event{
		Type:    "content_ingested",
		Payload: []byte(`{"content_id":"cont_002"}`),
	})
}

func TestCircuitPayload_roundtrip(t *testing.T) {
	roundtrip(t, CircuitPayload{Vendor: VendorBaidu, AccountID: "bd_01"})
}

func TestBanPayload_roundtrip(t *testing.T) {
	roundtrip(t, BanPayload{
		Vendor:    VendorOneDrive,
		AccountID: "od_01",
		Reason:    "http 403",
		BanUntil:  time.Date(2026, 7, 21, 12, 0, 0, 0, time.UTC),
	})
}

// Given a BAN payload carrying only vendor/account_id (old CP), When decoded,
// Then Reason/BanUntil stay zero without error.
func TestBanPayload_minimalJSON_decodesToZeroOptionals(t *testing.T) {
	var p BanPayload
	if err := json.Unmarshal([]byte(`{"vendor":"baidu","account_id":"bd_01"}`), &p); err != nil {
		t.Fatalf("minimal BAN payload must decode: %v", err)
	}
	if p.Vendor != VendorBaidu || p.AccountID != "bd_01" {
		t.Errorf("scalars = (%q, %q), want (baidu, bd_01)", p.Vendor, p.AccountID)
	}
	if p.Reason != "" || !p.BanUntil.IsZero() {
		t.Errorf("optionals must be zero, got reason=%q ban_until=%v", p.Reason, p.BanUntil)
	}
}

func TestCredentialChangePayload_roundtrip(t *testing.T) {
	roundtrip(t, CredentialChangePayload{
		Vendor:    VendorBaidu,
		AccountID: "bd_01",
		Credential: Credential{
			RefreshToken: "rt-new",
		},
	})
}

// Given a CREDENTIAL_UPDATE payload without the credential body (old CP
// contract), When decoded, Then Credential stays zero without error — the node
// waits for the next ACCOUNT_SNAPSHOT to converge.
func TestCredentialChangePayload_withoutCredential_decodesZero(t *testing.T) {
	var p CredentialChangePayload
	if err := json.Unmarshal([]byte(`{"vendor":"baidu","account_id":"bd_01"}`), &p); err != nil {
		t.Fatalf("credential-less payload must decode: %v", err)
	}
	if p.Vendor != VendorBaidu || p.AccountID != "bd_01" {
		t.Errorf("scalars = (%q, %q), want (baidu, bd_01)", p.Vendor, p.AccountID)
	}
	if p.Credential.RefreshToken != "" || p.Credential.Cookies != nil ||
		p.Credential.AccessToken != "" || !p.Credential.TokenExpire.IsZero() {
		t.Errorf("Credential must be zero when absent, got %+v", p.Credential)
	}
}

func TestVendor_constants(t *testing.T) {
	if Vendor115 != "115" {
		t.Fatalf("Vendor115 = %q, want %q", Vendor115, "115")
	}
	if VendorBaidu != "baidu" {
		t.Fatalf("VendorBaidu = %q, want %q", VendorBaidu, "baidu")
	}
	if VendorQuark != "quark" {
		t.Fatalf("VendorQuark = %q, want %q", VendorQuark, "quark")
	}
	if VendorOneDrive != "onedrive" {
		t.Fatalf("VendorOneDrive = %q, want %q", VendorOneDrive, "onedrive")
	}
	if VendorAliyundrive != "aliyundrive" {
		t.Fatalf("VendorAliyundrive = %q, want %q", VendorAliyundrive, "aliyundrive")
	}
}

func TestCredential_roundtrip(t *testing.T) {
	roundtrip(t, Credential{
		Cookies:      map[string]string{"cookie1": "val1"},
		AccessToken:  "access_abc",
		RefreshToken: "refresh_xyz",
		TokenExpire:  time.Date(2026, 7, 17, 12, 0, 0, 0, time.UTC),
	})
}

func TestDownloadLink_roundtrip(t *testing.T) {
	roundtrip(t, DownloadLink{
		URL:      "https://example.com/file",
		ExpireAt: time.Date(2026, 7, 17, 13, 0, 0, 0, time.UTC),
		IPBound:  true,
		Headers:  map[string]string{"User-Agent": "test"},
	})
}

func TestHealthState_roundtrip(t *testing.T) {
	roundtrip(t, HealthState{
		State:     "healthy",
		LastCheck: time.Date(2026, 7, 17, 12, 0, 0, 0, time.UTC),
		Latency:   150 * time.Millisecond,
		ErrorMsg:  "",
	})
}

func TestRateLimitConfig_roundtrip(t *testing.T) {
	roundtrip(t, RateLimitConfig{
		QPS:             1.0,
		Burst:           2,
		ConcurrentLimit: 5,
	})
}

func TestFileInfo_roundtrip(t *testing.T) {
	roundtrip(t, FileInfo{
		ID:       "file_001",
		Name:     "video.mp4",
		Size:     1048576,
		IsDir:    false,
		Modified: time.Date(2026, 7, 17, 10, 0, 0, 0, time.UTC),
		Hash:     "sha256:abc123",
	})
}

func TestVendorProfile_roundtrip(t *testing.T) {
	roundtrip(t, VendorProfile{
		Vendor:        Vendor115,
		Weight:        3.0,
		BaseLatencyMs: 100,
		BandwidthMbps: 50,
	})
}

func TestBanSignalError_Error(t *testing.T) {
	err := &BanSignalError{Code: 403, Msg: "rate limited"}
	want := "ban signal: 403 rate limited"
	if got := err.Error(); got != want {
		t.Fatalf("BanSignalError.Error() = %q, want %q", got, want)
	}
}

func TestBlobLocation_WithBackendID(t *testing.T) {
	loc := BlobLocation{
		BlobHash:  "blob_with_backend",
		BackendID: "115:acct_001",
		FileID:    "file_001",
	}
	data, err := json.Marshal(loc)
	if err != nil {
		t.Fatalf("json.Marshal: %v", err)
	}

	var got BlobLocation
	if err := json.Unmarshal(data, &got); err != nil {
		t.Fatalf("json.Unmarshal: %v", err)
	}
	if got.BackendID != "115:acct_001" {
		t.Fatalf("BackendID = %q, want %q", got.BackendID, "115:acct_001")
	}
}

func TestBlobLocation_BackendIDFormat(t *testing.T) {
	// BackendID is "vendor:account_id" format — verify roundtrip with common vendor IDs
	loc := BlobLocation{
		BlobHash:  "old_blob",
		BackendID: "115:acct_001",
		FileID:    "file_001",
	}
	data, err := json.Marshal(loc)
	if err != nil {
		t.Fatalf("json.Marshal: %v", err)
	}
	var got BlobLocation
	if err := json.Unmarshal(data, &got); err != nil {
		t.Fatalf("json.Unmarshal: %v", err)
	}
	if got.BackendID != "115:acct_001" {
		t.Fatalf("BackendID = %q, want %q", got.BackendID, "115:acct_001")
	}
}

func TestBlobLocation_roundtrip_with_backend(t *testing.T) {
	roundtrip(t, BlobLocation{
		BlobHash:  "existing_blob",
		BackendID: "baidu:acct_002",
		FileID:    "file_002",
	})
}

func TestBanSignalError_implements_error(t *testing.T) {
	var err error = &BanSignalError{Code: 429, Msg: "too many requests"}
	if err.Error() != "ban signal: 429 too many requests" {
		t.Fatalf("unexpected error: %s", err.Error())
	}
}

func TestVendor_roundtrip(t *testing.T) {
	// Vendor is a string type — verify JSON marshal/unmarshal
	data, err := json.Marshal(Vendor115)
	if err != nil {
		t.Fatalf("json.Marshal: %v", err)
	}
	var got Vendor
	if err := json.Unmarshal(data, &got); err != nil {
		t.Fatalf("json.Unmarshal: %v", err)
	}
	if got != Vendor115 {
		t.Fatalf("Vendor roundtrip: want %q, got %q", Vendor115, got)
	}
}

func TestClientConfig_roundtrip(t *testing.T) {
	roundtrip(t, ClientConfig{
		ClientID:     "cid",
		ClientSecret: "csecret",
		RedirectURI:  "https://example.com/cb",
		Region:       "cn",
	})
}

// Given an empty ClientConfig, When marshalled, Then omitempty drops every
// field and the output is exactly {}.
func TestClientConfig_empty_marshals_as_empty_object(t *testing.T) {
	data, err := json.Marshal(ClientConfig{})
	if err != nil {
		t.Fatalf("json.Marshal: %v", err)
	}
	if string(data) != "{}" {
		t.Fatalf("empty ClientConfig marshalled as %s, want {}", data)
	}
}

// Given a fully populated AccountSnapshotEntry, When marshalled, Then the JSON
// uses the accountregistry wire names (client_config included) and round-trips.
func TestAccountSnapshotEntry_wire_contract(t *testing.T) {
	entry := AccountSnapshotEntry{
		Vendor:    VendorOneDrive,
		AccountID: "od_acct_01",
		Credential: Credential{
			Cookies:      map[string]string{"sess": "abc"},
			RefreshToken: "rt-od",
		},
		ClientConfig: ClientConfig{
			ClientID:     "cid-od",
			ClientSecret: "cs-od",
			RedirectURI:  "https://login.example/od",
			Region:       "cn",
		},
		RateLimitCfg:  RateLimitConfig{QPS: 5, Burst: 10, ConcurrentLimit: 12},
		VendorProfile: VendorProfile{Vendor: VendorOneDrive, Weight: 3.5, BaseLatencyMs: 120, BandwidthMbps: 80},
		Enabled:       true,
	}
	data, err := json.Marshal(entry)
	if err != nil {
		t.Fatalf("json.Marshal: %v", err)
	}
	var decoded map[string]json.RawMessage
	if err := json.Unmarshal(data, &decoded); err != nil {
		t.Fatalf("json.Unmarshal to map: %v", err)
	}
	for _, key := range []string{"vendor", "account_id", "credential", "client_config", "rate_limit_config", "vendor_profile", "enabled"} {
		if _, ok := decoded[key]; !ok {
			t.Errorf("snapshot JSON missing wire key %q: %s", key, data)
		}
	}
	roundtrip(t, entry)
}
