package types

import (
	"encoding/json"
	"reflect"
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
		BlobHash:  "abc123def456",
		BlobType:  "media",
		Size:      1048576,
		SortOrder: 1,
	})
}

func TestContentMeta_roundtrip(t *testing.T) {
	roundtrip(t, ContentMeta{
		ContentID:    "cont_001",
		ContentType:  "dash_video",
		TypeMetadata: []byte(`{"width":1920,"height":1080}`),
	})
}

func TestContentIngestedEvent_roundtrip(t *testing.T) {
	roundtrip(t, ContentIngestedEvent{
		ContentID:   "cont_002",
		ContentType: "image",
		Blobs: []BlobDescriptor{
			{BlobHash: "blob_a", BlobType: "original", Size: 512000, SortOrder: 0},
			{BlobHash: "blob_b", BlobType: "thumbnail", Size: 32000, SortOrder: 1},
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
			{BlobHash: "blob_x", PinBlobs: []string{"blob_x"}, UnpinBlobs: nil},
			{BlobHash: "blob_y", PinBlobs: nil, UnpinBlobs: []string{"blob_z"}},
		},
	})
}

func TestPinUpdate_roundtrip(t *testing.T) {
	roundtrip(t, PinUpdate{
		BlobHash:   "blob_abc",
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
			Edge:           false,
			L4Backhaul:     true,
			RelayProvider:  true,
			PeerICP:        false,
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
		Edge:           true,
		L4Backhaul:     false,
		RelayProvider:  true,
		PeerICP:        false,
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
			Edge:           true,
			L4Backhaul:     false,
			RelayProvider:  false,
			PeerICP:        true,
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
			Edge:           true,
			L4Backhaul:     false,
			RelayProvider:  false,
			PeerICP:        true,
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
		Vendor:    "s3",
		AccountID: "acct_123",
		FileID:    "file_456",
	})
}

func TestEvent_roundtrip(t *testing.T) {
	roundtrip(t, Event{
		Type:    "content_ingested",
		Payload: []byte(`{"content_id":"cont_002"}`),
	})
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

func TestBlobLocation_WithContentID(t *testing.T) {
	loc := BlobLocation{
		BlobHash:  "blob_with_cid",
		Vendor:    "115",
		AccountID: "acct_001",
		FileID:    "file_001",
		ContentID: "cont_001",
	}
	data, err := json.Marshal(loc)
	if err != nil {
		t.Fatalf("json.Marshal: %v", err)
	}

	var got BlobLocation
	if err := json.Unmarshal(data, &got); err != nil {
		t.Fatalf("json.Unmarshal: %v", err)
	}
	if got.ContentID != "cont_001" {
		t.Fatalf("ContentID = %q, want %q", got.ContentID, "cont_001")
	}
}

func TestBlobLocation_OldJSONWithoutContentID(t *testing.T) {
	// Old JSON without "content_id" field — must deserialize with ContentID==""
	oldJSON := `{"blob_hash":"old_blob","vendor":"115","account_id":"acct_001","file_id":"file_001"}`
	var loc BlobLocation
	if err := json.Unmarshal([]byte(oldJSON), &loc); err != nil {
		t.Fatalf("json.Unmarshal old JSON: %v", err)
	}
	if loc.BlobHash != "old_blob" {
		t.Fatalf("BlobHash = %q, want %q", loc.BlobHash, "old_blob")
	}
	if loc.ContentID != "" {
		t.Fatalf("ContentID should be empty for old JSON, got %q", loc.ContentID)
	}
}

func TestBlobLocation_roundtrip_with_old_fields(t *testing.T) {
	// Ensure existing fields still roundtrip correctly without ContentID set
	roundtrip(t, BlobLocation{
		BlobHash:  "existing_blob",
		Vendor:    "baidu",
		AccountID: "acct_002",
		FileID:    "file_002",
		// ContentID intentionally empty
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