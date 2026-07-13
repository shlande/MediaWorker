// Package types defines domain types shared across MediaWorker distribution packages.
package types

// ─── 节点身份 (libp2p PeerId 绑定) ───

// PeerId is a node's cryptographic identity derived from an Ed25519 public key's multihash.
// Format: base58(multihash(pubkey)). Globally unique and unforgeable; a node carrying its
// private key retains the same identity across restarts or migration.
type PeerId string

// NodeCapabilities is the set of capabilities a node declares.
// Authorized via JWT; the control plane selects which capabilities are allowed when signing.
type NodeCapabilities struct {
	Edge          bool `json:"edge"`
	L4Backhaul    bool `json:"l4_backhaul"`
	RelayProvider bool `json:"relay_provider"`
	PeerICP       bool `json:"peer_icp"`
}

// CapabilityJWT is a node admission credential signed by the control plane (JWT format).
// Nodes present it to other nodes, which locally verify the signature without querying a
// bootstrap node. Format: base64url(header).base64url(payload).base64url(sig).
type CapabilityJWT string

// NodeJWTPayload is the decoded payload inside a CapabilityJWT.
type NodeJWTPayload struct {
	NodeID          string           `json:"node_id"`
	PeerID          PeerId           `json:"peer_id"`
	Capabilities    NodeCapabilities `json:"capabilities"`
	BandwidthQuota  int64            `json:"bandwidth_quota"`
	Iat             int64            `json:"iat"`
	Exp             int64            `json:"exp"`
}

// ─── 私有网络准入 (PSK) ───

// PSK (Pre-Shared Key) is a 32-byte random key injected at build time via
// `-ldflags "-X main.psk=<hex>"`. All mainnet nodes share the same PSK. When
// libp2p.PrivateNetwork(psk) is enabled, connections failing PSK exchange are
// rejected at the TCP/QUIC layer before entering libp2p security (Noise/TLS).
type PSK []byte

// ─── DHT 发现 ───

// DHTBootstrapPeer configures a DHT bootstrap peer (routing-only, not authentication).
type DHTBootstrapPeer struct {
	PeerID PeerId   `json:"peer_id"`
	Addrs  []string `json:"addrs"`
}

// ─── JWT 签发 ───

// JWTRequest is a node's request to the control-plane JWT service for a signed JWT.
type JWTRequest struct {
	PeerID       PeerId `json:"peer_id"`
	SignedPeerID []byte `json:"signed_peer_id"`
}

// JWTResponse is the control-plane's response containing the signed JWT and refresh info.
// RefreshBefore is seconds before Exp that the node should begin refreshing.
type JWTResponse struct {
	JWT           CapabilityJWT `json:"jwt"`
	RefreshBefore int64         `json:"refresh_before"` // seconds before exp to start refreshing
}

// PeerStoreEntry is a node's locally persisted peer information (BadgerDB, recoverable on restart).
// The hash ring is rebuilt from the PeerStore without depending on control-plane broadcast.
// ★ Write/update PeerStore only after verifying the peer's JWT (§3.2 InterceptSecured).
type PeerStoreEntry struct {
	PeerID       PeerId           `json:"peer_id"`
	Addrs        []string         `json:"addrs"`
	JWT          CapabilityJWT    `json:"jwt"`
	Capabilities NodeCapabilities `json:"capabilities"`
	JWTExp       int64            `json:"jwt_exp"`
	LastSeen     int64            `json:"last_seen"`
	Score        float64          `json:"score"`
	Stale        bool             `json:"stale"`
}

// ─── 来自 ingest 域的类型 ───

// BlobDescriptor describes a single blob (produced by the ingest domain, consumed by distribution).
type BlobDescriptor struct {
	BlobHash  string `json:"blob_hash"`
	BlobType  string `json:"blob_type"`
	Size      int64  `json:"size"`
	SortOrder int    `json:"sort_order"`
}

// ContentMeta is content metadata (produced by ingest, consumed by distribution).
type ContentMeta struct {
	ContentID    string `json:"content_id"`
	ContentType  string `json:"content_type"`
	TypeMetadata []byte `json:"type_metadata"`
}

// ContentIngestedEvent is the ingestion event (published by ingest domain, subscribed by distribution).
type ContentIngestedEvent struct {
	ContentID   string           `json:"content_id"`
	ContentType string           `json:"content_type"`
	Blobs       []BlobDescriptor `json:"blobs"`
	Timestamp   int64            `json:"timestamp"`
}

// ─── 分发域内部类型 ───

// NodeSpaceInfo is per-node space statistics (maintained by the control plane from NodeStatusReport).
type NodeSpaceInfo struct {
	NodeID         string `json:"node_id"`
	AvailableBytes int64  `json:"available_bytes"`
	PinnedCount    int32  `json:"pinned_count"`
}

// NodePinPlan is the pin plan for a single node (produced by the strategy layer).
type NodePinPlan struct {
	NodeID      string   `json:"node_id"`
	ContentID   string   `json:"content_id"`
	PinBlobs    []string `json:"pin_blobs"`
	UnpinBlobs  []string `json:"unpin_blobs"`
}

// PinPlan is a pin instruction delivered to a node (control plane → node).
type PinPlan struct {
	Seq        uint64      `json:"seq"`
	TargetNode string      `json:"target_node"`
	Updates    []PinUpdate `json:"updates"`
}

// PinUpdate is a single pin/unpin instruction within a PinPlan.
type PinUpdate struct {
	BlobHash   string   `json:"blob_hash"`
	PinBlobs   []string `json:"pin_blobs"`
	UnpinBlobs []string `json:"unpin_blobs"`
}

// PinSpaceInfo is the result of a node pin-space query (node → control plane RPC response).
type PinSpaceInfo struct {
	AvailableBytes  int64 `json:"available_bytes"`
	PinnedCount     int32 `json:"pinned_count"`
	TotalPinnedSize int64 `json:"total_pinned_size"`
}

// NodeStatusReport is a node's periodic status report (node → control plane).
type NodeStatusReport struct {
	NodeID       string           `json:"node_id"`
	PeerID       PeerId           `json:"peer_id"`
	Capabilities NodeCapabilities `json:"capabilities"`
	PrefixSpace  PartitionStatus  `json:"prefix_space"`
	WarmSpace    PartitionStatus  `json:"warm_space"`
	Healthy      bool             `json:"healthy"`
	LastUpdate   int64            `json:"last_update"`
}

// PartitionStatus is the status of a single storage partition.
type PartitionStatus struct {
	TotalBytes int64 `json:"total_bytes"`
	UsedBytes  int64 `json:"used_bytes"`
	BlobCount  int32 `json:"blob_count"`
}

// BlobLocation identifies where a blob is stored (for MetadataClient interface).
type BlobLocation struct {
	BlobHash  string `json:"blob_hash"`
	Vendor    string `json:"vendor"`
	AccountID string `json:"account_id"`
	FileID    string `json:"file_id"`
}

// Event is a generic event with a type tag and opaque payload (for SyncBroadcasterClient).
type Event struct {
	Type    string `json:"type"`
	Payload []byte `json:"payload"`
}