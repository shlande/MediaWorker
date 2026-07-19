// Package types defines domain types shared across MediaWorker distribution packages.
package types

import (
	"fmt"
	"time"
)

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
	NodeID         string           `json:"node_id"`
	PeerID         PeerId           `json:"peer_id"`
	Capabilities   NodeCapabilities `json:"capabilities"`
	BandwidthQuota int64            `json:"bandwidth_quota"`
	Iat            int64            `json:"iat"`
	Exp            int64            `json:"exp"`
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
//
// DeclaredCapabilities is OPTIONAL: when nil, the control plane grants its
// policy-default capabilities (backward-compatible with legacy clients). When
// present, each granted capability is the intersection of declared ∩ default.
// L4Backhaul inside DeclaredCapabilities is IGNORED — L4 is whitelist-only.
type JWTRequest struct {
	PeerID               PeerId            `json:"peer_id"`
	SignedPeerID         []byte            `json:"signed_peer_id"`
	DeclaredCapabilities *NodeCapabilities `json:"declared_capabilities,omitempty"`
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

// ─── 存储域类型 ───

// Vendor is a cloud drive vendor identifier.
type Vendor string

const (
	Vendor115         Vendor = "115"
	VendorBaidu       Vendor = "baidu"
	VendorQuark       Vendor = "quark"
	VendorOneDrive    Vendor = "onedrive"
	VendorAliyundrive Vendor = "aliyundrive"
)

// Credential holds secret authentication material for a cloud drive account.
// Cookies/RefreshToken are the primary carriers (cookie-based vendors use
// Cookies; OAuth2 vendors use RefreshToken).
type Credential struct {
	Cookies      map[string]string `json:"cookies"`
	AccessToken  string            `json:"access_token"` // deprecated: TokenManager 内存管理，不落库
	RefreshToken string            `json:"refresh_token"`
	TokenExpire  time.Time         `json:"token_expire"` // deprecated: TokenManager 内存管理，不落库
}

// ClientConfig holds the static OAuth2 client material for a cloud drive
// account. It is maintained by an admin (not by the vendor flow) and delivered
// to nodes via account snapshots. Separated from Credential (dynamic secret
// material) per docs/account-backend-adjustments.md B1.
type ClientConfig struct {
	ClientID     string `json:"client_id,omitempty"`
	ClientSecret string `json:"client_secret,omitempty"`
	RedirectURI  string `json:"redirect_uri,omitempty"`
	Region       string `json:"region,omitempty"` // onedrive: global|cn|us|de
}

// AccountSnapshotEntry mirrors the control plane's accountregistry.AccountInfo
// wire shape for node-side decoding of ACCOUNT_SNAPSHOT events. Nodes must NOT
// import controlplane packages, so the contract lives here. JSON tags must stay
// in lockstep with AccountInfo (vendor/account_id/credential/client_config/
// rate_limit_config/vendor_profile/enabled).
type AccountSnapshotEntry struct {
	Vendor        Vendor          `json:"vendor"`
	AccountID     string          `json:"account_id"`
	Credential    Credential      `json:"credential"`
	ClientConfig  ClientConfig    `json:"client_config"`
	RateLimitCfg  RateLimitConfig `json:"rate_limit_config"`
	VendorProfile VendorProfile   `json:"vendor_profile"`
	Enabled       bool            `json:"enabled"`
}

// DownloadLink represents a temporary download URL for a cloud drive file.
type DownloadLink struct {
	URL      string            `json:"url"`
	ExpireAt time.Time         `json:"expire_at"`
	IPBound  bool              `json:"ip_bound"`
	Headers  map[string]string `json:"headers"`
}

// HealthState describes the health status of a cloud drive account.
type HealthState struct {
	State     string        `json:"state"`
	LastCheck time.Time     `json:"last_check"`
	Latency   time.Duration `json:"latency"`
	ErrorMsg  string        `json:"error_msg"`
}

// RateLimitConfig defines rate-limiting parameters for a cloud drive account.
type RateLimitConfig struct {
	QPS             float64 `json:"qps"`
	Burst           int     `json:"burst"`
	ConcurrentLimit int     `json:"concurrent_limit"`
}

// FileInfo represents metadata for a single file on a cloud drive.
type FileInfo struct {
	ID       string    `json:"id"`
	Name     string    `json:"name"`
	Size     int64     `json:"size"`
	IsDir    bool      `json:"is_dir"`
	Modified time.Time `json:"modified"`
	Hash     string    `json:"hash"`
}

// VendorProfile is an operator-configurable capability profile for a cloud drive vendor.
type VendorProfile struct {
	Vendor        Vendor  `json:"vendor"`
	Weight        float64 `json:"weight"`
	BaseLatencyMs int     `json:"base_latency_ms"`
	BandwidthMbps int     `json:"bandwidth_mbps"`
}

// BanSignalError is a typed error representing a vendor ban or throttling signal.
type BanSignalError struct {
	Code int
	Msg  string
}

// Error implements the error interface for BanSignalError.
func (e *BanSignalError) Error() string {
	return fmt.Sprintf("ban signal: %d %s", e.Code, e.Msg)
}

// ─── 来自 ingest 域的类型 ───

// BlobDescriptor describes a single blob at the content-addressed storage layer.
// Produced by ingest; consumed by distribution and storage. BlobHash is SHA-256
// of the blob bytes — globally unique, carries NO business semantics
// (no bitrate, segment number, thumbnail size, etc.).
type BlobDescriptor struct {
	BlobHash string `json:"blob_hash"` // = SHA-256(blob bytes), globally unique, cross-content deduped
	BlobType string `json:"blob_type"` // binary production type, no business semantics:
	//   "mp4_init_segment" | "m4s_media_segment" | "jpeg_original" | "jpeg_thumbnail" | "pdf_page_image" | ...
	Size int64 `json:"size"`
}

// BlobRole describes how a blob is arranged within a specific content. It is the
// metadata-layer counterpart of BlobDescriptor: same BlobHash, but carries the
// business semantics (role / sort_order / business_meta) that the content-addressed
// blob table deliberately does not store.
type BlobRole struct {
	BlobHash     string         `json:"blob_hash"` // references BlobDescriptor.BlobHash
	Role         string         `json:"role"`      // "init" | "media" | "original" | "thumbnail" | "page" | ...
	SortOrder    int            `json:"sort_order"`
	BusinessMeta map[string]any `json:"business_meta,omitempty"` // {"representation_id":"720p","bitrate":1500000} etc.
}

// ContentMeta is content metadata (produced by ingest, consumed by distribution).
// Title is the admin-UI display name (ingest passthrough, may be empty);
// DeletedAt marks a soft-deleted content (nil = live).
type ContentMeta struct {
	ContentID    string     `json:"content_id"`
	ContentType  string     `json:"content_type"`
	TypeMetadata []byte     `json:"type_metadata"`
	Title        string     `json:"title,omitempty"`
	DeletedAt    *time.Time `json:"deleted_at,omitempty"`
}

// ContentIngestedEvent is the ingestion event (published by ingest, subscribed by distribution).
// Carries both the content-addressed blob list (Blobs) and the per-content arrangement (Roles).
type ContentIngestedEvent struct {
	ContentID   string           `json:"content_id"`
	ContentType string           `json:"content_type"`
	Blobs       []BlobDescriptor `json:"blobs"` // content-addressed layer: BlobHash(SHA-256) + BlobType + Size
	Roles       []BlobRole       `json:"roles"` // arrangement layer: role + sort_order + business_meta
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
	NodeID     string   `json:"node_id"`
	ContentID  string   `json:"content_id"`
	PinBlobs   []string `json:"pin_blobs"`
	UnpinBlobs []string `json:"unpin_blobs"`
}

// PinPlan is a pin instruction delivered to a node (control plane → node).
type PinPlan struct {
	Seq        uint64      `json:"seq"`
	TargetNode string      `json:"target_node"`
	Updates    []PinUpdate `json:"updates"`
}

// PinUpdate is a single pin/unpin instruction within a PinPlan.
// PinBlobs and UnpinBlobs are lists of blob_hash strings — no separate
// BlobHash field needed (pin is blob-level, list elements ARE blob hashes).
//
// Compatibility matrix (E6 wire extension):
//   - new CP → old node: ContentID/PinBlobMetas are unknown fields, ignored by
//     the old decoder; the node uses PinBlobs + local metadata as before.
//   - old CP → new node: ContentID empty and PinBlobMetas nil → the node falls
//     back to the PinBlobs + findBlob* lookup path. Metas stay OPTIONAL.
type PinUpdate struct {
	PinBlobs     []string      `json:"pin_blobs"`
	UnpinBlobs   []string      `json:"unpin_blobs"`
	ContentID    string        `json:"content_id,omitempty"`
	PinBlobMetas []PinBlobMeta `json:"pin_blob_metas,omitempty"`
}

// PinBlobMeta carries the per-blob metadata a node needs to apply a pin
// without consulting its local content cache: content-addressed identity
// (BlobHash), binary type (BlobType), arrangement role (Role) and size.
type PinBlobMeta struct {
	BlobHash string `json:"blob_hash"`
	BlobType string `json:"blob_type,omitempty"`
	Role     string `json:"role,omitempty"`
	Size     int64  `json:"size,omitempty"`
}

// PinSpaceInfo is the result of a node pin-space query (node → control plane RPC response).
type PinSpaceInfo struct {
	AvailableBytes  int64 `json:"available_bytes"`
	PinnedCount     int32 `json:"pinned_count"`
	TotalPinnedSize int64 `json:"total_pinned_size"`
}

// NodeStatusReport is a node's periodic status report (node → control plane).
// The extended fields are all omitempty: reports from old nodes lacking them
// (and old CPs receiving extended reports) remain wire-compatible.
type NodeStatusReport struct {
	NodeID       string           `json:"node_id"`
	PeerID       PeerId           `json:"peer_id"`
	Capabilities NodeCapabilities `json:"capabilities"`
	PrefixSpace  PartitionStatus  `json:"prefix_space"`
	WarmSpace    PartitionStatus  `json:"warm_space"`
	Healthy      bool             `json:"healthy"`
	LastUpdate   int64            `json:"last_update"`

	Region            string           `json:"region,omitempty"`               // from node.region config; empty = unknown
	Version           string           `json:"version,omitempty"`              // node binary version
	StartedAt         int64            `json:"started_at,omitempty"`           // unix seconds; uptime = now - StartedAt
	ConnCount         int              `json:"conn_count,omitempty"`           // libp2p connection count
	ColdSpace         *PartitionStatus `json:"cold_space,omitempty"`           // nil until cold cache is wired (cmd/edge-node/main.go:325-327)
	JWTRefreshFail24h int              `json:"jwt_refresh_fail_24h,omitempty"` // JWT refresh failures in trailing 24h (JWTClient.RefreshStats)
}

// PartitionStatus is the status of a single storage partition.
type PartitionStatus struct {
	TotalBytes int64 `json:"total_bytes"`
	UsedBytes  int64 `json:"used_bytes"`
	BlobCount  int32 `json:"blob_count"`
}

// BlobLocation identifies where a blob is stored. BlobHash is the content-addressed
// key (SHA-256); BackendID is "vendor:account_id" format (e.g. "115:acct_03").
// No ContentID — blob_location is cross-content shared (same blob, same locations).
type BlobLocation struct {
	BlobHash  string `json:"blob_hash"`
	BackendID string `json:"backend_id"` // "vendor:account_id", e.g. "115:acct_03"
	FileID    string `json:"file_id"`
}

const (
	EventCredentialUpdate   = "CREDENTIAL_UPDATE"
	EventAccountSnapshot    = "ACCOUNT_SNAPSHOT"
	EventHealthChange       = "HEALTH_CHANGE"
	EventBan                = "BAN"
	EventUnban              = "UNBAN"
	EventCircuitForceOpen   = "CIRCUIT_FORCE_OPEN"
	EventCircuitForceClose  = "CIRCUIT_FORCE_CLOSE"
	EventNewSegmentLocation = "NEW_SEGMENT_LOCATION"
	EventQuotaUpdate        = "QUOTA_UPDATE"
	EventQuotaBorrow        = "QUOTA_BORROW"
	EventContentIngested    = "CONTENT_INGESTED"
)

// Event is a generic event with a type tag and opaque payload (for SyncBroadcasterClient).
type Event struct {
	Type    string `json:"type"`
	Payload []byte `json:"payload"`
}

// CredentialChangePayload is the CREDENTIAL_UPDATE event payload. Credential and
// ClientConfig carry the new auth material; old control planes omit them —
// nodes then wait for the next ACCOUNT_SNAPSHOT (<=60s) to converge instead of
// applying them immediately.
type CredentialChangePayload struct {
	Vendor       Vendor       `json:"vendor"`
	AccountID    string       `json:"account_id"`
	Credential   Credential   `json:"credential,omitempty"`
	ClientConfig ClientConfig `json:"client_config,omitempty"`
}

// BanPayload is the BAN/UNBAN event payload.
type BanPayload struct {
	Vendor    Vendor    `json:"vendor"`
	AccountID string    `json:"account_id"`
	Reason    string    `json:"reason,omitempty"`
	BanUntil  time.Time `json:"ban_until,omitempty"`
}

// CircuitPayload is the CIRCUIT_FORCE_OPEN/CIRCUIT_FORCE_CLOSE event payload.
type CircuitPayload struct {
	Vendor    Vendor `json:"vendor"`
	AccountID string `json:"account_id"`
}
