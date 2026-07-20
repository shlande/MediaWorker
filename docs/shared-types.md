# 共享领域类型与存储层契约

源码：`internal/types/types.go`（全部 JSON 标签即跨服务线上契约）、`internal/shared/`、`internal/storage/`。

## 1. 节点身份与 JWT（网络层）

| 类型 | 关键字段（JSON） | 说明 |
|---|---|---|
| `PeerId` | string | 节点密码学身份：base58(multihash(Ed25519 公钥))，全局唯一不可伪造 |
| `NodeCapabilities` | `edge`, `l4_backhaul`, `relay_provider`, `peer_icp` (bool) | 节点能力集合，由 JWT 授权 |
| `CapabilityJWT` | string | 紧凑 JWT：`base64url(hdr).base64url(payload).base64url(sig)` |
| `NodeJWTPayload` | `node_id`, `peer_id`, `capabilities`, `bandwidth_quota`, `iat`, `exp` | JWT 载荷，1 小时有效期 |
| `PSK` | []byte | 32 字节私网预共享密钥 |
| `DHTBootstrapPeer` | `peer_id`, `addrs` | DHT 引导节点配置 |
| `JWTRequest` | `peer_id`, `signed_peer_id` | 节点 → 控制面 JWT 申请 |
| `JWTResponse` | `jwt`, `refresh_before` | 控制面响应（含提前刷新秒数） |
| `PeerStoreEntry` | `peer_id`, `addrs`, `jwt`, `capabilities`, `jwt_exp`, `last_seen`, `score`, `stale` | BadgerDB 持久化的对端记录，哈希环以其重建 |

**共享工具**（`internal/shared/`）：

- `jwt.SignJWT / VerifyJWT / VerifyJWTAnyPeerID / ExtractEd25519PubKey / GenerateNodeID / GeneratePeerID / SignPeerID`
- 哨兵错误：`ErrInvalidPeerID`、`ErrInvalidSignature`、`ErrJWTExpired`、`ErrJWTBadSignature`、`ErrPeerIDMismatch`、`ErrJWTStaleOrDuplicate`
- `identity.LoadOrGenerateIdentity(keyPath)` → `NodeIdentity{PrivKey, PeerID}`（控制面与节点共用）

## 2. 存储域类型（云盘）

| 类型 | 关键字段 | 说明 |
|---|---|---|
| `Vendor` | string | 厂商标识：`115` / `baidu` / `quark` / `onedrive` / `aliyundrive` |
| `Credential` | `cookies`, `access_token`, `refresh_token`, `token_expire` | 账号凭据 |
| `DownloadLink` | `url`, `expire_at`, `ip_bound`, `headers` | 临时下载链 |
| `HealthState` | `state`(healthy/degraded/banned), `last_check`, `latency`, `error_msg` | 账号健康 |
| `RateLimitConfig` | `qps`, `burst`, `concurrent_limit` | 限流参数 |
| `FileInfo` | `id`, `name`, `size`, `is_dir`, `modified`, `hash` | 云盘文件元数据 |
| `VendorProfile` | `vendor`, `weight`, `base_latency_ms`, `bandwidth_mbps` | 运营可配厂商画像，用于账号池打分 |
| `BanSignalError` | `Code`, `Msg` | 厂商封禁/限速信号（403/405/429），不序列化 |

## 3. 内容与入库域类型

| 类型 | 关键字段 | 说明 |
|---|---|---|
| `BlobDescriptor` | `blob_hash`(SHA-256), `blob_type`, `size` | 内容寻址 blob，入库产出、分发消费 |
| `BlobRole` | `blob_hash`, `role`(init/media/original/thumbnail/page), `sort_order`, `business_meta` | blob 在内容内的编排语义 |
| `ContentMeta` | `content_id`, `content_type`, `type_metadata`(JSON []byte) | 内容元数据 |
| `ContentIngestedEvent` | `content_id`, `content_type`, `blobs[]`, `roles[]`, `timestamp` | 入库事件（触发分发侧初始 pin） |

## 4. 分发域类型（Pin 与状态）

| 类型 | 关键字段 | 说明 |
|---|---|---|
| `NodeSpaceInfo` | `node_id`, `available_bytes`, `pinned_count` | 节点空间统计（控制面维护） |
| `NodePinPlan` | `node_id`, `content_id`, `pin_blobs[]`, `unpin_blobs[]` | 单节点 pin 计划（策略层产出） |
| `PinPlan` | `seq`, `target_node`, `updates[]` | 下发到节点的 pin 指令 |
| `PinUpdate` | `pin_blobs[]`, `unpin_blobs[]` | 单条 pin/unpin 指令 |
| `PinSpaceInfo` | `available_bytes`, `pinned_count`, `total_pinned_size` | 节点 pin 空间查询结果 |
| `NodeStatusReport` | `node_id`, `peer_id`, `capabilities`, `prefix_space`, `warm_space`, `healthy`, `last_update` | 节点周期上报 |
| `PartitionStatus` | `total_bytes`, `used_bytes`, `blob_count` | 单分区状态 |
| `BlobLocation` | `blob_hash`, `backend_id`("vendor:account_id"), `file_id` | blob 存储位置，跨内容共享去重 |

## 5. 事件类型常量（`types.go:266-277`）

`CREDENTIAL_UPDATE`、`HEALTH_CHANGE`、`BAN`、`UNBAN`、`NEW_SEGMENT_LOCATION`、`QUOTA_UPDATE`、`QUOTA_BORROW`、`CONTENT_INGESTED`。通用信封：`Event{ type, payload([]byte) }`。

## 6. 存储层接口契约（`internal/storage/`）

### 6.1 `driver.Driver` —— 云盘统一抽象（最核心的存储契约）

```go
type Driver interface {
    Vendor() types.Vendor
    List(ctx, dirID string, page int) ([]types.FileInfo, error)
    Get(ctx, fileID string) (types.FileInfo, error)
    GetLink(ctx, fileID string) (*types.DownloadLink, error)
    Put(ctx, dirID, name string, r io.Reader, size int64) (*types.FileInfo, error)
    Remove(ctx, fileID string) error
    Mkdir(ctx, parentID, name string) (*types.FileInfo, error)
    HealthCheck(ctx) types.HealthState
    RateLimitConfig() types.RateLimitConfig
}
```

实现：`baidu.BaiduDriver`（PCS API）、`onedrive.OneDriveDriver`（Graph API）；mock：115 / 夸克 / 阿里云盘。`DriverRegistry` 按厂商注册/查找。

### 6.2 `accountpool.AccountPool` —— 账号池

- `SelectForRead(ctx, blobHash)`：按 blob 位置过滤健康/熔断/限流账号，按 `score = 并发数 / 厂商权重` 升序选最优。
- `SelectK(ctx, k)`：上传选 K 个账号，跨厂商分散（每厂商 ≤ k/2）。
- `UploadBlob`：K=2 并行上传。另含 `ReplaceAll`（快照同步）、`UpdateCredential`、`UpdateHealth`、`MarkBanned`（置 banned + 强制开熔断）、`SnapshotAccounts`。
- 配套接口：`Limiter`（`Allow/SetLimit`）、`CircuitBreaker`（`State/ForceOpen/ForceClose`）、`BlobLocationClient`（`GetBlobLocations`）。

### 6.3 `dataplane.LocalDataPlane` —— L4 回源

`FetchBlobLocal(ctx, blobHash) → io.ReadCloser`：位置查询 → 选账号 → 匹配 BlobLocation → LinkPool 取链 → HTTP GET → 检查封禁信号（403/405/429 → `BanSignalError`）→ 返回流。

### 6.4 其余组件

| 组件 | 契约要点 |
|---|---|
| `linkpool.LinkPool` | 下载链 LRU（默认 1 万条），剩余 >2min 直接命中，5min 窗口后台刷新，miss 同步拉取 |
| `circuitbreaker.CircuitBreaker` | Closed→Open→HalfOpen；`BanSignalError` 累计达阈值（默认 5）开断 10min |
| `healthcheck.HealthChecker` | 周期探测全部账号，可经 `MetadataWriter.ReportAccountHealth` 上报控制面 |
| `auth.TokenManager` | OAuth2 refresh_token 授权，缓存 >5min 直接命中，singleflight 去重；OneDrive 按 region 选端点 |
| `quota.QuotaAllocator` | 控制面侧配额再平衡：`baseShare = globalQPS×0.8/节点数`，按负载调整；借用审批（全局用量 <80% 时 +30%，30s） |
| `quota.BorrowableLimiter` | 节点侧可借用限流器，先消耗基础令牌再消耗借用令牌 |
| `monitor.StorageMetrics` / `MetricsServer` | `storage_` 前缀 Prometheus 指标 + `/metrics` HTTP 服务 |

## 7. 配置系统

三套 YAML 加载器（`internal/config/`），均用 `yaml.v3`，必填字段缺失即报错：

| 加载器 | 服务 | 必填项 |
|---|---|---|
| `LoadConfig` | edge-node | `priv_key_path`、`listen`、`dht.namespace`、`jwt_service.endpoint` |
| `LoadControlPlaneConfig` | control-plane | `jwt_http.listen`、`dht_bootstrap.namespace`、`metadata.pg_dsn`、两个身份密钥路径 |
| `LoadIngestWorkerConfig` | ingest-worker | `http.listen`、`metadata.pg_dsn`、`ingest.ffmpeg_path`、`ingest.work_dir` |

身份密钥：`LoadOrGenerateControlPlaneKey`（PEM PKCS#8，0o600）与 `LoadOrGenerateIdentity`（libp2p protobuf）均在文件不存在时自动生成。

## 8. 端口速查

| 服务 | 端口 | 协议 |
|---|---|---|
| edge-node（L4 / 非 L4） | 9001 | libp2p TCP + QUIC |
| edge-node | 8080 | HTTP（blob 取流） |
| control-plane | 8443 | HTTP（JWT 签发） |
| control-plane | 9001 | libp2p（DHT bootstrap） |
| ingest-worker | 8080（K8s Service 80→8080） | HTTP |
| Prometheus | 可配（如 2112） | HTTP `/metrics` |
