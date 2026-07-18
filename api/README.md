# MediaWorker 系统模块与接口文档

> 本文档基于源码实际实现整理（以 `internal/` 与 `cmd/` 下的代码为准），用于说明系统模块划分、各模块功能、对外接口设计与规划中的功能。
> 架构设计背景与取舍请参考 `docs/` 目录下的领域文档；本文档聚焦"接口契约"层面。

## 1. 系统概览

MediaWorker 是一个基于 **libp2p 私有 P2P 网络** 的分布式媒体分发系统，由三个独立部署的服务进程组成：

| 服务 | 入口 | 角色 | 对外端口 |
|---|---|---|---|
| **control-plane**（控制面） | `cmd/control-plane/` | 节点鉴权（签发能力 JWT）、DHT 引导、Pin 编排、云盘账号注册表、元数据存储、blob 位置查询 API（T9） | HTTP `:8443`（JWT 签发 + blob 位置查询 + `/metrics`）、libp2p `:9001`（DHT bootstrap） |
| **edge-node**（边缘节点） | `cmd/edge-node/` | 面向终端用户的 blob 内容分发、三级缓存、ICP 节点协作、哈希环路由（T11/T12）、L4 回源 | HTTP `:8080`（blob 取流 + `/metrics`）、libp2p `:9001` |
| **ingest-worker**（入库 worker） | `cmd/ingest-worker/` | 内容入库：转码/缩略图处理、K 冗余上传云盘、元数据事务写入、事件直发控制面（T8） | HTTP `:8080`（含 `/metrics`） |
| **janitor**（GC 服务） | `cmd/janitor/` | 独立 GC 二进制：两阶段软删（mark + sweep）、dry-run 默认 true、once/interval 双模式（T13/T14） | 无 HTTP 端口（CLI 运行） |

通信方式分三类：

- **HTTP**：客户端-facing 接口（取 blob、提交入库、申请 JWT）。
- **libp2p 流协议**：节点间与控制面 ↔ 节点间的内部通道（鉴权握手、ICP 缓存协作、控制事件下发）。
- **GossipSub 发布订阅**：节点间热度（popularity） gossip 同步。

```
                 ┌────────────────────────────────────────────┐
                 │              control-plane                 │
                 │  POST /v1/node/jwt (HTTP :8443)            │
                 │  DHT bootstrap (libp2p :9001)              │
                 │  PinOrchestrator / AccountRegistry         │
                 └───────┬───────────────────────▲────────────┘
        PIN_PLAN_UPDATE │ /edge/control/1.0.0   │ NODE_STATUS_REPORT
        CREDENTIAL_UPDATE│                      │ CONTENT_INGESTED
                 ┌───────▼───────────────────────┴────────────┐
                 │         edge-node 私有 P2P 网络             │
                 │  /edge/auth · /edge/blob/head · /edge/blob/get
                 │  GossipSub: edge-popularity-v1             │
                 └───────▲────────────────────────────────────┘
                         │ GET /blob/{hash} (HTTP :8080)
                      终端用户
                 ┌────────────────────────────────────────────┐
                 │              ingest-worker                 │
                 │  POST /ingest/{content_type} (HTTP :8080)  │
                 │  ffmpeg 转码 → K=2 冗余上传云盘 → PG 事务   │
                 └────────────────────────────────────────────┘
```

## 2. 模块与功能对照

### 2.1 control-plane（`internal/controlplane/`）

| 子模块 | 功能 |
|---|---|
| `jwt/` | 节点身份校验（Ed25519 签名验证）、能力 JWT 签发、按 IP 限流（1 次/小时）、L4 白名单（BadgerDB 持久化）、审计日志 |
| `pinstrategy/` | Pin 编排器：依据节点状态上报与 24h 内容热度，用可插拔策略（已实现 DASH 视频策略）计算各节点 pin/unpin 计划并下发 |
| `syncbroadcaster/` | 控制面 ↔ 节点的 libp2p 控制通道（`/edge/control/1.0.0`），支持定向发送、广播、重连回放（1000 条环形缓冲） |
| `dhtbootstrap/` | 公共 libp2p Kademlia DHT 引导节点（Server 模式），供边缘节点在私有命名空间内互相发现 |
| `metadata/` | PostgreSQL 元数据客户端：blob / blob_location / content / content_blob / 热度 / 账号健康，内嵌 SQL 迁移（13 个迁移文件） |
| `accountregistry/` | 云盘账号主库（`cloud_account` 表），凭据更新事件推送 + 周期全量快照广播 |

### 2.2 edge-node（`internal/node/`）

| 子模块 | 功能 |
|---|---|
| `backhaul/` | 回源管线编排：本地缓存 → ICP 兄弟节点 → 数据面（L4）/ L4 流式回退（非 L4），singleflight 合并同 blob 并发请求 |
| `cache/` | 三级磁盘缓存：prefix（NVMe，pin 驻留不淘汰）→ warm（SSD，内容感知 LRU）→ cold（HDD，简单 LRU），外加内存索引 |
| `dht/` | 私有 DHT 发现：Advertise / FindPeers 循环，支持从 GossipSub prune 消息做 PeX |
| `gossippop/` | GossipSub 热度同步：6 分钟滑窗本地热度统计、节点信誉分（灰名单 -20）、加权合并远端热度 |
| `hashring/` | 一致性哈希环（CRC32，150 虚拟节点），防抖重建，心跳丢失 3 次标记 stale |
| `icp/` | Inter-Cache Peer 协议：HEAD 存在性探测（10ms 超时）与 GET 拉流 |
| `jwt/` | JWT 客户端（向控制面申请，指数退避重试）、验签器、JWT 推送刷新协议处理 |
| `libp2phost/` | libp2p 主机生命周期：PSK 私网、NAT 穿透（AutoNAT/relay/DCUtR）、连接门控（CIDR 白名单 + 按 IP 限流 + JWT 过期检查） |
| `monitor/` | Prometheus 指标定义（缓存命中、TTFB、回源带宽、节点分数等） |
| `peerstore/` | BadgerDB 持久化的对端元数据存储（JWT、能力、分数、stale 标记） |
| `pinstore/` | Pin 状态管理：BadgerDB 元数据 + NVMe 数据分区，幂等 ApplyPin/ApplyUnpin，异步取 blob |
| `pinstrategy/` | 节点侧 PinPlan 执行器：逐条解析 PinUpdate 并调用 PinStore |
| `routing/` | 哈希环路由（`EdgeRouter`）+ DNS+302 调度器。T11 已将 `EdgeRouter.HandleBlobRequest` 挂 edge-node HTTP mux；`scheduler.go` 仍为模拟实现（非生产路径） |
| `syncbroadcaster/` | 节点侧控制通道客户端：接收 PinPlan，回传 NodeStatusReport |

### 2.3 ingest-worker（`internal/ingest/`）

| 子模块 | 功能 |
|---|---|
| `ingest.go` | 核心接口定义：`ContentIngester`、`BlobStoreWriter`、`BackendPool`、`EventPublisher` |
| `pipeline.go` | 入库管线编排：路由 → 处理 → 并发上传（限 10、K=2 冗余、容忍部分失败）→ PG 事务 → 事件发布 |
| `dash.go` | `dash_video` 类型处理器：ffmpeg 多码率 DASH 转码、SHA-256 计算、blob 角色/元数据生成 |
| `image.go` | `image` 类型处理器：原图保存、JPEG 缩略图（默认 200/800 宽）、简化 EXIF 方向提取 |
| `adapters.go` | 桥接层：账号池适配上传后端、`LogPublisher`（保留供测试与降级） |
| `syncpub/` | T8 新增：`SyncPublisher` 经 libp2p `/edge/control/1.0.0` 直发控制面 `SyncBroadcaster`（`CONTENT_INGESTED`），3x 重试 + 失败计数 + 启动 `CheckConnectivity` fail-closed |

### 2.4 共享层（`internal/types/`、`internal/shared/`、`internal/storage/`、`internal/config/`）

| 模块 | 功能 |
|---|---|
| `types/` | 全部跨服务领域类型（节点身份、JWT、blob、内容、Pin、账号），JSON 标签即线上契约 |
| `shared/jwt`、`shared/identity` | JWT 签发/验签、Ed25519 身份加载/生成（控制面与节点共用） |
| `storage/driver/` | 云盘统一抽象 `Driver` 接口（List/Get/GetLink/Put/Remove/Mkdir/HealthCheck）；已实现 Baidu、OneDrive，mock 了 115/Quark/AliyunDrive |
| `storage/accountpool/` | 账号池：健康/熔断/限流/并发加权选账号（读 `SelectForRead`、写 `SelectK` 跨厂商冗余） |
| `storage/dataplane/` | L4 本地数据面：blob → 位置查询 → 选账号 → 取下载链 → HTTP 拉流 |
| `storage/linkpool/` | 下载链 LRU 缓存（默认 1 万条），到期前后台刷新 |
| `storage/circuitbreaker/` | 熔断器：Closed→Open→HalfOpen，BanSignal 累计阈值触发 |
| `storage/healthcheck/` | 账号周期健康探测，可上报控制面 |
| `storage/auth/` | OAuth2 TokenManager（refresh_token 授权、singleflight 去重） |
| `storage/quota/` | 配额分配器（控制面）+ 可借用限流器（节点侧） |
| `storage/monitor/` | 存储域 Prometheus 指标与 `/metrics` HTTP 服务 |
| `config/` | 三套 YAML 配置加载器（edge-node / control-plane / ingest-worker），身份密钥管理 |

## 3. 接口总表

### 3.1 HTTP 接口

| 服务 | 方法 | 路径 | 功能 | 鉴权 |
|---|---|---|---|---|
| control-plane | POST | `/v1/node/jwt` | 节点凭签名 PeerID 换取能力 JWT | Ed25519 签名 + 按 IP 限流 |
| control-plane | GET | `/v1/blob-locations/{hash}` | 按 blob hash 查询存储位置（T9） | Bearer JWT + Edge 能力 |
| control-plane | GET | `/metrics` | Prometheus 抓取（T20） | 无（部署层隔离） |
| edge-node | GET | `/blob/{hash}` | 按 SHA-256 哈希取 blob 字节流 | 无（当前实现） |
| edge-node | GET | `/metrics` | Prometheus 抓取（T20） | 无（部署层隔离） |
| ingest-worker | POST | `/ingest/{content_type}` | multipart 上传内容并入库 | 无（当前实现） |
| ingest-worker | GET | `/healthz` | 存活探针 | 无 |
| ingest-worker | GET | `/metrics` | Prometheus 抓取（T20） | 无（部署层隔离） |

> 备注：`edge-node` 与 `ingest-worker` 的 HTTP 面当前均无鉴权中间件，依赖部署层（内网/K8s Service）隔离。control-plane 的 `/v1/blob-locations/*` 与 `/v1/node/jwt` 共用同一 mux，但前者要求 Bearer JWT 鉴权（见 `api/control-plane.md` §2.2）。

### 3.2 libp2p 流协议（节点间 / 控制面 ↔ 节点）

| 协议 ID | 方向 | 功能 |
|---|---|---|
| `/edge/auth/1.0.0` | 节点 → 节点 | 连接建立后出示/校验 JWT 握手 |
| `/edge/jwt-refresh/1.0.0` | 节点 → 节点 | 推送更新的 JWT（按 Exp 去重） |
| `/edge/blob/head/1.0.0` | 节点 → 节点 | ICP 存在性探测（0x01/0x00） |
| `/edge/blob/get/1.0.0` | 节点 → 节点 | ICP 拉取 blob 原始字节流 |
| `/edge/control/1.0.0` | 双向 | 控制面下发 PinPlan/凭据/快照；节点上报状态/入库事件 |

### 3.3 GossipSub 主题

| 主题 | 发布者 | 内容 |
|---|---|---|
| `edge-popularity-v1` | 每个 edge-node（30s 周期） | 签名后的本地 blob 请求热度计数，供全网合并 |

### 3.4 控制通道事件类型（`/edge/control/1.0.0` 载荷）

| 事件 | 方向 | 载荷类型 | 说明 |
|---|---|---|---|
| `PIN_PLAN_UPDATE` | 控制面 → 节点 | `PinPlan` | pin/unpin 指令 |
| `NODE_STATUS_REPORT` | 节点 → 控制面 | `NodeStatusReport` | 空间与健康状态 |
| `CONTENT_INGESTED` | 节点/入库 → 控制面 | `ContentIngestedEvent` | 新内容通知（触发初始 pin） |
| `CREDENTIAL_UPDATE` | 控制面 → 全部节点 | `CredentialChangePayload` | 云盘凭据轮换 |
| `ACCOUNT_SNAPSHOT` | 控制面 → 全部节点 | `[]AccountInfo` | 账号全量快照（约 60s 周期） |

## 4. 规划/未接线功能与已落地项对照

> 本节原本罗列"代码已存在但未启用"项。经 T1-T20 remediation，多数已落地。下表逐项标注当前状态，原历史记录保留在 plan checkbox 22 与 git history。

| 功能 | 位置 | 当前状态 | 落地 todo |
|---|---|---|---|
| 哈希环路由 + 代理到主节点 | `internal/node/routing/edge_router.go` + `cmd/edge-node/main.go` | **已接线**：`EdgeRouter.HandleBlobRequest` 挂 HTTP mux，代理失败回退本地 backhaul | T11/T12 |
| DNS+302 区域调度器 | `internal/node/routing/scheduler.go` | 模拟实现，未接入 main.go（设计探索，非生产路径） | — |
| 非 L4 节点的 L4 流式回退取流 | `backhaul.HandleBlobNoL4` 中 `L4Fetcher` | **已接线**：`backhaulICPFetcher` 经 `ring.Get` + `icp.FetchFromPeer` 向主节点拉流，并写回本地 warmCache | T12 |
| ingest 事件经 SyncBroadcaster 实时推送 | `internal/ingest/syncpub/`（T8） | **已接线**：`SyncPublisher` 经 libp2p `/edge/control/1.0.0` 直发控制面 `SyncBroadcaster`，事件类型 `CONTENT_INGESTED`；启动 `CheckConnectivity` fail-closed | T8 |
| IngestOrchestrator gRPC 任务派发 | 仅存在于 `docs/ingest/` 设计 | 未实现，当前为 HTTP 直推（独立部署模式） | — |
| ingest 冗余度配置生效 | `pipeline.go` + `NewIngestPipeline` | **已生效**：`ingest.redundancy` 经 `NewIngestPipeline(... redundancy)` 传入 `uploadAllBlobs`，`<=0` 规范化为 `2` | T3 |
| edge-node `/metrics` 暴露 | `monitor.Metrics.HTTPHandler()` | **已暴露**：edge-node、control-plane、ingest-worker 三服务 mux 均挂 `GET /metrics`，无独立端口 | T20 |
| 部分配置项（`advertise_interval`、`peer_store.gc_interval`、JWT/NAT/cache enabled 等） | `internal/config/` | **已接线**：`advertise_interval` 入 `NewEdgeDiscovery`；`peer_store.gc_interval` 启 `PeerEntryStore.StartValueLogGC`；`*_cache.enabled` 门控构造；`nat_traversal.*` 经 `*bool` + Effective() 路径生效 | T15 |
| 配置项删除（`cold_cache`、`fetch_segment_*`、`metadata.popularity_query_interval` 等） | `internal/config/` | **已删除**：相关字段移除，`scanDeprecatedConfigKeys` 对遗留键发 `slog.Warn` | T17 |
| 位置查询控制面 API | `GET /v1/blob-locations/{hash}`（T9） | **新增**：control-plane JWT HTTP server mux 新增此路由；Bearer JWT + Edge capability 鉴权；返回 `{"locations":[...]}` | T9 |
| 位置查询客户端 | `internal/storage/dataplane/httplocclient.go`（T10） | **新增**：`HTTPLocationClient`，404 → 空切片+nil error；edge main 暂未接线（`LocalDataPlane` 保持 nil，独立后续项） | T10 |
| janitor GC 服务 | `cmd/janitor`（T13/T14） | **新增**：独立二进制；两阶段软删 + dry-run 默认 true + once/interval 双模式；详见 `api/janitor.md` | T13/T14 |
| ingest 临时目录治理 | `pipeline.go` `ProcessResult.WorkDir` + `sweepStaleWorkDir` | **已生效**：处理成功后 `defer os.RemoveAll(result.WorkDir)`，启动扫描陈旧目录 | T4 |
| ingest 上传限额 + work_dir 空闲检查 | `http.max_upload_bytes` + `checkWorkDirDiskSpace` | **已生效**：默认 10 GiB，启动时若 free < 2×MaxUploadBytes 发 `slog.Warn` | T5 |
| JWT 策略化 + 节点续签循环 | CP `jwt_policy` + 节点 `runJWTRefreshLoop` | **已生效**：`default_capabilities` 与 `declared ∩ default` 授权；节点循环按 `min(refreshInterval, exp-now-refreshBeforeExpiry)` 续签 | T6/T7 |
| gossip 热度驱动缓存淘汰 | `WarmCache.SetPinChecker` + `SetPopSource` + `MergedPopularity.Snapshot` | **已生效**：gossip 订阅合并热度，经 `MinTrustedWeight` 过滤后驱动 `Evict` | T18 |
| 配置接线批 B（CP 侧） | `jwt_http.read/write_timeout`、`pin_orchestrator.top_contents_limit`、`sync_broadcaster.{protocol_id,send_timeout}` | **已生效**：经函数式选项或 `Run` 参数传入 | T16 |
| metrics 跨服务装配 | edge-node/CP/ingest-worker | **已暴露**：详见上文 `/metrics` 行 | T20 |
| CP/Node 包隔离边界 | `internal/controlplane/metrics/` | **已修复**：CP 专属计数器迁出 `internal/node/monitor`，恢复 `TestIsolation_ControlPlaneBinaryNoNodeCode` | T20-fix |

## 5. 详细文档导航

- [control-plane 接口详情](./control-plane.md)
- [edge-node 接口详情](./edge-node.md)
- [ingest-worker 接口详情](./ingest-worker.md)
- [janitor 接口详情](./janitor.md)
- [共享类型与存储层契约](./shared-types.md)

设计背景文档（`docs/`）：[总体架构](../docs/README.md) · [分发域](../docs/distribution/README.md) · [网络与准入](../docs/distribution/network.md) · [存储域](../docs/storage/README.md) · [入库域](../docs/ingest/README.md) · [策略控制](../docs/policy/README.md)
