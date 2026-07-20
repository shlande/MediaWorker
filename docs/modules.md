# MediaWorker 系统模块与功能对照

> 本文档基于源码实际实现整理（以 `internal/` 与 `cmd/` 下的代码为准），用于说明系统模块划分与各模块功能。HTTP API 契约见 `api/` 目录 Swagger 文档；架构设计背景与取舍见 `docs/README.md` 及 `docs/` 下领域文档。

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
