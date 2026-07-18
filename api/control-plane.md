# control-plane（控制面）接口文档

入口：`cmd/control-plane/main.go`　配置：`configs/control-plane.yaml`（`internal/config/controlplane.go`）

## 1. 职责

控制面是整个系统的中枢，承担四项核心职能：

1. **节点鉴权与授权** —— 通过 HTTP 向边缘节点签发 Ed25519 签名的能力 JWT，使 P2P 网络内的互信无需中心侧逐次校验。
2. **DHT 引导** —— 运行公共 libp2p Kademlia DHT 引导节点（Server 模式），让边缘节点在私有命名空间内互相发现。
3. **Pin 编排** —— 依据节点状态上报（反向通道）与 PostgreSQL 中 24h 内容热度，用可插拔策略计算各节点的 pin/unpin 计划并周期再平衡。
4. **账号注册表** —— 管理云盘账号凭据（115 / 百度 / 夸克 / OneDrive / 阿里云盘），向所有在线节点推送凭据更新事件与周期全量快照。

另提供 PostgreSQL 元数据存储（content / blob / blob_location / 热度 / 账号健康）。

## 2. HTTP API

控制面 HTTP mux（`internal/controlplane/jwt/httpserver.go` 的 `JWTHTTPServer.Serve`）暴露三个路由：`POST /v1/node/jwt`、`GET /v1/blob-locations/{hash}`（T9）、`GET /metrics`（T20）。三者复用同一 `*http.ServeMux` 与同一监听端口——**无独立端口**（plan line 176/275 硬性约束）。

### 2.1 `POST /v1/node/jwt` —— 签发节点 JWT

注册位置：`internal/controlplane/jwt/httpserver.go:30`（Go 1.22+ `http.ServeMux` 方法路由）

**请求体**（`types.JWTRequest`，`internal/types/types.go:58`）：

```json
{
  "peer_id": "12D3KooW...",        // 节点 libp2p PeerID（base58）
  "signed_peer_id": "<base64>"      // 节点对 PeerID 字节的 Ed25519 签名
}
```

**处理流程**（`jwt/service.go:47-96`）：

1. 从 PeerID 提取 Ed25519 公钥，校验 `signed_peer_id` 签名（持有证明）。
2. 按来源 IP 限流（默认 1 次/小时，`X-Forwarded-For` 优先）。
3. 检查 L4 白名单，决定 `l4_backhaul` 能力。
4. 构造 payload 并签名：能力固定为 `edge=true, peer_icp=true, relay_provider=false`，带宽配额 50,000,000，有效期 1 小时。

**响应 200**（`types.JWTResponse`，`types.go:64`）：

```json
{
  "jwt": "base64url(hdr).base64url(payload).base64url(sig)",
  "refresh_before": 300
}
```

JWT payload（`types.NodeJWTPayload`）：`node_id`（UUID v4）、`peer_id`、`capabilities`、`bandwidth_quota`、`iat`、`exp`（iat+3600）。

**错误码**：

| 状态码 | 条件 | 响应体 |
|---|---|---|
| 400 | JSON 非法 / PeerID 非法 | `{"error":"jwt: invalid peer ID: ..."}` |
| 403 | Ed25519 签名校验失败 | `{"error":"jwt: invalid peer signature"}` |
| 405 | 非 POST 方法 | `{"error":"method not allowed"}` |
| 429 | 触发按 IP 限流 | `{"error":"jwt: rate limited"}` |
| 500 | 其他内部错误 | `{"error":"internal error"}` |

**中间件**：无。该服务没有日志/鉴权/CORS 中间件，防护仅靠限流 + 签名持有证明；HTTP 层本身无 API Key 或 mTLS（依赖部署层监听在非公开地址）。

### 2.2 `GET /v1/blob-locations/{hash}` —— blob 位置查询（T9）

注册位置：`internal/controlplane/jwt/httpserver.go::JWTHTTPServer.RegisterLocationHandler`，挂在与 `/v1/node/jwt` 同一 mux。Handler 实现：`internal/controlplane/locationsvc/handler.go::Handler`。

**路径参数**：`{hash}` —— blob 的 SHA-256 hex（经 Go 1.22+ `r.PathValue("hash")` 取值）。

**请求**：

| 头 | 必填 | 说明 |
|---|---|---|
| `Authorization: Bearer <jwt>` | 是 | 节点能力 JWT（`CapabilityJWT`），由 `POST /v1/node/jwt` 签发 |

**处理流程**（`handler.go::Handler.ServeHTTP`）：

1. 读 `Authorization` 头，校验 `Bearer ` 前缀。
2. `sjwt.VerifyJWTAnyPeerID(jwt, pubKey)` 验签 + 解析 payload（控制面 Ed25519 公钥注入构造器）。
3. 检查 `payload.Capabilities.Edge == true`（必须有 Edge 能力）。
4. `mc.GetBlobLocations(ctx, hash)` 查询 `blob_location` 表。
5. 非空 → 200 + `{"locations":[...]}`；空 → 404。

**响应 200**（JSON）：

```json
{
  "locations": [
    {"blob_hash":"sha256:...","backend_id":"baidu:acc-123","file_id":"..."}
  ]
}
```

`locations` 元素类型为 `types.BlobLocation{blob_hash, backend_id, file_id}`。空数组（`[]`）也合法，但当前实现倾向返回 404 表示"无存储位置"。

**错误码**：

| 状态码 | 条件 | 响应体 |
|---|---|---|
| 200 | 成功，且 `blob_location` 表中存在 ≥1 条记录 | `{"locations":[...]}` |
| 400 | 路径参数缺失（Go 1.22 mux 保证不命中） | `{"error":"missing hash path value"}` |
| 401 | `Authorization` 头缺失 / 非 `Bearer ` 前缀 / JWT 签名无效 / 过期 / PeerID 不匹配 | `{"error":"unauthorized"}` |
| 403 | JWT 合法但 `capabilities.edge != true` | `{"error":"forbidden: edge capability required"}` |
| 404 | `blob_location` 表无对应 hash 的记录 | `{"error":"not found"}` |
| 500 | 元数据查询返回错误（非 nil-DB 错误） | `{"error":"internal error"}` |
| 503 | `metadata.BlobStoreClient` 为 nil（控制面启动时 PG 不可达，`mc` 降级为 nil） | `{"error":"metadata unavailable"}` |

**鉴权**：**Bearer JWT + Edge 能力**。JWT 由控制面自身的 `/v1/node/jwt` 签发（同源信任），无需额外 API Key。注意 503 是确定性合约：无论 CP 的 PG 是否可用，edge 都能看到稳定的 HTTP 状态码，便于客户端区分"暂时不可用"与"配置错误"。

**注意事项**：

- 客户端（edge 侧 `HTTPLocationClient`，T10）将 404 归一化为空切片 + nil error（非重试可恢复状态），其他 4xx/5xx 视为错误。
- 单次请求一次调用——**无重试风暴**（plan line 189，位置查询位于热读路径）。
- 默认超时 5s（`defaultHTTPLocationTimeout`，T10 客户端侧）；服务端无显式超时，依赖 `http.Server` 的 `ReadTimeout`/`WriteTimeout`（T16，默认 10s）。
- **当前未在 edge-node main.go 装配**：`LocalDataPlane` 仍为 nil（T10 仅交付客户端构件，未触线）。生产装配是后续独立项；客户端构造闭包 `func() string { return string(jwtClient.CurrentJWT()) }` 已在 T10 集成测试中验证。

### 2.3 `GET /metrics` —— Prometheus 抓取（T20）

注册位置：`JWTHTTPServer.RegisterMetricsHandler(metrics *cpmetrics.Metrics)`（注：CP 专属计数器位于 `internal/controlplane/metrics/` 包，**不**导入 `internal/node/monitor`——T20-fix 恢复的隔离边界）。挂在与 `/v1/node/jwt` 同一 mux。

返回指标：

| 指标 | 类型 | 标签 | 说明 |
|---|---|---|---|
| `cp_jwt_issued_total` | Counter | `outcome` ∈ `success`/`invalid_peerid`/`invalid_signature`/`rate_limited`/`internal_error` | JWT 签发结果计数 |
| `cp_content_ingested_received_total` | Counter | — | `CONTENT_INGESTED` 事件接收计数 |
| `cp_pin_plan_dispatched_total` | Counter | — | PinPlan 下发计数 |

**鉴权**：无（部署层隔离）。

## 3. libp2p 控制通道 `/edge/control/1.0.0`

实现：`internal/controlplane/syncbroadcaster/syncbroadcaster.go`。这是控制面与节点间的**主通信通道**，非 gRPC——自定义 varint 长度前缀 JSON 协议。

**线格式**：`4 字节大端长度 | JSON(WireMessage)`，`WireMessage{ type, payload(json.RawMessage) }`。

| 事件 | 方向 | 载荷 | 触发时机 |
|---|---|---|---|
| `PIN_PLAN_UPDATE` | 控制面 → 节点 | `types.PinPlan` | 初始 pin（收到 CONTENT_INGESTED）、节点空间变化、周期再平衡 |
| `NODE_STATUS_REPORT` | 节点 → 控制面 | `types.NodeStatusReport` | 节点周期上报，驱动 PinOrchestrator |
| `CONTENT_INGESTED` | → 控制面 | `types.ContentIngestedEvent` | 新内容入库，触发初始 pin |
| `CREDENTIAL_UPDATE` | 控制面 → 全部节点 | `CredentialChangePayload` | 账号凭据变更时 |
| `ACCOUNT_SNAPSHOT` | 控制面 → 全部节点 | `[]AccountInfo` | 周期全量快照（默认约 60s） |

可靠性：`SnapshotStore` + 1000 条环形缓冲支持断线重连事件回放；每条消息有 `send_timeout`（默认 30s）。

## 4. 子模块明细

### 4.1 `jwt/` —— JWT 服务

| 文件 | 说明 |
|---|---|
| `service.go` | `JWTService`：身份校验、白名单、限流、构造并签发 JWT |
| `httpserver.go` | `JWTHTTPServer`：HTTP 层、IP 提取、错误映射 |
| `whitelist.go` / `whitelist_store.go` | L4 白名单内存集合 + BadgerDB 持久化（key: `w:<peerID>`） |
| `ratelimit.go` | 按 IP 的互斥锁区间限流器（默认 1 次/小时） |
| `auditlog.go` | JSON-lines 签发审计日志 |

### 4.2 `pinstrategy/` —— Pin 编排

- `PinOrchestrator`（`orchestrator.go`）：事件驱动。订阅 `CONTENT_INGESTED`（初始 pin）与 `NODE_STATUS_REPORT`（空间更新）；周期再平衡拉取 top-N 热门内容逐个调用策略 `AdjustPin`；经 `SendToNode` 下发 `PinPlan`。
- `DashPinStrategy`（`strategy.go`）：DASH 视频策略——`init` 角色 blob 始终 pin；`media` 角色按节点剩余空间分级（>50GB pin 5 个、>20GB pin 2 个、≤20GB 不 pin）。

### 4.3 `dhtbootstrap/` —— DHT 引导

`BootstrapHost`：公共 libp2p 主机（无 NAT 穿透），Server 模式 DHT，按命名空间 advertise，带周期重 advertise 心跳。

### 4.4 `metadata/` —— PG 元数据

`PGMetadataClient` 实现四类接口：`BlobStoreClient`（blob/位置 CRUD）、`ContentMetaClient`、`PopularityClient`（top-N / 24h 热度）、`MetadataWriter`（入库事务、账号健康）。内嵌 13 个 SQL 迁移文件，启动时按文件名序执行。表：`content`、`blob`、`content_blob`、`blob_location`（v2）、`content_popularity`、`cloud_account`、`account_health`。

### 4.5 `accountregistry/` —— 账号注册表

`AccountRegistry`：`cloud_account` 表的 CRUD（`CreateAccount` / `UpdateCredential` / `Revoke` / `ListByVendor`），变更时广播 `CREDENTIAL_UPDATE`，并周期广播全量 `ACCOUNT_SNAPSHOT`。

## 5. 配置项（`configs/control-plane.yaml`）

| YAML 路径 | 必填 | 默认 | 说明 |
|---|---|---|---|
| `identity.priv_key_path` | 是 | — | JWT 签名 Ed25519 私钥（PEM PKCS#8） |
| `identity.libp2p_priv_key_path` | 是 | — | libp2p 身份私钥（protobuf 格式） |
| `jwt_http.listen` | 是 | — | JWT HTTP 监听地址，如 `:8443` |
| `jwt_http.read_timeout` / `write_timeout` | 否 | `10s` | HTTP 超时（T16）；空 → 10s 默认，非法字符串 → 启动失败并指名字段 |
| `jwt_policy.ttl` / `refresh_before_seconds` / `bandwidth_quota_bytes` | 否 | `1h` / `300` / `50000000` | JWT 签发策略（T6）；覆盖默认 TTL/refresh/quota |
| `jwt_policy.default_capabilities.{edge,peer_icp,relay_provider}` | 否 | edge=true, peer_icp=true, relay=false | 默认能力集（T6）；`l4_backhaul` 仅白名单授权，不受此字段影响 |
| `l4_whitelist.db_path` | 否 | — | 白名单 BadgerDB 路径 |
| `pin_orchestrator.rebalance_interval` | 否 | — | 再平衡周期，如 `10m` |
| `pin_orchestrator.top_contents_limit` | 否 | `5000` | 再平衡 top-N（T16）；零/负值回退默认 5000 |
| `dht_bootstrap.listen_addrs` | 否 | `["/ip4/0.0.0.0/tcp/9001"]` | libp2p 监听 multiaddr |
| `dht_bootstrap.namespace` | 是 | — | DHT 发现命名空间，如 `edge` |
| `dht_bootstrap.advertise_ttl` / `advertise_interval` | 否 | `15m` / `5m` | advertise TTL 与心跳 |
| `dht_bootstrap.bootstrap_peers` | 否 | `[]` | 备用引导节点 |
| `metadata.pg_dsn` | 是 | — | PostgreSQL DSN |
| `sync_broadcaster.protocol_id` | 否 | `/edge/control/1.0.0` | 控制通道协议 ID（T16）；**改单侧会导致协议分裂**，CP/节点必须同步 |
| `sync_broadcaster.send_timeout` | 否 | `30s` | 单条发送超时（T16）；空 → 默认 30s |
| 环境变量 `LIBP2P_PSK` | 否 | — | 私网 32 字节 PSK（hex） |

## 6. 启动序列（`cmd/control-plane/main.go`）

加载配置 → 加载/生成 JWT 签名密钥 → 打开白名单 BadgerDB → 限流器/审计日志 → JWT HTTP 服务 → 加载 libp2p 身份 + 读 `LIBP2P_PSK` → DHT bootstrap → SyncBroadcaster → PG 客户端（失败时降级为 nil）→ PinOrchestrator（注册 `dash_video` 策略）→ 订阅反向通道 → 阻塞等待 SIGINT/SIGTERM → 5s 优雅退出。
