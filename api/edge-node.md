# edge-node（边缘节点）接口文档

入口：`cmd/edge-node/main.go`　配置：`configs/node-edge.yaml` / `configs/node-l4.yaml`（`internal/config/config.go`）

## 1. 职责

边缘节点是面向终端用户的内容分发组件：

- 通过 HTTP 按 SHA-256 哈希响应 blob 请求；
- 维护三级磁盘缓存（prefix/NVMe → warm/SSD → cold/HDD）；
- 参与私有 libp2p 网络：ICP 缓存协作、DHT 节点发现、GossipSub 热度同步；
- 接收控制面下发的 PinPlan，在 prefix 层驻留/驱逐指定 blob；
- 两种运行模式：**L4**（本地数据面可直接从云盘回源）与**非 L4**（仅缓存 + ICP + 向 L4 兄弟节点回退）；
- 回源管线逐级升级：本地缓存 → ICP 兄弟 → 数据面（L4）→ L4 流式回退（非 L4）；
- 安全：JWT 能力鉴权、PSK 私网准入、连接门控按 IP 限流。

## 2. HTTP API

边缘节点 HTTP mux（注册于 `cmd/edge-node/main.go`）暴露三个路由：`GET /blob/{hash}`、`GET /metrics`（T20，Prometheus 抓取，无鉴权）、`GET /healthz`（如有挂载）。请求由 `EdgeRouter.HandleBlobRequest` 统一分发（T11）：哈希环判定本节点是否为主节点——若是则走本地 `BackhaulManager`；否则经 libp2p `/edge/blob/get/1.0.0` 代理至主节点，代理失败自动回退本地 backhaul。

### `GET /blob/{hash}` —— 取 blob

| 项 | 值 |
|---|---|
| 路径参数 | `{hash}`：blob 的 SHA-256 hex |
| 查询参数 | 无 |
| 请求体 | 无 |
| 成功响应 | blob 原始字节流（未显式设置 Content-Type） |
| 失败响应 | `404 Not Found`，body `blob not found` |
| 超时 | 30s（`context.WithTimeout`） |

**行为**（经 EdgeRouter 路由后，若本节点为主或代理失败回退本地）：

- `access_layer.data_plane.enabled=true`（L4 模式）→ `BackhaulManager.HandleBlobL4()`：warm 缓存 → ICP 兄弟 → 本地数据面（云盘拉流）。
- `enabled=false`（非 L4 模式）→ `HandleBlobNoL4()`：warm 缓存 → ICP 兄弟 → `backhaulICPFetcher`（T12）向哈希环主节点经 `/edge/blob/get/1.0.0` 拉流，并写回本地 warmCache。
- 两条路径都使用 `singleflight` 合并同一 blob 的并发请求。

**鉴权/中间件**：无。HTTP 面当前无鉴权、无限流中间件。

### `GET /metrics` —— Prometheus 抓取（T20）

返回 `internal/node/monitor.Metrics` 暴露的 Prometheus 文本格式指标（缓存命中、TTFB、回源带宽、节点分数、JWT 失败计数等）。无鉴权，依赖部署层（内网/K8s Service）隔离。

> **路由能力接线状态（T11/T12/T20）**：`internal/node/routing/edge_router.go` 的 `EdgeRouter.HandleBlobRequest` **已挂 edge-node HTTP mux**（`cmd/edge-node/main.go`），通过哈希环判定主节点并代理转发，代理失败自动回退本地 backhaul；`backhaulICPFetcher` 经 `ring.Get` + `icp.FetchFromPeer` 向主节点拉流并写回本地 warmCache。`routing/scheduler.go`（DNS+302 区域调度模拟）仍为设计探索，未接入生产路径。`monitor.Metrics.HTTPHandler()` 已挂载为 `GET /metrics`（T20）。`peer_store.gc_interval` 已通过 `PeerEntryStore.StartValueLogGC` 接线（T15）。

## 3. libp2p 流协议

节点注册 5 个流协议处理器，这是节点间与节点↔控制面的全部内部通信。

### 3.1 `/edge/auth/1.0.0` —— JWT 鉴权握手

- 处理：`libp2phost.HandleAuth()`（`internal/node/libp2phost/gater.go:155`）；出站侧 `PresentAuth()`。
- 线格式：一行 = JWT 字符串 + `\n`。
- 行为：校验对端 JWT（Ed25519 签名、PeerID 绑定、有效期），通过后将该 peer 写入 `PeerEntryStore`，初始中立分 0。

### 3.2 `/edge/jwt-refresh/1.0.0` —— JWT 推送刷新

- 处理：`nodejwt.HandleJWTPush()`（`internal/node/jwt/protocol.go:88`）。
- 线格式：一行 JWT + `\n`。
- 去重规则：新 JWT 的 `exp` 小于等于已存记录则拒绝（过期/重复），更大则更新。

### 3.3 `/edge/blob/head/1.0.0` —— ICP 存在性探测

- 处理：`icp.HandleBlobHead()`（`internal/node/icp/protocol.go:142`）；客户端 `FetchFromPeerHead()`（10ms 超时）。
- 线格式：客户端发 varint 前缀的 blob 哈希；服务端回 1 字节 `0x01`（HIT）/ `0x00`（MISS）。

### 3.4 `/edge/blob/get/1.0.0` —— ICP 拉流

- 处理：`icp.HandleBlobGet()`（`protocol.go:169`）；客户端 `FetchFromPeerGet()` / 组合 `FetchFromPeer()`。
- 线格式：客户端发 varint 前缀哈希；服务端直接流式回写原始字节（无长度前缀，读到 EOF）。出错时 reset 流（客户端表现为流错误而非干净 EOF）。

### 3.5 `/edge/control/1.0.0` —— 控制面通道（节点侧）

- 处理：`nodesync.Client.handleStream()`（`internal/node/syncbroadcaster/client.go:103`）。
- 线格式：varint 前缀 JSON `WireMessage{type, payload}`。
- 行为：解码 `PIN_PLAN_UPDATE` → `pinstrategy.HandlePinPlan()` → `PinStore.ApplyPin/ApplyUnpin`；其余事件转发 `OnEvent` 回调。反向经 `SendToControlPlane()` 上报 `NodeStatusReport`。

### 3.6 GossipSub `edge-popularity-v1` —— 热度同步

- 发布：每 30s 快照本地 6 分钟滑窗请求计数，Ed25519 签名后发布（`gossippop.PublishPopularity()`）。
- 消息：`PopularityUpdate{ peer_id, timestamp, counts: map[blob]int64, sig }`。
- 接收：验签 + 检查来源节点分数（≤ 灰名单阈值 -20 丢弃），按权重合并进 `MergedPopularity`。

## 4. 回源数据流

```
GET /blob/{hash}
    │
    EdgeRouter.HandleBlobRequest (T11)
    │  ├─ ring.Get(hash) == self (or ring empty) → 走本地 backhaul
    │  └─ 否则：proxyToPeer(/edge/blob/get/1.0.0)
    │            └─ 失败回退本地 backhaul（slog.Warn + serveAsPrimary）
    ▼
本地 BackhaulManager
    ├─ L4 模式 → HandleBlobL4
    │      ├─ WarmCache.Get
    │      ├─ ICP: /edge/blob/head → /edge/blob/get（兄弟节点）
    │      └─ DataPlane.FetchBlobLocal（位置查询→选账号→取链→云盘拉流）
    │
    └─ 非 L4 模式 → HandleBlobNoL4
           ├─ WarmCache.Get
           ├─ ICP（兄弟节点）
           └─ backhaulICPFetcher.FetchFromL4Node (T12)
                  → ring.Get(hash) → icp.FetchFromPeer(主节点)
                  → 写回本地 warmCache（backhaulWarmCache.Put）
```

## 5. 缓存与淘汰

| 层 | 实现 | 介质 | 策略 |
|---|---|---|---|
| L1 内存索引 | `cache/index.go` `MemoryIndex` | RAM | 快查 |
| L2 prefix | `cache/prefix.go` `PrefixCache` | NVMe | pin 驻留，不做 LRU 淘汰 |
| L3 warm | `cache/warm.go` `WarmCache` | SSD | 内容感知 LRU：按视频热度升序、同视频内按码率降序淘汰，跳过 pin 与高延迟分片（`cache/evict.go`） |
| L4 cold | `cache/cold.go` `ColdCache` | HDD | 简单时间戳 LRU（仅 L4 节点配置） |

## 6. 连接门控与准入（`libp2phost/gater.go`）

- `InterceptAccept`：CIDR 白名单 + 按 IP 连接速率限制（默认 50 _conn/s_）。
- `InterceptSecured`：检查 peer 的 `stale` 标记与分数（< 灰名单 -20 拒绝）。
- `InterceptUpgraded`：JWT 过期检查（无宽限期），过期即标记 stale。
- 准入三要素：PSK 私网 → 门控 → `/edge/auth/1.0.0` JWT 握手。

## 7. 配置项（摘要）

完整结构见 `internal/config/config.go` 与示例 `configs/node-edge.yaml` / `configs/node-l4.yaml`。

| YAML 路径 | 说明 |
|---|---|
| `node.identity.priv_key_path` | **必填**，libp2p 身份私钥（protobuf） |
| `node.declared_capabilities.{edge,l4_backhaul,relay_provider,peer_icp}` | 申请的能力声明 |
| `node.libp2p.listen` | multiaddr 列表，如 `/ip4/0.0.0.0/tcp/9001` + QUIC |
| `node.libp2p.private_network.{enabled,force_pnet_env}` | PSK 私网开关 |
| `node.libp2p.dht.{mode,namespace,advertise_ttl,bootstrap_peers}` | DHT 发现 |
| `node.libp2p.nat_traversal.{autonat,auto_relay,dcutr}` | NAT 穿透 |
| `node.libp2p.peer_store.{path,gc_interval}` | peerstore BadgerDB。`gc_interval` 已接线（T15）：`PeerEntryStore.StartValueLogGC(ctx, interval)` 周期调 `badger.RunValueLogGC(0.5)`；默认 1h，零/负值 no-op |
| `node.libp2p.conn_gater.{ip_rate_limit,cidr_allowlist}` | 门控 |
| `node.jwt_service.endpoint` | **必填**，控制面 JWT HTTP 地址 |
| `edge.{prefix,warm,cold}_cache.{enabled,path,size_gb}` | 三级缓存 |
| `access_layer.data_plane.{enabled,drivers,link_pool.max_entries,rate_limit_local}` | L4 数据面 |
| `access_layer.{fetch_segment_server,fetch_segment_client}.enabled` | 兄弟节点分段取流开关 |
| `access_layer.vendor_profiles.<vendor>.{weight,base_latency_ms,bandwidth_mbps}` | 厂商画像 |
| `access_layer.rate_limits.<vendor>.{qps,burst,concurrent}` | 厂商限流 |
| `access_layer.cloud_accounts[]` | 云盘账号（vendor/account_id/client_id/client_secret/refresh_token/region/enabled） |
| `hash_ring.replicas` | 哈希环虚拟节点数，默认 150 |

**环境变量**：`CONTROL_PLANE_PUBKEY`（必填，控制面 Ed25519 公钥 hex）；`LIBP2P_PSK`（私网 PSK，`force_pnet_env` 时必填）。

**命令行**：`-config`（默认 `configs/node-edge.yaml`）、`-version`。

## 8. 后台循环

| 循环 | 周期 | 说明 |
|---|---|---|
| DHT heartbeat / discover | `advertise_interval`（T15；缺省 `advertise_ttl/2`，30s 下限） | 重新 advertise + FindPeers |
| 哈希环重建 | 变更触发（防抖 1s，最长 5s） | 依据 `PeerEntryStore.ActivePeers()` |
| GossipSub 热度发布 | 30s | 签名快照本地热度 |
| GossipSub 热度订阅 | 常驻（T18） | `sub.Next(rootCtx)` → `HandlePopularityMessage` → 合并入 `MergedPopularity` → 经 `WarmCache.SetPopSource` 驱动缓存淘汰（`MinTrustedWeight` 过滤） |
| JWT 刷新 | `min(refreshInterval, exp-now-refreshBeforeExpiry)`（T7） | `runJWTRefreshLoop`：失败 `slog.Error` 不退出，ctx 取消退出 |
| peerstore BadgerDB VLog GC | `peer_store.gc_interval`（T15，默认 1h） | `PeerEntryStore.StartValueLogGC` → `badger.RunValueLogGC(0.5)`；零/负值 no-op |
| pin 异步取流 | 每个 pin 操作 | warm → prefix 分区搬运 |
