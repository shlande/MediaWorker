# MediaWorker P2P 协议契约

本文档描述 MediaWorker 的 P2P 层协议契约（非 HTTP），HTTP API 契约见 api/ 目录 Swagger 文档。

## 1. libp2p 流协议总表（节点间 / 控制面 ↔ 节点）

| 协议 ID | 方向 | 功能 |
|---|---|---|
| `/edge/auth/1.0.0` | 节点 → 节点 | 连接建立后出示/校验 JWT 握手 |
| `/edge/jwt-refresh/1.0.0` | 节点 → 节点 | 推送更新的 JWT（按 Exp 去重） |
| `/edge/blob/head/1.0.0` | 节点 → 节点 | ICP 存在性探测（0x01/0x00） |
| `/edge/blob/get/1.0.0` | 节点 → 节点 | ICP 拉取 blob 原始字节流 |
| `/edge/control/1.0.0` | 双向 | 控制面下发 PinPlan/凭据/快照；节点上报状态/入库事件 |

## 2. 流协议明细（节点侧实现）

节点注册 5 个流协议处理器，这是节点间与节点↔控制面的全部内部通信。

### 2.1 `/edge/auth/1.0.0` —— JWT 鉴权握手

- 处理：`libp2phost.HandleAuth()`（`internal/node/libp2phost/gater.go:155`）；出站侧 `PresentAuth()`。
- 线格式：一行 = JWT 字符串 + `\n`。
- 行为：校验对端 JWT（Ed25519 签名、PeerID 绑定、有效期），通过后将该 peer 写入 `PeerEntryStore`，初始中立分 0。

### 2.2 `/edge/jwt-refresh/1.0.0` —— JWT 推送刷新

- 处理：`nodejwt.HandleJWTPush()`（`internal/node/jwt/protocol.go:88`）。
- 线格式：一行 JWT + `\n`。
- 去重规则：新 JWT 的 `exp` 小于等于已存记录则拒绝（过期/重复），更大则更新。

### 2.3 `/edge/blob/head/1.0.0` —— ICP 存在性探测

- 处理：`icp.HandleBlobHead()`（`internal/node/icp/protocol.go:142`）；客户端 `FetchFromPeerHead()`（10ms 超时）。
- 线格式：客户端发 varint 前缀的 blob 哈希；服务端回 1 字节 `0x01`（HIT）/ `0x00`（MISS）。

### 2.4 `/edge/blob/get/1.0.0` —— ICP 拉流

- 处理：`icp.HandleBlobGet()`（`protocol.go:169`）；客户端 `FetchFromPeerGet()` / 组合 `FetchFromPeer()`。
- 线格式：客户端发 varint 前缀哈希；服务端直接流式回写原始字节（无长度前缀，读到 EOF）。出错时 reset 流（客户端表现为流错误而非干净 EOF）。

### 2.5 `/edge/control/1.0.0` —— 控制面通道（节点侧）

- 处理：`nodesync.Client.handleStream()`（`internal/node/syncbroadcaster/client.go:103`）。
- 线格式：varint 前缀 JSON `WireMessage{type, payload}`。
- 行为：解码 `PIN_PLAN_UPDATE` → `pinstrategy.HandlePinPlan()` → `PinStore.ApplyPin/ApplyUnpin`；其余事件转发 `OnEvent` 回调。反向经 `SendToControlPlane()` 上报 `NodeStatusReport`。

## 3. 控制通道 `/edge/control/1.0.0`（控制面侧）

实现：`internal/controlplane/syncbroadcaster/syncbroadcaster.go`。这是控制面与节点间的**主通信通道**，非 gRPC——自定义 varint 长度前缀 JSON 协议。

**线格式**：`4 字节大端长度 | JSON(WireMessage)`，`WireMessage{ type, payload(json.RawMessage) }`。

| 事件 | 方向 | 载荷类型 | 说明 | 触发时机 |
|---|---|---|---|---|
| `PIN_PLAN_UPDATE` | 控制面 → 节点 | `types.PinPlan` | pin/unpin 指令 | 初始 pin（收到 CONTENT_INGESTED）、节点空间变化、周期再平衡 |
| `NODE_STATUS_REPORT` | 节点 → 控制面 | `types.NodeStatusReport` | 空间与健康状态 | 节点周期上报，驱动 PinOrchestrator |
| `CONTENT_INGESTED` | 节点/入库 → 控制面 | `types.ContentIngestedEvent` | 新内容通知（触发初始 pin） | 新内容入库，触发初始 pin |
| `CREDENTIAL_UPDATE` | 控制面 → 全部节点 | `CredentialChangePayload` | 云盘凭据轮换 | 账号凭据变更时 |
| `ACCOUNT_SNAPSHOT` | 控制面 → 全部节点 | `[]AccountInfo` | 账号全量快照（约 60s 周期） | 周期全量快照（默认约 60s） |

可靠性：`SnapshotStore` + 1000 条环形缓冲支持断线重连事件回放；每条消息有 `send_timeout`（默认 30s）。

## 4. GossipSub 主题

| 主题 | 发布者 | 内容 |
|---|---|---|
| `edge-popularity-v1` | 每个 edge-node（30s 周期） | 签名后的本地 blob 请求热度计数，供全网合并 |

### 4.1 `edge-popularity-v1` —— 热度同步

- 发布：每 30s 快照本地 6 分钟滑窗请求计数，Ed25519 签名后发布（`gossippop.PublishPopularity()`）。
- 消息：`PopularityUpdate{ peer_id, timestamp, counts: map[blob]int64, sig }`。
- 接收：验签 + 检查来源节点分数（≤ 灰名单阈值 -20 丢弃），按权重合并进 `MergedPopularity`。
