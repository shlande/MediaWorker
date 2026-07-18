# 分发域 — 网络与准入层

本文档覆盖分发域的**网络基础设施**：节点身份、JWT 准入、libp2p 节点发现、ConnectionGater、GossipSub 评分、NAT 穿透。

缓存策略、回源、动态 Pin、路由调度、容灾等见 [README.md](README.md)。

## 目录

1. [共享类型：节点身份与能力](#1-共享类型节点身份与能力)
2. [节点身份与能力配置](#2-节点身份与能力配置)
   - [2.1 节点身份：libp2p PeerId](#21-节点身份libp2p-peerid)
   - [2.2 能力模型与 JWT](#22-能力模型与-jwt)
   - [2.3 节点配置文件](#23-节点配置文件)
   - [2.4 能力组合与部署场景](#24-能力组合与部署场景)
   - [2.5 节点发现流程总览](#25-节点发现流程总览)
3. [节点间通信与连接门控](#3-节点间通信与连接门控)
   - [3.1 兄弟节点拉取流程（libp2p stream）](#31-兄弟节点拉取流程libp2p-stream)
   - [3.2 ConnectionGater：连接层资源保护与评分隔离](#32-connectiongater连接层资源保护与评分隔离)
4. [本地流行度 GossipSub（带评分的不可信同步）](#4-本地流行度-gossipsub带评分的不可信同步)
   - [4.1 GossipSub topic 与消息格式](#41-gossipsub-topic-与消息格式)
   - [4.2 带评分的合并策略](#42-带评分的合并策略)
   - [4.3 PeerScorer：GossipSub AppSpecificScore](#43-peerscorergossipsub-appspecificscore)
   - [4.4 热度数据模型（分发域所有）](#44-热度数据模型分发域所有)
5. [路由策略与控制面职责](#5-路由策略与控制面职责)
   - [5.1 设计原则：控制面退化为入口](#51-设计原则控制面退化为入口)
   - [5.2 节点发现：PSK 私有网络 + DHT + GossipSub PeX](#52-节点发现psk-私有网络--dht--gossipsub-pex)
   - [5.3 pin/unpin 下发：fire-and-forget](#53-pinunpin-下发fire-and-forget)
   - [5.4 控制面需要的节点信息](#54-控制面需要的节点信息)
   - [5.5 策略层决策：按需查询](#55-策略层决策按需查询)
   - [5.6 数据量对比](#56-数据量对比)
6. [负载均衡与 NAT 穿透](#6-负载均衡与-nat-穿透)
   - [6.1 非 L4 节点到 L4 节点的负载均衡](#61-非-l4-节点到-l4-节点的负载均衡)
   - [6.2 NAT 穿透栈（社区节点硬需求）](#62-nat-穿透栈社区节点硬需求)

---


## 1. 共享类型：节点身份与能力

### 1.0 节点身份与能力（libp2p 准入模型）

节点不再使用离散角色枚举（`L2_L4` / `L2_ONLY`），改为**能力模型**：每个节点声明一组 capabilities，由控制面签发的 JWT 授权。所有节点（自建 + 社区）统一走 PeerId + JWT 准入。

```go
// ─── 节点身份 (libp2p PeerId 绑定) ───

// PeerId: 节点的密码学身份, 由 Ed25519 公钥的 multihash 派生
// 格式: base58(multihash(pubkey))
// 全局唯一, 不可伪造; 节点重启/迁移时携带私钥即保持同一身份
type PeerId string

// NodeCapabilities: 节点声明的能力集合
// 由 JWT 授权, 控制面签发时勾选哪些能力被允许
type NodeCapabilities struct {
    Edge           bool   // 边缘分发 (HTTP/gRPC 服务客户端). 默认 true
    L4Backhaul     bool   // 网盘回源 (本地数据面 + 接触网盘凭证). 限白名单 PeerId
    RelayProvider  bool   // 作为 Circuit Relay v2 中继节点 (需公网 IP). 自建公网节点默认 true
    PeerICP        bool   // 作为兄弟节点提供缓存互助. 默认 true
}

// CapabilityJWT: 控制面签发的节点准入凭证 (JWT 格式)
// 节点向控制面 JWT 服务请求签发时获取, 节点持有并出示给其他节点
// 其他节点本地自验证 JWT 签名 (不回问 bootstrap), 确认 PeerId + capabilities + 有效期
//
// JWT 结构: header.payload.signature
//   header:  {"alg": "EdDSA", "typ": "JWT"}
//   payload: NodeJWTPayload (下方定义)
//   signature: Ed25519(payload, 控制面私钥)
//
// 关键特性:
//   - 自验证: 任何持有控制面公钥的节点都能本地验证, 不回问 bootstrap
//   - 有时效: exp 字段强制过期, 过期且未续期 → PeerStore 标记 Stale → 拒绝通信
//   - 不可伪造: 签名用控制面私钥, 节点无法自行签发或篡改
//   - 不可吊销: JWT 在 exp 前无法撤销 (与 L4 白名单的协同见 §2.2)
type CapabilityJWT string  // base64url(header).base64url(payload).base64url(sig)

type NodeJWTPayload struct {
    NodeID       string           // 控制面分配的逻辑节点 ID (与 PeerId 解耦, 用于运维追踪)
    PeerID       PeerId           // 绑定的 libp2p PeerId (防止 JWT 被冒用)
    Capabilities NodeCapabilities // 允许的能力 (控制面按信任度勾选)
    BandwidthQuota int64          // 出口带宽配额 (bytes/sec, 0 = 不限)
    Iat          int64            // 签发时间 (unix timestamp)
    Exp          int64            // 过期时间 (unix timestamp, 默认 Iat + 1h)
}

// JWT 有效期设计:
//   - Exp - Iat = 1h (短有效期, 降低泄露风险)
//   - 节点每 5min 向 JWT 服务请求续签 → 获得新 JWT (刷新)
//   - 节点通过 IdentifyPush / GossipSub PeX 广播新 JWT 给已连接的 peer
//   - JWT 过期 + 5min 内未收到新 JWT → PeerStore 标记 Stale → 哈希环剔除 + 拒绝通信

// ─── 私有网络准入 (PSK) ───

// PSK (Pre-Shared Key): 编译时注入, 所有主网节点共享同一个 PSK
// 启用 libp2p.PrivateNetwork(psk) 后, 所有连接在握手阶段必须交换并验证 PSK
// PSK 不匹配的连接在 TCP/QUIC 层被拒绝, 不进入 libp2p 安全层 (Noise/TLS)
// 配合 LIBP2P_FORCE_PNET=1 环境变量作为护栏: 未配置 PSK 时 libp2p 拒绝启动所有连接
//
// PSK 通过 go build -ldflags "-X main.psk=<hex>" 编译时注入:
//   go build -ldflags "-X main.psk=$(cat psk.hex)" -o edge-node ./cmd/edge-node
// PSK 轮换需要重新编译和重新分发二进制 (无过期机制, 静态密钥)
type PSK []byte  // 32 字节随机密钥, hex 编译时注入

// ─── DHT 发现 (见 §5.2) ───

// 私有 DHT 网络: 不连接公共 IPFS bootstrap, 仅用自建 bootstrap 节点
// DHT Advertise/FindPeers 使用固定 namespace "edge" 作为查找约定 (不承担安全职责, 安全由 PSK 保证)
// namespace 经 SHA256 → CID, 在 DHT 上 Provide/FindProviders
const DiscoveryNamespace = "edge"

// DHTBootstrapPeer: DHT 引导节点配置 (纯路由引导, 不兼承担认证)
// bootstrap 节点 = 控制面运行的 3-5 个公网稳定节点, 持有同一个 PSK
type DHTBootstrapPeer struct {
    PeerID PeerId
    Addrs  []string  // multiaddr 列表 (含 /p2p/ 后缀)
}

// ─── JWT 签发 (独立服务, 见 §5.2) ───

// JWTRequest: 节点向控制面 JWT 服务请求签发/续签 JWT
// 独立于 DHT 发现, 通过 HTTP/gRPC 端点请求 (POST /v1/node/jwt)
type JWTRequest struct {
    PeerID       PeerId  // 节点身份 (用于绑定 JWT)
    SignedPeerID []byte  // 节点用 Ed25519 私钥对 PeerID 的签名 (证明私钥持有)
}

// JWTResponse: 控制面签发的 JWT + 续签信息
type JWTResponse struct {
    JWT            CapabilityJWT  // 控制面签发的 JWT (1h 有效期)
    RefreshBefore  time.Duration  // 过期前多久开始续签 (默认 5min before exp, 无宽限期)
}

// PeerStoreEntry: 节点本地持久化的 peer 信息 (BadgerDB, 重启可恢复)
// 哈希环 (§5.2) 从 PeerStore 重建, 不依赖控制面广播
// ★ 写入/更新 PeerStore 前必须验证对方 JWT (§3.2 InterceptSecured)
type PeerStoreEntry struct {
    PeerID       PeerId
    Addrs        []string         // 最近一次 IdentifyPush 收到的地址
    JWT          CapabilityJWT    // 对方出示的 JWT (用于有效期检查)
    Capabilities NodeCapabilities // 从 JWT payload 解析
    JWTExp       int64            // JWT 过期时间 (从 payload.Exp 解析, 用于快速检查)
    LastSeen     int64            // 最近一次 JWT 续签或 GossipSub 心跳时间
    Score        float64          // GossipSub 评分 (§4.3), 低于 GraylistThreshold 时被屏蔽
    Stale        bool             // JWT 过期且未续期, 或长期无心跳
}
```

**JWT 与 L4 白名单的协同**：
- JWT 的 `L4Backhaul` 字段在签发时从 L4 白名单查询，白名单变更后下次 JWT 续签时自动生效
- JWT 1h 有效期意味着白名单变更最长 1h 生效（旧 JWT 过期后拒绝 L4 操作）
- 紧急吊销：通过 SyncBroadcaster 广播 `L4_REVOKE` 指令，节点立即将对应 PeerId 的 PeerStore 标记 Stale（不等 JWT 过期）

---

## 2. 节点身份与能力配置

同一份二进制，按配置 + 控制面签发的 JWT 决定节点实际承担的能力。**节点不再有离散角色枚举**——`L2_L4` / `L2_ONLY` 的二分法被能力集合取代。背景：L2 节点可能由社区运营，节点能力应能动态声明与升级，而非出厂固定。

### 2.1 节点身份：libp2p PeerId

每个节点启动时持有 Ed25519 私钥（持久化在本地，迁移时随节点带走），公钥派生出 libp2p PeerId 作为全局唯一身份。

```go
// 节点启动时加载/生成身份
type NodeIdentity struct {
    PrivKey crypto.PrivKey  // Ed25519, 持久化在 /data/identity/ed25519.key (0600)
    PeerID  PeerId          // 由 PrivKey.GetPublic() 派生
}

func LoadOrGenerateIdentity(keyPath string) (*NodeIdentity, error) {
    // 1. 尝试加载已有私钥
    // 2. 不存在则生成新 Ed25519 密钥对, 写入 keyPath
    // 3. 派生 PeerId
    // 私钥丢失 = 节点身份丢失, 需重新向 JWT 服务请求签发新 JWT (绑定新 PeerId)
}
```

**身份与运维的关系**：
- 同一台机器重启 → 同一 PeerId（私钥从磁盘加载）
- 机器迁移/重装 → 私钥随数据盘带走则同一 PeerId，否则视为新节点
- 控制面签发的 JWT 与 PeerId 绑定，**冒名顶替在 libp2p 安全握手层就被拦截**（详见 §3.2 ConnectionGater）

### 2.2 能力模型与 JWT

节点声明一组能力（见 §1.0 `NodeCapabilities`），控制面在 JWT 签发时勾选哪些能力被授权。**节点声明 ≠ 节点被授权**——但授权策略**按能力分层**，不同能力的签发门槛不同：

| 能力 | 签发策略 | 触发条件 | 启用后影响 |
|------|---------|---------|----------|
| `Edge` + `PeerICP` | **自动签发**，无审核 | 节点申请即得（限速防刷） | 服务客户端 + 进入哈希环，可被查询/拉取 |
| `RelayProvider` | **自动探测**，公网可达即签发 | 控制面定期探测节点监听端口公网可达 | 承担社区节点的 NAT 中转流量（见 §6.2） |
| `L4Backhaul` | **人工认证**，白名单审核 | 运营者向控制面提交认证，审核通过后 PeerId 入白名单 | 节点 miss 时走本地回源 + 接触网盘凭证 |

**纯边缘节点的自动注册**：社区节点只需 `Edge + PeerICP`，可**完全自动注册**——节点启动后连接 DHT bootstrap peer（PSK 握手通过），然后向控制面 JWT 服务发起请求（`POST /v1/node/jwt`，用 Ed25519 私钥签名 PeerID 证明身份），控制面校验签名 + 限速后，**直接签发并返回 JWT**（`L4Backhaul=false`, `BandwidthQuota` 受限, 1h 有效期）。JWT 签发是独立步骤，与 DHT 发现解耦。

**L4 能力的人工认证**：`L4Backhaul` = 接触网盘账号 cookie/API token。由于主流网盘（115/百度/夸克/阿里云盘）的凭证体系**不支持细粒度权限委派**——cookie 即全部权限，下发后无法回收，恶意节点可在 5min TTL 内导出转卖。因此 L4 必须**人工认证**：运营者向控制面提交认证材料（`POST /v1/node/l4-certify`）→ 控制面管理员审核 → PeerId 加入 L4 白名单 → 节点下次 JWT 续签时，控制面签发的 JWT 勾选 `L4Backhaul=true` → SyncBroadcaster 开始向该节点下发网盘凭证。

**L4 凭证安全**：控制面 SyncBroadcaster 下发凭证时，**只对 `L4Backhaul=true` 的 PeerId 下发**。社区节点默认不持有此能力，无法接触凭证。L4 白名单的吊销 = 节点下次 JWT 续签时控制面签发 `L4Backhaul=false` 的 JWT + 立即停止下发新凭证 + 通知该节点销毁本地凭证缓存（节点拒绝则评分降级 + 哈希环剔除）。

**JWT 刷新机制**：
- JWT 有效期 1h（短有效期，降低泄露风险）
- 节点在 JWT 过期前 5min 向控制面 JWT 服务请求续签 → 获得新 JWT → 节点持有新 JWT
- 续签失败时立即重试（指数退避：1s, 2s, 4s... 上限 30s），不等过期
- 节点通过 libp2p IdentifyPush 将新 JWT 推送给已连接的 peer → peer 更新 PeerStore 中的 JWT + JWTExp
- JWT 过期（`Exp` 到达）且未续签成功 → PeerStore 立即标记 Stale → 哈希环剔除 + 拒绝通信（**无宽限期**）
- 提前续签设计：5min 续签窗口足够覆盖重试 + 网络抖动，确保续签在过期前完成；过期即降级保证安全边界清晰

**DHT bootstrap + JWT 签发（独立步骤）**：

节点分两步完成网络加入：先连接 DHT bootstrap peer（纯路由引导），再向控制面 JWT 服务请求 JWT（独立认证）。发现与认证解耦，各自可独立演进。

```go
// L4 人工认证端点 (独立, 需管理员审批, 唯一保留的 HTTP 端点)
// POST /v1/node/l4-certify  (运营者提交认证材料)
// 审批通过后, 控制面将该 PeerId 加入 L4 白名单, 下次 JWT 续签时 JWT 自动勾选 L4Backhaul

// JWT 签发端点 (独立, 节点启动时 + 每 5min 续签时调用)
// POST /v1/node/jwt  (节点用 Ed25519 私钥签名 PeerID 证明身份)

// RelayProvider 自动探测 (控制面定期任务)
func (s *ControlPlane) autoPromoteRelayProvider(ctx context.Context) {
    for _, node := range s.registeredNodes() {
        if node.Capabilities.RelayProvider { continue }
        // 探测: 能否从公网反向连接该节点的 libp2p 监听端口
        if reachable, _ := s.probePublicReachability(ctx, node.PeerID, node.Addrs); reachable {
            s.reissueToken(node, func(c *NodeCapabilities) { c.RelayProvider = true })
        }
    }
}
```

### 2.3 节点配置文件

```yaml
# node-config.yaml -- 启用 L4 回源能力的自建节点示例
node:
  identity:
    priv_key_path: "/data/identity/ed25519.key"   # 持久化, 迁移时带走
   
  # 节点声明的能力 (实际是否授权取决于控制面签发的 JWT)
  declared_capabilities:
    edge: true
    l4_backhaul: true       # 声明 L4, 需 PeerId 在白名单才生效
    relay_provider: true    # 公网节点可作 relay 中继
    peer_icp: true

  # libp2p 主机配置
  libp2p:
    listen:
      - "/ip4/0.0.0.0/tcp/9001"        # 公网可达 (relay_provider 必须公网)
      - "/ip4/0.0.0.0/udp/9001/quic"   # QUIC, 推荐 (DCUtR 打洞成功率更高)
    # ★ PSK 私有网络准入 (编译时注入, 此处仅声明启用)
    # 编译: go build -ldflags "-X main.psk=$(cat psk.hex)"
    # 运行时设 LIBP2P_FORCE_PNET=1 作为护栏 (未配置 PSK 时拒绝启动)
    private_network:
      enabled: true                    # 等价于 libp2p.PrivateNetwork(psk)
      force_pnet_env: true             # 设置 LIBP2P_FORCE_PNET=1

    # ★ DHT 配置 (私有 DHT, 不连公共 IPFS bootstrap)
    dht:
      mode: server                     # 公网节点用 ModeServer (参与 DHT 路由)
      namespace: "edge"                # 固定查找约定 (安全由 PSK 保证, 非隔离用途)
      advertise_ttl: 15m               # DHT Advertise TTL
      advertise_interval: 5m           # re-Advertise 心跳频率
      bootstrap_peers:                 # 自建 DHT bootstrap 节点 (3-5 个, 控制面运行)
        - "/dnsaddr/dht-bootstrap-01.example.com/tcp/9001/p2p/QmBootstrap01"
        - "/dnsaddr/dht-bootstrap-02.example.com/tcp/9001/p2p/QmBootstrap02"
        - "/dnsaddr/dht-bootstrap-03.example.com/tcp/9001/p2p/QmBootstrap03"
      # ★ 不使用 dht.DefaultBootstrapPeers (公共 IPFS 节点)

    nat_traversal:
      autonat: true                     # 自动探测 NAT 状态
      auto_relay: true                  # NAT 后自动预约 relay
      dcutr: true                       # 经 relay 协调打洞
    peer_store:
      path: "/data/identity/peerstore.db"  # BadgerDB, 重启可恢复
      gc_interval: 1h                   # 清理 Stale 条目
    conn_gater:
      ip_rate_limit: 50                 # 单 IP 每秒最大连接数
      cidr_allowlist: []                # 可选, 限制来源 CIDR

  # ★ JWT 签发服务配置 (独立于 DHT 发现)
  jwt_service:
    endpoint: "https://control-plane.example.com/v1/node/jwt"  # 控制面 JWT 签发端点
    refresh_interval: 5m               # JWT 续签频率
    refresh_before_expiry: 5m          # JWT 过期前多久开始续签

edge:
  prefix_cache: { enabled: true, path: "/data/prefix", size_gb: 2000 }
  warm_cache:   { enabled: true, path: "/data/warm",   size_gb: 50000 }
  cold_cache:   { enabled: true, path: "/data/cold",   size_gb: 100000 }

access_layer:
  data_plane:
    enabled: true           # ★ 启用数据面 = 节点能本地回源 (需 JWT 授权 L4Backhaul)
    # 若 JWT 未授权 L4Backhaul, 此处 enabled=true 也无法实际工作:
    # SyncBroadcaster 不下发网盘凭证, Driver 实例化失败
    subscribe_control: true
    drivers: ["115", "baidu", "quark", "onedrive", "aliyundrive"]
    link_pool: { max_entries: 10000 }
    rate_limit_local: true
  fetch_segment_server:
    enabled: true           # 暴露 libp2p stream FetchSegment 给兄弟节点
```

```yaml
# node-config.yaml -- 纯边缘社区节点示例 (无 L4, 可能在 NAT 后)
node:
  identity:
    priv_key_path: "/data/identity/ed25519.key"
   
  declared_capabilities:
    edge: true
    l4_backhaul: false      # 不申请 L4
    relay_provider: false   # NAT 后无法作 relay
    peer_icp: true

  libp2p:
    listen:
      - "/ip4/0.0.0.0/tcp/9001"
      - "/ip4/0.0.0.0/udp/9001/quic"
    # ★ PSK 私有网络准入 (编译时注入, 与 L4 节点同一 PSK)
    private_network:
      enabled: true
      force_pnet_env: true
    # ★ DHT 配置 (私有 DHT)
    dht:
      mode: client                     # NAT 后节点用 ModeClient (不承担 DHT 路由)
      namespace: "edge"                # 固定查找约定
      advertise_ttl: 15m
      advertise_interval: 5m
      bootstrap_peers:                 # 同一组自建 DHT bootstrap 节点
        - "/dnsaddr/dht-bootstrap-01.example.com/tcp/9001/p2p/QmBootstrap01"
        - "/dnsaddr/dht-bootstrap-02.example.com/tcp/9001/p2p/QmBootstrap02"
        - "/dnsaddr/dht-bootstrap-03.example.com/tcp/9001/p2p/QmBootstrap03"
    nat_traversal:
      autonat: true
      auto_relay: true      # ★ NAT 后必启用, 经 relay 与 L4 节点通信
      dcutr: true
    peer_store:
      path: "/data/identity/peerstore.db"
      gc_interval: 1h
    conn_gater:
      ip_rate_limit: 50

  # JWT 签发服务配置 (与 L4 节点共用同一端点, 但签发的 JWT L4Backhaul=false)
  jwt_service:
    endpoint: "https://control-plane.example.com/v1/node/jwt"
    refresh_interval: 5m
    refresh_before_expiry: 5m

edge:
  prefix_cache: { enabled: true, path: "/data/prefix", size_gb: 2000 }
  warm_cache:   { enabled: true, path: "/data/warm",   size_gb: 50000 }

access_layer:
  data_plane:
    enabled: false          # 不启用 Driver / 不接触网盘凭证
  fetch_segment_client:
    enabled: true           # 通过 libp2p stream 向兄弟节点 (含 L4 节点) 拉取
    # 不再硬编码 endpoints, 兄弟节点列表从 DHT 动态发现 (§5.2)
```

### 2.4 能力组合与部署场景

| 部署场景 | 典型能力组合 | 网络要求 | 凭证接触 |
|---------|------------|---------|---------|
| 自建核心节点（公网机房） | edge + l4_backhaul + relay_provider + peer_icp | 公网 IP，大带宽 | 持有网盘凭证 |
| 自建边缘节点（公网机房） | edge + peer_icp（+ 可选 relay_provider） | 公网 IP | 不接触 |
| 社区节点（家宽 NAT 后） | edge + peer_icp | 任意，需 NAT 穿透 | 不接触 |
| 社区可信节点（升级后） | edge + l4_backhaul + peer_icp | 公网 IP + 白名单 PeerId | 持有凭证（受控） |

**关键差异于原设计**：原 `L2_L4` / `L2_ONLY` 二分法假设"L2+L4 = 自建可信、L2-only = 不可信"。新模型把"是否自建"与"是否启用 L4"解耦——社区节点通过审查后也能启用 L4，自建节点也可部署为纯边缘（节省出口带宽）。能力是 JWT payload 字段，可由控制面动态调整（吊销 L4 只需下次 JWT 续签时签发新 JWT 置 false，节点 5min 内自然降级）。

### 2.5 节点发现流程总览

节点启动到提供服务的完整流程，详见 §5.2。**纯边缘节点可全自动完成**，L4 节点需额外人工认证步骤。

```
纯边缘节点 (自动注册, 全自动):
1. 加载/生成 Ed25519 身份 → 派生 PeerId
2. ★ PSK 校验: 加载编译时注入的 PSK, 启用 libp2p.PrivateNetwork(psk)
   └── 无 PSK 的连接在 TCP/QUIC 层被拒绝 (LIBP2P_FORCE_PNET=1 护栏)
3. 连接 DHT bootstrap 节点 (配置文件中的 bootstrap_peers)
   └── PSK 握手通过后, 进入 DHT 路由表
4. ★ 向控制面 JWT 服务请求 JWT (POST /v1/node/jwt, 独立于 DHT 发现)
   ├── 节点用 Ed25519 私钥签名 PeerID 证明身份
   ├── 控制面校验签名 + 限速检查 (同 IP 1次/小时)
   ├── 签发 JWT (Edge+PeerICP, L4=false, 1h 有效期)
   └── 返回 JWT 给节点
5. DHT Advertise("edge") → 在 DHT 上注册自己为 "edge" key 的 provider
   └── FindPeers("edge") → 获取全网 peer 列表 → 写入本地 PeerStore (libp2p 默认机制)
6. 与若干 peer 建立 libp2p 连接 (NAT 后经 relay)
   └── 连接时出示 JWT, 对方本地验签 + 检查有效期 → 写入 PeerStore
7. 加入 GossipSub topic "edge-popularity-v1" (用于热度同步 + PeX)
8. 从 PeerStore 重建一致性哈希环 → 开始服务客户端
9. 每 5min:
   ├── 向 JWT 服务请求续签 JWT
   └── DHT re-Advertise("edge") 心跳 (TTL 续期)
   └── 新 JWT 通过 IdentifyPush 推送给已连接 peer → peer 更新 PeerStore
10. (可选) 控制面探测公网可达 → 自动勾选 RelayProvider

L4 节点 (需人工认证, 在纯边缘流程基础上增加):
1-8. 同上 (先以纯边缘身份加入网络, JWT.L4Backhaul=false)
9. ★ 运营者向控制面提交 L4 认证材料 (POST /v1/node/l4-certify)
10. 控制面管理员审核 → PeerId 加入 L4 白名单
11. 节点下次 JWT 续签时, 控制面签发的 JWT.L4Backhaul = true
12. SyncBroadcaster 开始向该节点下发网盘凭证
13. 节点本地数据面启用, 可本地回源
```

调度层/控制面在节点发现中的角色降级为：**签发 JWT**（步骤 4，纯边缘自动 / L4 人工）+ **运行 DHT bootstrap 节点**（步骤 3 的入口）。节点之间的发现、连接、哈希环维护全部去中心化。FindPeers 使用 libp2p 默认机制返回全网 peer，后续可基于 IP 地理位置和节点质量推荐优化 peer 选择（TODO）。

---

---

## 3. 节点间通信与连接门控

### 3.1 兄弟节点拉取流程（libp2p stream）

原设计的 HTTP HEAD + HTTP GET 升级为 **libp2p 已认证 stream**。兄弟节点间的连接在 libp2p 安全握手层已完成 PeerId 校验（详见 §3.2），无需应用层再认证。

```go
// 兄弟节点拉取: libp2p stream 协议 /edge/blob/1.0.0
func (e *EdgeNode) fetchFromPeer(ctx context.Context, blobHash string) ([]byte, bool) {
    // 1. 一致性哈希找主节点 (PeerId)
    mainPeerID := e.hashRing.Get(blobHash)  // 返回 PeerId, 非 IP:port
    if mainPeerID == e.selfPeerID {
        return nil, false  // 自己就是主节点, 不自查
    }

    // 2. HEAD 探测: libp2p stream, 协议 /edge/blob/head/1.0.0
    //    超时仍为 10ms (区域内 RTT < 2ms)
    headCtx, cancel := context.WithTimeout(ctx, 10*time.Millisecond)
    defer cancel()

    stream, err := e.host.NewStream(headCtx, peer.ID(mainPeerID), edgeBlobHeadProto)
    if err != nil {
        // 主节点不可达: 评分衰减 (§4.2), 视为兄弟不可用
        e.peerScorer.RecordICPTimeout(mainPeerID)
        return nil, false
    }
    defer stream.Close()

    // 写入 blob_hash, 读取响应头 (1 byte: 0x01=HIT, 0x00=MISS)
    proto.WriteVarint(stream, uint64(len(blobHash)))
    stream.Write([]byte(blobHash))
    resp := make([]byte, 1)
    if _, err := io.ReadFull(stream, resp); err != nil || resp[0] != 0x01 {
        return nil, false  // 主节点也无, 不再尝试其他兄弟 (避免放大)
    }

    // 3. GET 拉取: 复用已建立的 libp2p 连接 (多路复用), 新开 stream
    getStream, err := e.host.NewStream(ctx, peer.ID(mainPeerID), edgeBlobGetProto)
    if err != nil { return nil, false }
    defer getStream.Close()

    proto.WriteVarint(getStream, uint64(len(blobHash)))
    getStream.Write([]byte(blobHash))
    data, err := io.ReadAll(getStream)
    if err != nil { return nil, false }

    // 4. 评分反馈: 成功交付 → 主节点评分 +
    e.peerScorer.RecordICPSuccess(mainPeerID, len(data))
    return data, true
}
```

**HEAD 探测超时仍为 10ms**。兄弟节点经 libp2p 连接（持久化多路复用），HEAD 探测不再有 TCP 握手开销，实际比原 HTTP HEAD 更快。若超时，视为兄弟不可用，按节点能力分流：`L4Backhaul=true` 走本地回源，`L4Backhaul=false` 走 DHT 发现的 L4 节点拉取。

### 3.2 ConnectionGater：连接层资源保护与评分隔离

ConnectionGater 在本系统中承担两个职责：**IP 层 DDoS 防御**和**已降级节点的连接拒绝**。它**不是准入层**——准入由 PSK（传输层）+ JWT 签发（应用层）完成（§5.2），节点获得 JWT 后进入 PeerStore，ConnectionGater 只是查 PeerStore 决定是否放行。

```go
// EdgeConnectionGater: 分发域节点连接门控
type EdgeConnectionGater struct {
    peerStore    *PeerStore
    jwtVerifier  *JWTVerifier    // 本地验证 JWT 签名 (持有控制面公钥, 不回问 bootstrap)
    ipLimiter    *rate.Limiter   // 单 IP 连接限速
    cidrAllowlist *netipx.IPSet  // 可选, 限制来源 CIDR
}

// 1. InterceptAccept: 接受 TCP/QUIC 连接前 (此时无 PeerId, 只能按 IP)
//    用途: IP 限速 + 可选 CIDR 白名单 (DDoS 防御)
func (g *EdgeConnectionGater) InterceptAccept(addrs network.ConnMultiaddrs) bool {
    remoteIP := extractIP(addrs)
    if g.cidrAllowlist != nil && !g.cidrAllowlist.Contains(remoteIP) {
        return false
    }
    if !g.ipLimiter.Allow() {
        return false  // 限速: 单 IP 每秒超过 50 个新连接拒绝
    }
    return true
}

// 2. InterceptSecured: TLS/Noise 握手完成后 (已有 PeerId)
//    用途: JWT 自验证 + PeerStore 写入/更新 + 评分检查
//    ★ 这是节点间准入的核心检查点
func (g *EdgeConnectionGater) InterceptSecured(dir network.Direction, p peer.ID, addrs network.ConnMultiaddrs) bool {
    // 查 PeerStore: 该 PeerId 是否已知
    entry, ok := g.peerStore.Get(p)
    if !ok {
        // 未知 peer: 允许连接 (用于新节点首次连接, 后续 stream 层要求出示 JWT)
        return true
    }
    // 已知 peer: JWT 过期检查
    if entry.Stale {
        // JWT 已过期且未续期 → 拒绝
        return false
    }
    // 评分低于 GraylistThreshold 的恶意节点拒绝
    if entry.Score < GraylistThreshold {
        return false
    }
    return true
}

// 3. InterceptUpgraded: 多路复用器升级后 (可开 stream)
//    用途: 首次连接时验证 JWT 并写入 PeerStore; 后续连接检查 JWT 有效性
func (g *EdgeConnectionGater) InterceptUpgraded(conn network.Conn) (bool, control.DisconnectReason) {
    p := conn.RemotePeer()
    entry, ok := g.peerStore.Get(p)

    if !ok {
        // 未知 peer: 要求在首个 stream 中出示 JWT
        // JWT 由 stream 协议层传递 (/edge/auth/1.0.0), 验证通过后写入 PeerStore
        // 未出示有效 JWT 的 peer 无法开业务 stream
        return true, 0  // 允许连接升级, 但业务 stream 会被拒绝
    }

    // 已知 peer: 检查 JWT 有效期 (无宽限期, 过期即拒绝)
    now := time.Now().Unix()
    if entry.JWTExp < now {
        // JWT 已过期 → 立即标记 Stale → 拒绝 (节点应在过期前 5min 续签, 未续签成功视为失联)
        g.peerStore.MarkStale(p)
        return false, control.DisconnectReason(0)
    }

    return true, 0
}

// JWT 验证 (stream 协议层, 首次连接时调用)
// 协议: /edge/auth/1.0.0
// 对方在首个 stream 中发送 JWT, 本地验证签名 + 有效期 + PeerId 绑定
func (e *EdgeNode) verifyPeerJWT(p peer.ID, jwtStr CapabilityJWT) (*PeerStoreEntry, error) {
    // 1. 本地验证 JWT 签名 (用控制面公钥, 不回问 bootstrap)
    payload, err := e.jwtVerifier.Verify(jwtStr)
    if err != nil { return nil, ErrInvalidJWT }

    // 2. 校验 PeerId 绑定 (防冒用)
    if payload.PeerID != PeerId(p) { return nil, ErrPeerIDMismatch }

    // 3. 校验有效期 (无宽限期, 过期即拒绝)
    now := time.Now().Unix()
    if payload.Exp < now {
        return nil, ErrJWTExpired
    }

    // 4. 验证通过 → 写入/更新 PeerStore
    entry := &PeerStoreEntry{
        PeerID:       PeerId(p),
        JWT:          jwtStr,
        Capabilities: payload.Capabilities,
        JWTExp:       payload.Exp,
        LastSeen:     now,
        Score:        0,  // 初始中性评分
        Stale:        false,
    }
    e.peerStore.Put(PeerId(p), entry)
    return entry, nil
}

// JWT 续签策略: 过期前 5min 开始续签, 续签失败立即重试 (指数退避)
// JWT 过期即降级: 无宽限期, Exp 到达且未续签成功 → PeerStore Stale → 拒绝通信
const JWTRefreshBeforeExpiry = 5 * time.Minute  // 过期前 5min 开始续签
```

**JWT 刷新传播**：
节点向 JWT 服务续签获得新 JWT 后，通过 libp2p **IdentifyPush** 协议将新 JWT 推送给所有已连接的 peer。peer 收到后本地验证新 JWT 签名，更新 PeerStore 中的 `JWT` + `JWTExp` 字段。

```go
// 节点向 JWT 服务续签获得新 JWT 后, 推送给已连接 peer
func (e *EdgeNode) onJWTRefreshed(newJWT CapabilityJWT) {
    // IdentifyPush 自动将新 JWT 随 Identify 消息推送给所有已连接 peer
    // peer 收到后调用 verifyPeerJWT 更新 PeerStore
    e.host.Peerstore().PutData(e.selfPeerID, "jwt", newJWT)
    // libp2p IdentifyPush 会自动通知已连接 peer
}

// peer 侧: 收到 IdentifyPush 后更新 JWT
func (e *EdgeNode) OnIdentifyPush(p peer.ID) {
    // 从 peer store 读取对方推送的 JWT
    jwtStr, err := e.host.Peerstore().GetData(p, "jwt")
    if err != nil { return }
    // 本地验证新 JWT
    if _, err := e.verifyPeerJWT(p, jwtStr.(CapabilityJWT)); err != nil {
        // 验证失败 → 不更新 (保持旧 JWT, 等其过期后标记 Stale)
        return
    }
}
```

**防御层级说明**：本系统的网络准入实际只有三层：

| 层 | 机制 | 防什么 |
|---|------|--------|
| **传输层** | PSK（编译时注入, `libp2p.PrivateNetwork`） | 无关节点 / 公共 IPFS 节点连入 |
| **认证层** | JWT 签发 + ConnectionGater 验签 | 未授权节点进入网络拓扑 |
| **行为层** | GossipSub AppSpecificScore + JWT 有效期 | 已加入节点的恶意行为 + 过期节点自动退出 |

libp2p 安全握手（Noise/TLS）是内置的身份认证，不算设计层。InterceptSecured 不是独立准入层——它验证 JWT（自验证，不回问 bootstrap），拒绝的是"JWT 过期"或"评分降级"的节点。

**信任链总览**：

```
新节点首次加入:
  1. 加载 PSK (编译时注入) → 连接 DHT bootstrap (PSK 握手通过)
  2. 向控制面 JWT 服务请求 JWT (Ed25519 签名 PeerID 证明身份)
  3. DHT Advertise("edge") + FindPeers → 获取全网 peer → 与 peer 建立连接
  4. 首个 stream 出示 JWT → 对方本地验签 → 写入 PeerStore
  5. 后续连接: InterceptSecured 查 PeerStore → JWT 未过期 + 评分达标 → 放行

JWT 刷新:
  - 每 5min 向 JWT 服务请求续签 → 新 JWT → IdentifyPush 推送给已连接 peer
  - peer 验证新 JWT → 更新 PeerStore 的 JWT + JWTExp

恶意节点处置:
  - 投毒热度数据 → 其他节点对账发现 → AppSpecificScore 降分 (§4.2)
  - ICP 超时率高 → RecordICPTimeout 累积 → 评分降级
  - 评分 < GraylistThreshold → InterceptSecured 拒绝新连接 + 哈希环剔除
  - JWT 过期 → 立即 PeerStore Stale → 拒绝通信 + 哈希环剔除 (无宽限期)
  - 节点离线 → 停止 JWT 续签 → JWT 过期 → 自然退出
```

**与原 HTTP ICP 的对比**：

| 维度 | 原 HTTP HEAD ICP | 新 libp2p stream ICP |
|------|-----------------|---------------------|
| 认证 | 无（任何 IP 可查询） | PeerId 密码学绑定 |
| 连接复用 | 每次 TCP 握手 | 持久连接多路复用 |
| 恶意节点隔离 | 无（依赖 IP 黑名单） | GossipSub 评分 + ConnectionGater |
| NAT 穿越 | 不支持（兄弟必须公网） | DCUtR 打洞 + relay 兜底 |
| 协议升级 | 需改 HTTP 接口 | libp2p 协议多版本协商 |

---

---

## 4. 本地流行度 GossipSub（带评分的不可信同步）

在边缘节点之间用 **libp2p GossipSub** 同步流行度。原设计的"无条件取 max 合并"在社区节点不可信的场景下会被投毒——恶意节点可虚报任意 blob 的高热度，诱导其他节点缓存垃圾或驱逐热门内容。新版引入 GossipSub v1.1 的 **AppSpecificScore** 评分机制，对等节点的热度上报按其评分加权采信。

### 4.1 GossipSub topic 与消息格式

```go
// GossipSub topic: "edge-popularity-v1" (全局统一, 不按区域划分)
// 节点 DHT Advertise("edge") 后, 自动 subscribe 此 topic

type PopularityUpdate struct {
    // 来源节点签名 (防止篡改 / 重放)
    PeerID    PeerId
    Timestamp int64
    Sig       []byte  // 对 Payload 的 Ed25519 签名

    // Payload: blob_hash → 近 5min 命中次数
    Counts    map[string]int64  // key = blob_hash, value = sliding window sum
}

// 每个边缘节点本地维护的流行度滑动窗口
type LocalPopularity struct {
    mu      sync.RWMutex
    counters map[string]*SlidingWindow  // key = blob_hash
}

type SlidingWindow struct {
    Buckets [6]int64  // 6 x 1min = 6min 滑动窗口
    CurIdx  int
    LastRotate time.Time
}

// 每次 blob 命中/回源时调用
func (lp *LocalPopularity) Hit(blobHash string) {
    lp.mu.Lock()
    defer lp.mu.Unlock()
    w, ok := lp.counters[blobHash]
    if !ok {
        w = &SlidingWindow{LastRotate: time.Now()}
        lp.counters[blobHash] = w
    }
    w.rotateIfNeeded()
    w.Buckets[w.CurIdx]++
}

// GossipSub 发布: 每 30s 将本地窗口快照签名后发布
// GossipSub 自动决定传播给哪些 mesh peer (基于评分, 见 §4.2)
func (e *EdgeNode) publishPopularity(ctx context.Context) {
    ticker := time.NewTicker(30 * time.Second)
    defer ticker.Stop()
    for {
        select {
        case <-ctx.Done(): return
        case <-ticker.C:
            snapshot := e.localPop.Snapshot()  // map[blob_hash]int64
            update := PopularityUpdate{
                PeerID:    e.selfPeerID,
                Timestamp: time.Now().Unix(),
                Counts:    snapshot,
            }
            update.Sig = ed25519.Sign(e.privKey, encodePayload(update))
            e.gossipSub.Publish(e.popTopic, encode(update))  // 失败静默
        }
    }
}
```

### 4.2 带评分的合并策略

原设计的 `mergedPop.Merge(remote)` 无条件取 max——任何节点上报的高热度都被采信。新版改为**按来源评分加权**：

```go
// 合并视图: blob_hash → (热度, 来源评分加权和)
type MergedPopularity struct {
    mu       sync.RWMutex
    entries  map[string]*MergedEntry  // key = blob_hash
}

type MergedEntry struct {
    // 加权热度: sum(source_score * reported_count) / sum(source_score)
    // 仅当 sum(source_score) > 阈值时认为该热度可信
    WeightedHeat float64
    TotalWeight  float64  // 参与上报的节点评分总和
    LastUpdate   int64
}

// 接收 GossipSub 消息: 按来源评分加权合并
func (e *EdgeNode) OnPopularityUpdate(msg *pubsub.Message) {
    var update PopularityUpdate
    if err := decode(msg.Data, &update); err != nil { return }

    // 1. 验证签名 (防篡改)
    if !ed25519.Verify(pubKeyOf(update.PeerID), encodePayload(update), update.Sig) {
        e.peerScorer.RecordInvalidSignature(update.PeerID)
        return
    }

    // 2. 查来源评分 (§4.3)
    sourceScore := e.peerScorer.GetScore(update.PeerID)
    if sourceScore < GraylistThreshold {
        return  // 屏蔽节点的消息不采信
    }

    // 3. 加权合并
    e.mergedPop.mu.Lock()
    defer e.mergedPop.mu.Unlock()
    for blobHash, count := range update.Counts {
        entry, ok := e.mergedPop.entries[blobHash]
        if !ok {
            entry = &MergedEntry{}
            e.mergedPop.entries[blobHash] = entry
        }
        // 增量加权平均: new_heat = (old_w*old_heat + src_score*count) / (old_w + src_score)
        entry.WeightedHeat = (entry.TotalWeight*entry.WeightedHeat + sourceScore*float64(count)) /
                             (entry.TotalWeight + sourceScore)
        entry.TotalWeight += sourceScore
        entry.LastUpdate = update.Timestamp
    }
}

// 逐出决策时读取的流行度来源 (修订)
func (e *EdgeNode) getVideoPopularity(blobHash string) float64 {
    // 1. 优先读 GossipSub 加权合并的近 5min 热度 (分钟级, 实时)
    e.mergedPop.mu.RLock()
    if entry, ok := e.mergedPop.entries[blobHash]; ok && entry.TotalWeight > MinTrustedWeight {
        e.mergedPop.mu.RUnlock()
        return entry.WeightedHeat
    }
    e.mergedPop.mu.RUnlock()
    // 2. fallback 到 PG 的 window_24h (小时级, 长期趋势)
    return e.metadataCache.GetPopularity24h(blobHash)
}

const MinTrustedWeight = 5.0  // 至少 5.0 的累计评分才采信合并热度, 防止单节点投毒
```

**投毒防御**：
- 单个恶意节点虚报 blob X 热度 10000 → 其评分若低，加权后影响小
- 多个 Sybil 节点协同投毒 → `IPColocationFactor` 评分项惩罚同 IP 多 PeerId（见 §4.3）
- 签名验证失败 → `RecordInvalidSignature` 直接降分
- 评分 < GraylistThreshold → 后续消息直接丢弃

### 4.3 PeerScorer：GossipSub AppSpecificScore

GossipSub v1.1 的 `AppSpecificScore` 回调允许应用层注入评分。本系统在此综合 4 类行为指标：

```go
type PeerScorer struct {
    scores sync.Map  // map[PeerId]float64
}

// AppSpecificScore: GossipSub 每秒心跳时调用, 影响该 peer 的 mesh 留存
func (s *PeerScorer) AppSpecificScore(p peer.ID) float64 {
    v, ok := s.scores.Load(p)
    if !ok { return 0 }  // 未知 peer 中性评分
    return v.(float64)
}

// ─── 评分输入事件 ───

// 1. ICP 交付成功 (§3.1): +0.5/次, 上限 +10
func (s *PeerScorer) RecordICPSuccess(p PeerId, bytes int) {
    s.adjust(p, min(0.5, 10.0-s.current(p)))
}

// 2. ICP 超时/失败 (§3.1): -1.0/次
func (s *PeerScorer) RecordICPTimeout(p PeerId) {
    s.adjust(p, -1.0)
}

// 3. 带宽贡献: 每 GB +1.0 (奖励中继流量 / 服务流量)
func (s *PeerScorer) RecordBandwidthContributed(p PeerId, bytes int64) {
    s.adjust(p, float64(bytes)/1e9)
}

// 4. 异常行为: -5.0 (重罚)
//    - 签名验证失败 (冒充/篡改)
//    - 对账发现的热度投毒 (上报热度与多数节点偏差 > 5x)
//    - 限流违规 (单 IP 大量连接)
func (s *PeerScorer) RecordMisbehavior(p PeerId, kind MisbehaviorKind) {
    s.adjust(p, -5.0)
    if s.current(p) < GraylistThreshold {
        s.markGraylisted(p)  // 触发 ConnectionGater 拒绝新连接 (§3.2)
    }
}

// ─── GossipSub 评分阈值 (来自 v1.1 spec) ───

const (
    GraylistThreshold    = -10.0  // 低于此值: ConnectionGater 拒绝 + 哈希环剔除
    PublishThreshold     = -20.0  // 低于此值: 不向该 peer flood publish
    GossipThreshold      = -5.0   // 低于此值: 抑制 IHAVE/IWANT
    OpportunisticGraftThreshold = 5.0  // mesh 中位数低于此值时, 主动 graft 高分 peer
)
```

**GossipSub 内置评分项**（除 AppSpecificScore 外，由库自动维护）：

| 评分项 | 含义 | 对本系统的意义 |
|--------|------|--------------|
| `TimeInMesh` | 在 mesh 中停留时长 | 奖励稳定在线的节点 |
| `FirstMessageDeliveries` | 首次交付新消息 | 奖励低延迟节点 |
| `MeshMessageDeliveries` | mesh 消息交付率 | 惩罚静默节点 |
| `IPColocationFactor` | 同 IP 多 PeerId 惩罚 | **Sybil 防御核心** |
| `BehaviourPenalty` | IWANT 不响应 / 过早 re-graft | 惩罚协议违规 |

**评分传播路径**：

```
节点行为 (ICP 成功/超时, 带宽贡献, 投毒)
    │
    v
PeerScorer.AppSpecificScore(peer) ─┐
                                   │
GossipSub 内置评分项 ──────────────┤
                                   v
                          总评分 = AppSpecific + 内置项
                                   │
                ┌──────────────────┼──────────────────┐
                v                  v                  v
        < GraylistThreshold   < GossipThreshold   正常
        ConnectionGater       抑制 gossip          正常 mesh
        拒绝 + 哈希环剔除     消息传播
```

### 4.4 热度数据模型（分发域所有）

热度数据完全属于分发域，不依赖存储域，也不依赖元数据模块。分发域通过 `content_id` 与元数据模块关联（content 维度热度），通过 `blob_hash` 与 storage 域关联（回源查位置）。

> **设计说明**：热度是 content 维度的概念（"这个视频热门"），不是 blob 维度（"这个 720p seg3 热门"无意义）。所以持久化热度表用 `content_id` 作主键；gossip 本地热度按 `blob_hash` 跟踪是为了缓存逐出（"这个 blob 的访问次数"），但 PinOrchestrator 的全局决策基于 content 维度。

#### 8.1.1 持久化热度表（PG）

```sql
-- 热度表: 由分发域维护, content 维度
-- 关联键: content_id (与元数据模块的 content 表关联)
CREATE TABLE content_popularity (
    content_id      UUID PRIMARY KEY,           -- 关联键: 与元数据模块 content.content_id 一致
    request_count   BIGINT DEFAULT 0,           -- 累计请求次数 (所有 blob 共享)
    last_access     TIMESTAMPTZ,
    window_24h      BIGINT DEFAULT 0,           -- 过去 24h 请求次数（pg_cron 定期清零）
    window_1h       BIGINT DEFAULT 0            -- 过去 1h 请求次数
);

-- 索引: 按 window_24h 降序, 供 PinOrchestrator 查询 top-N 热门内容
CREATE INDEX idx_popularity_24h ON content_popularity (window_24h DESC);
```

#### 8.1.2 热度 gRPC 接口

```go
// 分发域热度服务 (可独立部署, 也可与元数据服务共进程但逻辑独立)
service PopularityService {
    // 边缘节点异步上报访问事件
    // blob 级别上报, 服务内部聚合到 content 维度 (按 blob_hash → content_id 反查)
    rpc ReportAccess(ReportAccessReq) returns (Empty);
    // PinOrchestrator 查询热门/冷门内容列表
    rpc GetTopContents(GetTopContentsReq) returns (GetTopContentsResp);
    rpc GetColdContents(GetColdContentsReq) returns (GetColdContentsResp);
}

message ReportAccessReq {
    string content_id = 1;   // content 维度上报 (边缘节点已知 content_id)
    string blob_hash = 2;    // blob 维度上报 (用于本地缓存逐出决策)
    int64  count = 3;        // 批量上报时可为 N
    int64  timestamp = 4;
}
```

#### 8.1.3 与存储域 / 元数据模块的关联

```
元数据模块                       distribution 域
─────────────                    ─────────────────
content                       content_popularity
  content_id (PK)  ◄── 关联键 ──►  content_id (PK)
  content_type
  type_metadata                 gossip 本地热度 (内存, blob 维度)
                                 └── blob_hash → sliding window

storage 域 (内容寻址层)
blob                            (逐出决策按 blob_hash 查 gossip 本地热度)
  blob_hash (PK)
  blob_type                     (PinOrchestrator 全局决策按 content_id 查 PG 热度)
  size_bytes
blob_location
  blob_hash (FK)  ◄── 关联键 ──►  (回源时按 blob_hash 查 blob_location)
  backend_id
  file_id
```

**关联规则**：
- 分发域通过 `content_id` 关联元数据模块的 `content` 表（content 维度热度，驱动 PinOrchestrator 全局决策）
- 分发域通过 `blob_hash` 关联 storage 域的 `blob_location`（回源时按 blob 查位置）
- 分发域的逐出决策查 gossip 本地热度时，用 `blob_hash`（blob 维度访问计数，用于缓存逐出）
- 分发域的 PinOrchestrator 重算时，用 `content_id` 查 `content_popularity`（content 维度热度）
- 元数据模块的 `content_blob` 关联表把 `content_id` 和 `blob_hash` 桥接起来，分发域可据此从 content 找到所有 blob 或反向
- 三个域不共享表，只通过 ID 关联

#### 8.1.4 热度数据流

```
┌─ distribution 域内部闭环 ────────────────────────────────────────┐
│                                                                  │
│  边缘节点本地 gossip 热度 (30s, 分钟级)                           │
│  ┌────────────────────┐                                          │
│  │ LocalPopularity    │ → 驱动本地逐出/预取 (1min内生效)          │
│  │ (滑动窗口, 内存)    │                                          │
│  │ blob_hash → counts │                                          │
│  └─────────┬──────────┘                                          │
│            │                                                     │
│            │ ReportAccess (gRPC, 异步批量)                        │
│            │ 上报 content_id + blob_hash                          │
│            v                                                     │
│  ┌────────────────────┐                                          │
│  │ PG content_popularity│ → pg_cron 每小时重算 window_24h         │
│  │ (持久化, 小时级)    │ → 驱动 PinOrchestrator (10min重算)        │
│  │ content_id → counts │                                          │
│  └────────────────────┘   → PinPlan 下发 (prefix 扩展/降级)       │
│                                                                  │
└──────────────────────────────────────────────────────────────────┘
         │
         │ 双关联键: content_id (元数据模块) + blob_hash (storage 域)
         v
┌─ 元数据模块 ────────────────────────────────────────────────────┐
│  content + content_blob (通过 content_id 关联)                   │
│  PinOrchestrator 按 content_id 查元数据模块拿到该 content 的所有  │
│  blob + role/sort_order, 然后下发 PinPlan                        │
└──────────────────────────────────────────────────────────────────┘
         │
         │ blob_hash (回源热路径)
         v
┌─ storage 域 (内容寻址层) ───────────────────────────────────────┐
│  blob + blob_location (通过 blob_hash 关联)                     │
│  回源时: distribution 用 blob_hash 查 storage 的 blob_location   │
└──────────────────────────────────────────────────────────────────┘
```

**与存储域/元数据模块流行度的关系**：
- **gossip 热度**（分钟级，blob 维度）：驱动本地逐出决策和本地预取决策，延迟 < 1min
- **PG `content_popularity`**（小时级，content 维度）：驱动控制面的 PinOrchestrator 做全局 prefix/pin 决策（§9），保留为长期趋势源
- 两者不冲突：gossip 是"快但局部"，PG 是"慢但全局"
- 两者都属于分发域，存储域不参与热度数据的读写

### 4.5 热度信任模型现状

`HandlePopularityMessage`（`internal/node/gossippop/mergedpop.go`）是 GossipSub 热度消息的入口。社区节点不可信，恶意节点可虚报任意 blob 的高热度诱导其他节点缓存垃圾或驱逐热门内容。当前实现采用**四层防御 + 一个不变式**，按顺序执行：

| 层 | 防御 | 实现位置 | 失败处置 |
|----|------|----------|----------|
| 1. **信任守卫**（T19） | 拒绝 stale / 未知 peer 的热度 | `HandlePopularityMessage` 入口，`PeerEntryLookup.StaleOrUnknown` | `slog.Debug` + 丢弃 |
| 2. **签名验证** | Ed25519 验签（防篡改 / 重放） | `OnPopularityUpdate:86-93` | `errInvalidSig` + 丢弃 |
| 3. **GraylistThreshold 地板** | 评分 ≤ -20.0 的灰名单 peer 不采信 | `OnPopularityUpdate:96` | `errScoreTooLow` + 丢弃 |
| 4. **MinTrustedWeight 输出门** | `TotalWeight < 5.0` 的条目不进入 `Snapshot` / `getVideoPopularity` | `Snapshot:149` / `getVideoPopularity:129` | 静默过滤（不进入逐出排序） |
| 5. **零注入不变式** | 增量加权平均公式天然免疫 `sourceScore=0` 注入（分子分母同加 0） | `OnPopularityUpdate:110-114` | 无需显式处置，公式自守恒 |

**第 1 层信任守卫的判定来源**：`PeerEntryLookup` 接口由 `cmd/edge-node/main.go` 的 `peerEntryLookupAdapter` 适配 `*peerstore.PeerEntryStore`。判定逻辑：

- `Get(peerID)` 返回 `!ok` → 未知 peer（从未通过 JWT 验证写入 PeerEntryStore）→ 丢弃
- `entry.Stale == true` → JWT 过期 / 被驱逐的 peer → 丢弃
- 其余 → 进入后续防御层

**当前消费范围（重要边界）**：

gossip 热度目前**只驱动本地缓存逐出**（`warmCache.SetPopSource` → `cache.Evict`），**不参与 pin 决策**。Pin 决策由控制面 `PinOrchestrator` 基于 PG `content_popularity`（小时级、content 维度）做出。这是有意的安全边界：

- 即使四层防御全部被绕过（极端假设），恶意热度也只能影响单个节点的本地缓存逐出顺序，不能诱导其他节点 pin 垃圾内容
- 全局 pin 决策的输入源（PG `content_popularity`）由控制面汇聚，社区节点无法直接写入

**未覆盖的场景**（已知限制，留待未来工作）：

- 多 peer 协同投毒（sybil + 短时间内多 peer 上报同一 blob 高热度）可通过第 4 层 `MinTrustedWeight` 门，但需要 ≥ 11 个独立 ICP 成功（11 × 0.5 = 5.5）才能突破。GossipSub 的 `IPColocationFactor` 评分会惩罚同 IP 多 PeerId，提高了 sybil 成本
- 热度真实性仲裁（"上报热度与多数节点偏差 > 5x"）目前由 `PeerScorer.RecordMisbehavior(MisbehaviorPoisonedHeat)` 触发，但仲裁逻辑（对账）尚未实现——`MisbehaviorPoisonedHeat` 仅作为分类常量存在

---


---

### 5.1 设计原则：控制面退化为入口

原设计控制面是节点发现的权威：维护节点列表、广播 `NODE_LIST_UPDATE`、心跳摘除。新设计将节点发现**去中心化**，控制面在节点发现这件事上退化为**入口**（DHT bootstrap 提供者 + JWT 签发者），不再维护运行时节点列表。

**控制面仍然中心化的职责**（与节点发现无关）：

| 职责 | 中心化原因 |
|------|----------|
| DNS GeoIP 健康状态同步 | DNS 调度天然中心化 |
| PinPlan 计算 + 下发 | 全局流行度视图需要中心化聚合 |
| 网盘凭证 / 账号健康 SyncBroadcaster | 凭证主库单点，安全要求 |
| JWT 签发 | 准入授权天然中心化 |
| DHT bootstrap 节点 | 网络入口，需公网稳定 |

**控制面退出的职责**：

| 职责 | 原方式 | 新方式 |
|------|--------|--------|
| 维护运行时节点列表 | `NODE_LIST_UPDATE` 广播 | 节点本地 PeerStore + DHT 发现 |
| 心跳检测 + 摘除 | 10s 心跳 / 30s 摘除 | DHT Advertise TTL 15min / 5min re-Advertise + JWT 5min 续签 |
| 哈希环成员广播 | 控制面触发 | 节点 PeerStore 变更触发 (README.md §5.1) |
| 同区 L4 节点列表下发 | 调度层推送 | 节点从 PeerStore 筛选 L4Backhaul=true (DHT 发现) |

**仍保留的设计原则**（与原 §5.1 一致）：控制面**不维护节点的缓存索引和 pin 全量状态**。原因：

1. **数据量不可承受**：单节点最大 ~5243 万 blob，10 节点 × Bloom Filter ~630MB 内存 + 每 60s 630MB 上报带宽
2. **正确性不依赖**：缓存 miss → 回源，不影响功能；pin 未执行 → 走普通缓存，不影响功能
3. **业界验证**：IPFS 用 DHT（无中心索引），传统 CDN 用一致性哈希（无全量缓存索引）

### 5.2 节点发现：PSK 私有网络 + DHT + GossipSub PeX

节点发现完全去中心化，基于 **libp2p DHT**（`go-libp2p-kad-dht`，go-libp2p 核心维护）。控制面运行 3-5 个 DHT bootstrap 节点（与控制面实例共进程，逻辑独立），作为新节点加入网络的入口。**PSK（Pre-Shared Key）编译时注入**，确保只有持有同一 PSK 的节点能加入私有网络，与公共 IPFS 网络完全隔离。

**安全层次**：

| 层 | 机制 | 防什么 |
|---|------|--------|
| **传输层** | PSK（`libp2p.PrivateNetwork(psk)`，编译时注入） | 无关节点 / 公共 IPFS 节点连入；`LIBP2P_FORCE_PNET=1` 护栏防止误配置 |
| **应用层** | JWT（ConnectionGater 验签，独立服务签发） | 已加入节点的越权（如非 L4 尝试 L4 操作）、过期节点、评分降级节点 |

**发现流程**：

```
新节点启动
  │
  1. 加载 Ed25519 身份 → 派生 PeerId
  │
  2. ★ PSK 校验: 加载编译时注入的 PSK
  │    └── libp2p.New(..., libp2p.PrivateNetwork(psk))
  │    └── 所有连接在 TCP/QUIC 层强制 PSK 握手, 不匹配则拒绝
  │    └── LIBP2P_FORCE_PNET=1 护栏: 未配置 PSK 时拒绝启动连接
  │
  3. 连接 DHT bootstrap 节点 (配置文件中的 bootstrap_peers, 自建不连公共 IPFS)
  │    └── PSK 握手通过后, 进入 DHT 路由表
  │    └── 私有 DHT: dht.New(ctx, host, dht.Mode(dht.ModeServer/ModeClient))
  │       不使用 dht.DefaultBootstrapPeers
  │
  4. ★ 向控制面 JWT 服务请求 JWT (POST /v1/node/jwt, 独立于 DHT 发现)
  │    └── 节点用 Ed25519 私钥签名 PeerID 证明身份
  │    └── 控制面校验签名 + 限速 (同 IP 1次/小时)
  │    └── 签发 JWT (L4 仅对白名单 PeerId, 1h 有效期)
  │    └── 返回 JWT 给节点
  │
  5. DHT Advertise("edge") + FindPeers("edge") → 获取全网 peer 列表
  │    └── namespace "edge" 经 SHA256 → CID, 在 DHT 上 Provide/FindProviders
  │    └── PSK 已保证只有主网节点在 DHT 中, namespace 仅作查找约定 (不承担安全职责)
  │    └── 写入本地 PeerStore (BadgerDB, 重启可恢复)
  │    └── 后续可基于 IP 地理位置 + 节点质量推荐优化 (TODO)
  │
  6. 与若干 peer 建立 libp2p 连接 (NAT 后经 relay, §6.2)
  │    └── PSK 握手自动完成 (libp2p 层)
  │    └── 首个 stream 出示 JWT → 对方本地验签 → 写入 PeerStore (§3.2)
  │
  7. GossipSub subscribe "edge-popularity-v1" (§8) + PeX 启用
  │    └── GossipSub prune 消息捎带对端 peer, 增量补充 PeerStore
  │
  8. 从 PeerStore 重建一致性哈希环 (README.md §5.1) → 开始服务客户端
  │
  9. 每 5min:
  │    ├── 向 JWT 服务请求续签 JWT (POST /v1/node/jwt)
  │    └── DHT re-Advertise("edge") 心跳 (TTL 续期)
  │    └── 新 JWT 通过 IdentifyPush 推送给已连接 peer → peer 更新 PeerStore
  │    └── GossipSub 心跳 1s; PeerStore 持续增量更新
```

```go
// 节点侧: DHT 发现 + JWT 签发客户端 (两个独立职责)
type EdgeDiscovery struct {
    host          host.Host
    dht           *dht.IpfsDHT          // 私有 DHT (go-libp2p-kad-dht)
    routingDisc   *routing.RoutingDiscovery  // discovery.NewRoutingDiscovery(dht)
    jwtClient     *JWTClient             // 独立 JWT 签发客户端
    jwt           CapabilityJWT          // 当前持有的 JWT
    namespace     string                 // 固定 "edge", 查找约定 (安全由 PSK 保证)
    peerStore     *PeerStore
    bootstrapAddrs []peer.AddrInfo       // 自建 DHT bootstrap 节点
}

// 启动: PSK 已在 libp2p.New 时注入, 这里只做 DHT + JWT
func (d *EdgeDiscovery) Start(ctx context.Context) error {
    // 1. 连接 DHT bootstrap 节点 (自建, 不连公共 IPFS)
    for _, addr := range d.bootstrapAddrs {
        if err := d.host.Connect(ctx, addr); err == nil { break }
    }

    // 2. 初始化私有 DHT (ModeServer 公网节点 / ModeClient NAT 后节点)
    //    不使用 dht.DefaultBootstrapPeers
    idht, err := dht.New(ctx, d.host)
    if err != nil { return err }
    d.dht = idht

    // 3. 向控制面 JWT 服务请求 JWT (独立于 DHT 发现)
    //    节点用 Ed25519 私钥签名 PeerID 证明身份
    resp, err := d.jwtClient.RequestJWT(ctx)
    if err != nil { return err }
    d.jwt = resp.JWT

    // 4. DHT Advertise + FindPeers 获取全网 peer
    d.routingDisc = routing.NewRoutingDiscovery(d.dht)
    routing.Advertise(ctx, d.routingDisc, d.namespace, routing.TTL(15*time.Minute))

    peerChan, err := d.routingDisc.FindPeers(ctx, d.namespace, discovery.Limit(50))
    if err != nil { return err }
    for p := range peerChan {
        d.peerStore.Put(PeerId(p.ID), PeerStoreEntry{
            PeerID:   PeerId(p.ID),
            Addrs:    p.Addrs,
            LastSeen: time.Now().Unix(),
        })
    }

    // 5. 启动心跳循环: JWT 续签 + DHT re-Advertise
    go d.heartbeatLoop(ctx)
    // 6. 启动周期性 FindPeers: 每 5min 校正 PeerStore
    go d.discoverLoop(ctx)
    return nil
}

func (d *EdgeDiscovery) heartbeatLoop(ctx context.Context) {
    jwtTicker := time.NewTicker(5 * time.Minute)       // JWT 续签 (过期前 5min)
    advTicker := time.NewTicker(5 * time.Minute)       // DHT re-Advertise
    defer jwtTicker.Stop()
    defer advTicker.Stop()
    for {
        select {
        case <-ctx.Done(): return
        case <-jwtTicker.C:
            // 向 JWT 服务请求续签 (独立 HTTP/gRPC 端点)
            // JWT 每次续签 1h 有效期, L4Backhaul 由白名单实时决定:
            //   PeerId 在 L4 白名单 → 新 JWT L4=true (自动续期 L4 权限)
            //   PeerId 不在 L4 白名单 → 新 JWT L4=false (白名单移除后 1h 内降级)
            // 续签失败时立即重试 (指数退避: 1s, 2s, 4s... 上限 30s)
            // JWT 过期前未续签成功 → 节点立即降级 (无宽限期)
            resp, err := d.jwtClient.RequestJWTWithRetry(ctx)
            if err == nil {
                d.jwt = resp.JWT
                // 通过 IdentifyPush 推送新 JWT 给已连接 peer (§3.2)
                d.host.Peerstore().PutData(d.host.ID(), "jwt", d.jwt)
            } else {
                // 续签失败: 检查 JWT 是否已过期
                if d.isJWTExpired() {
                    // JWT 已过期且续签失败 → 立即降级 (无宽限期)
                    log.Error("JWT expired and refresh failed, entering degraded mode", "err", err)
                    d.enterDegradedMode()
                }
                // JWT 未过期: 继续重试, 下次 ticker 触发时再试
            }
        case <-advTicker.C:
            // DHT re-Advertise 心跳 (TTL 续期)
            routing.Advertise(ctx, d.routingDisc, d.namespace, routing.TTL(15*time.Minute))
        }
    }
}
```

**JWT 签发服务（控制面侧，独立 HTTP/gRPC 端点）**：

```go
// JWT 签发服务 (与控制面共进程, 逻辑独立)
// 职责: 签发 JWT (独立 HTTP 端点, 不再与节点发现合并)
// DHT bootstrap 节点也是这个进程运行, 但 DHT 只做路由引导, 不承担认证
type JWTService struct {
    privKey         crypto.PrivKey  // 控制面 Ed25519 私钥, 用于签发 JWT
    l4Whitelist     *PeerIdSet      // L4 白名单 (人工认证后加入)
    rateLimiter     *rate.Limiter   // 同 IP 限速 1次/小时
    auditLog        *AuditLog
}

// 处理 JWT 签发请求: POST /v1/node/jwt
// 节点用 Ed25519 私钥签名 PeerID 证明身份 (防冒充)
func (s *JWTService) HandleJWTRequest(ctx context.Context, req JWTRequest) (*JWTResponse, error) {
    // 1. 验证签名: 用 PeerID 对应的公钥验签 req.SignedPeerID
    pubKey, err := extractPubKeyFromPeerID(req.PeerID)
    if err != nil { return nil, ErrInvalidPeerID }
    if !ed25519.Verify(pubKey, []byte(req.PeerID), req.SignedPeerID) {
        return nil, ErrInvalidSignature
    }

    // 2. 反滥用: 同 IP 限速 (1次/小时)
    remoteIP := getRemoteIP(ctx)
    if !s.rateLimiter.Allow(remoteIP, req.PeerID) {
        return nil, ErrRateLimited
    }

    // 3. 签发 JWT: L4Backhaul 仅对白名单 PeerId 开启
    isL4Whitelisted := s.l4Whitelist.Contains(req.PeerID)
    now := time.Now()
    payload := NodeJWTPayload{
        NodeID:       generateNodeID(),
        PeerID:       req.PeerID,
        Capabilities: NodeCapabilities{
            Edge:          true,
            L4Backhaul:    isL4Whitelisted,
            RelayProvider: false,  // 由后续自动探测决定
            PeerICP:       true,
        },
        BandwidthQuota: 50_000_000,
        Iat:          now.Unix(),
        Exp:          now.Add(1 * time.Hour).Unix(),  // ★ 1h 有效期
    }
    jwt := signJWT(payload, s.privKey)  // header.payload.sig

    // 4. 审计日志
    s.auditLog.Log("jwt_issue", req.PeerID, remoteIP, isL4Whitelisted)

    return &JWTResponse{
        JWT:           jwt,
        RefreshBefore: 5 * time.Minute,  // 过期前 5min 续签, 无宽限期
    }, nil
}
```

**DHT bootstrap 节点联邦**：3-5 个 bootstrap 节点通过 DHT 协议自然形成联邦——每个 bootstrap 节点都是 DHT 路由表的入口，节点连接任一 bootstrap 后即可通过 DHT 路由发现全网。任一 bootstrap 故障不影响发现（节点重试其他 bootstrap 地址）。

**GossipSub PeX 增量补充**：

```go
// GossipSub prune 消息捎带对端 peer (PeX)
// 节点被从某 peer 的 mesh 中 prune 时, 对方可附带最多 16 个替代 peer 的签名记录
// 节点收到 PeX 后, 校验签名并写入 PeerStore
func (e *EdgeNode) OnGossipSubPruneWithPX(p peer.ID, pxPeers []peer.PeerRecord) {
    for _, rec := range pxPeers {
        // 验证 peer record 签名 (libp2p 内置)
        if err := verifyPeerRecord(rec); err != nil { continue }
        e.peerStore.Put(PeerId(rec.ID), PeerStoreEntry{
            PeerID:   PeerId(rec.ID),
            Addrs:    rec.Addrs,
            LastSeen: time.Now().Unix(),
        })
    }
}
```

**与原 NODE_LIST_UPDATE 广播的对比**：

| 维度 | 原设计 | 新设计 |
|------|--------|--------|
| 节点列表来源 | 控制面广播 | DHT Advertise/FindPeers + PeX |
| 心跳机制 | 10s TCP 心跳 | 5min JWT 续签 + DHT re-Advertise |
| 故障摘除延迟 | 30s (3 次心跳失败) | DHT Advertise TTL 15min 过期 + GossipSub 评分实时降级 |
| 控制面故障影响 | 全网节点列表停止更新 | bootstrap 不可达时, 节点用 PeerStore 缓存继续运行 (§12) |
| NAT 后节点支持 | 不支持 (需被主动连接) | 支持 (DCUtR + relay) |
| 网络隔离 | 无 (依赖 IP 白名单) | PSK 编译时注入 (传输层密码学准入) |

### 10.3 缓存路由：一致性哈希 + libp2p ICP

一致性哈希路由逻辑不变（README.md §5.1 已描述），但**主节点判定从 nodeID 改为 PeerId**，302 跳转变更为 libp2p stream 转发。

```
客户端请求 blob_hash
  │
  │  DNS → 一致性哈希(blob_hash) % N → 主节点 PeerId
  v
主节点查本地缓存
  ├── hit → 直接返回
  └── miss → 查兄弟节点 (libp2p stream HEAD, 10ms 超时, §3.1)
              ├── 兄弟有 → 拉取 + 返回 + 本地缓存
              └── 兄弟无 → 按本节点能力分流:
                          +-- L4Backhaul=true  → 本地数据面回源
                          +-- L4Backhaul=false → libp2p stream 拉取 L4 节点
```

```go
// 一致性哈希路由 (节点侧, 非 control plane)
type HashRing struct {
    mu       sync.RWMutex
    ring     *consistenthash.Map
    selfPeer PeerId
}

// 请求到达任意节点时, 判断是否自己是主节点
func (n *EdgeNode) isPrimaryNode(blobHash string) bool {
    n.hashRing.mu.RLock()
    defer n.hashRing.mu.RUnlock()
    primary := n.hashRing.ring.Get(blobHash)
    return primary == string(n.hashRing.selfPeer)
}

// 非主节点: 通过 libp2p stream 转发到主节点 (非 HTTP 302)
// 原因: 302 要求客户端能直连主节点, 社区节点 NAT 后不一定可达
func (n *EdgeNode) HandleBlobRequest(w http.ResponseWriter, r *http.Request, blobHash string) {
    if !n.isPrimaryNode(blobHash) {
        n.hashRing.mu.RLock()
        primary := n.hashRing.ring.Get(blobHash)
        n.hashRing.mu.RUnlock()
        // 代理转发到主节点 (libp2p stream), 客户端无感
        n.proxyToPeer(w, r, peer.ID(primary), blobHash)
        return
    }
    // 主节点: 查本地 → 查兄弟 → 回源
    n.serveAsPrimary(w, r, blobHash)
}
```

**哈希环更新机制**：节点加入/离开时，**无中心广播**。各节点通过 DHT 周期性 Advertise/FindPeers + GossipSub PeX 增量更新本地 PeerStore，PeerStore 变更触发哈希环重建（README.md §6.1）。

```go
// 节点启动时: 从 DHT 拉取 peer 列表, 初始化 PeerStore + 哈希环
func (n *EdgeNode) initHashRing() error {
    // 1. DHT bootstrap + Advertise + FindPeers (§5.2)
    //    PSK 已在 libp2p.New 时注入, DHT 连接自动完成 PSK 握手
    if err := n.edgeDiscovery.Start(n.ctx); err != nil { return err }
    // 2. 从 PeerStore 重建哈希环 (§6.1)
    n.rebuildHashRing()
    return nil
}
```

**节点加入/离开的影响**：

| 场景 | 触发 | 哈希环更新 | 缓存影响 |
|------|------|----------|---------|
| 新节点加入 | DHT Advertise("edge") → 其他节点 FindPeers 发现 → PeerStore 写入 → 触发重建 | 新节点承担 ~1/N 的 blob 路由；原主节点的部分 blob 不再是主节点，缓存仍有效但不再被路由到 |
| 节点离开 | DHT Advertise TTL 过期 (15min) → FindPeers 不再返回 → PeerStore 标记 Stale → 触发重建 | 该节点的 blob 路由到新主节点 → miss → 回源 → 重新缓存 |
| 节点短暂抖动 | GossipSub 评分衰减 (§4.3) → 低于 GraylistThreshold 时哈希环剔除 | 抖动期间请求转发到不可达节点 → 超时 → 客户端重试 → DNS 降级 |
| 节点恢复 | 评分回升 + DHT Advertise 仍有效 → 重新进环 | 缓存仍有效，恢复正常路由 |

**虚拟节点配置**：

```yaml
# node-config.yaml
hash_ring:
  replicas: 150          # 每个物理节点的虚拟节点数 (默认 150)
  # 心跳由 DHT advertise_interval (5min) + JWT refresh_interval (5min) 承担, 不再单独配置
```

### 5.3 pin/unpin 下发：fire-and-forget（不变）

控制面下发 PinPlan 后**不跟踪执行结果**。节点收到后尽力执行，失败不影响正确性。PinPlan 下发仍走 SyncBroadcaster（控制面 → 节点的增量通道，与节点发现解耦）。

```go
// 控制面: 下发 PinPlan, 不等待确认
func (po *PinOrchestrator) sendNodePinPlan(np NodePinPlan) {
    plan := PinPlan{
        Seq:        po.nextSeq(),
        TargetNode: np.NodeID,
        Updates: []PinUpdate{{
            BlobHashes: np.PinBlobs,
            UnpinBlobs: np.UnpinBlobs,
        }},
    }
    // fire-and-forget: 发送后即认为最终一致
    po.broadcaster.SendToNode(np.NodeID, PIN_PLAN_UPDATE, plan)
}
```

**不做全量 pin 状态同步的原因**：
- pin 失败 → blob 走普通缓存 miss + 回源（功能完整，仅失去 prefix 加速）
- pin 重复下发 → 幂等（ApplyPin 重复调用无副作用）
- pin 未执行 → 下次 AdjustPin 重算时会再次下发

### 5.4 控制面需要的节点信息

控制面**只需要节点的空间统计和健康状态**，不需要 blob 级别的缓存/pin 列表：

```go
// 节点上报: 仅统计信息 (固定大小, 不随 blob 数量增长)
// 角色信息不在上报中, 控制面从签发的 JWT 记录中查 (§1.0)
type NodeStatusReport struct {
    NodeID       string
    PeerID       PeerId
    Capabilities NodeCapabilities  // 节点当前实际启用的能力 (供控制面对账)

    // 空间统计 (固定大小)
    PrefixSpace  PartitionStatus  // pin 分区
    WarmSpace    PartitionStatus  // 温缓存
    ColdSpace    PartitionStatus  // 冷缓存 (可选)

    // 健康状态
    Healthy      bool
    LastUpdate   time.Time
}

type PartitionStatus struct {
    TotalBytes  int64
    UsedBytes   int64
    BlobCount   int32  // 总数 (供策略层估算, 不上报具体列表)
}
```

**上报频率**：30s 一次，体积 < 200 字节/节点。10 节点 × 200B = 2KB，可忽略。上报走 SyncBroadcaster 的反向通道（节点 → 控制面），与节点发现的 DHT 通道独立。

### 5.5 策略层决策：按需查询（不变）

PinStrategy 做决策时，需要知道目标节点的空间和 pin 情况。空间从 `NodeStatusReport` 获取（控制面已维护）。pin 情况**按需 RPC 查询**目标节点（RPC 走 SyncBroadcaster 通道，非 libp2p stream）：

```go
// PinStrategy 做决策时, 主动查询目标节点的 pin 状态
// 只在 DecideInitialPin / AdjustPin 时查询, 不持续维护
func (po *PinOrchestrator) getNodePinSpace(nodeID string) (PinSpaceInfo, error) {
    // RPC 查询节点: "你的 prefix 分区还剩多少空间? 当前 pin 了多少 blob?"
    // 不查"具体 pin 了哪些 blob" — 那不需要
    return po.rpcClient.QueryPinSpace(nodeID)
}

// 节点侧: 响应 pin 空间查询
func (n *EdgeNode) HandleQueryPinSpace() PinSpaceInfo {
    return PinSpaceInfo{
        AvailableBytes: n.pinStore.storage.Available(),
        PinnedCount:    n.pinStore.pinCount(),    // 只返回数量
        TotalPinnedSize: n.pinStore.totalPinnedSize(),
    }
}
```

**决策流程**：
1. PinStrategy 收到 ContentIngestedEvent
2. 查各节点的 `NodeStatusReport`（控制面已维护的空间统计）
3. 如需更精确的 pin 空间信息，RPC 查询目标节点
4. 产出按节点差异化的 PinPlan
5. fire-and-forget 下发

### 5.6 数据量对比

| 方案 | 控制面内存 | 上报带宽/30s | 决策延迟 |
|------|----------|------------|---------|
| 全量 Bloom Filter (原方案) | 630MB (10节点×63MB) | 630MB | 0 (本地查) |
| **DHT + 按需查询 (当前)** | **< 1KB** (10节点×200B统计) + DHT 路由表 (~50KB) | **2KB** | 5ms/节点 (RPC 查 pin 空间) |

5243 万 blob 的场景下，按需查询方案内存和带宽优势 3 个数量级，决策延迟仅多 5ms（同区域 RPC）。DHT 路由表内存开销与节点数线性相关（每节点 ~100B），可忽略。



---

## 6. 负载均衡与 NAT 穿透

### 6.1 非 L4 节点到 L4 节点的负载均衡

原设计由调度层下发同区 L2+L4 节点列表。新设计改为节点**从本地 PeerStore 筛选** `L4Backhaul=true` 的 peer（PeerStore 来源见 §5.2 DHT 发现）。后续可基于 IP 地理位置和节点质量推荐优化 L4 节点选择（TODO）。

```go
// 非 L4 节点: 从 PeerStore 筛选 L4 节点, 负载均衡选择
func (e *EdgeNode) FetchFromL4Node(ctx context.Context, blobHash string) (io.Reader, error) {
    // 1. 筛选 L4Backhaul=true + 评分达标的 peer (全网, 后续可加地理推荐)
    candidates := e.peerStore.Filter(func(p PeerStoreEntry) bool {
        return p.Capabilities.L4Backhaul &&
               p.Score >= GraylistThreshold &&
               !p.Stale
    })
    if len(candidates) == 0 {
        return nil, ErrNoL4NodeAvailable
    }

    // 2. round-robin + least-conn 选择
    target := e.l4Selector.Select(candidates)  // 考虑活跃连接数 + 评分

    // 3. libp2p stream 拉取 (复用已建立连接, 多路复用)
    stream, err := e.host.NewStream(ctx, peer.ID(target.PeerID), edgeFetchFromL4Proto)
    if err != nil {
        e.peerScorer.RecordICPTimeout(target.PeerID)
        // 标记该 peer 短期不可用, 重试下一个候选
        e.l4Selector.MarkUnavailable(target.PeerID, 30*time.Second)
        return e.FetchFromL4Node(ctx, blobHash)  // 递归尝试下一个
    }
    defer stream.Close()

    proto.WriteVarint(stream, uint64(len(blobHash)))
    stream.Write([]byte(blobHash))
    return stream, nil  // 流式返回, 调用方边读边写本地缓存
}
```

**与原设计的差异**：
- 节点列表来源：调度层推送 → PeerStore 筛选（去中心化）
- 协议：gRPC → libp2p stream（NAT 后也能用，经 relay）
- 故障感知：TCP RST → libp2p 连接失败 + 评分衰减（双信号）
- L4 节点本身的回源不涉及负载均衡——直接调用本地数据面

### 6.2 NAT 穿透栈（社区节点硬需求）

社区节点大概率在 NAT 后（家宽、动态 IP、共享 IP）。原设计假设节点公网可达，对社区节点不适用。引入 libp2p 完整 NAT 穿透栈（go-libp2p 全部内置）：

```
节点启动
  │
  1. AutoNAT: 主动探测自身 NAT 状态
  │    └── 请求若干已连接 peer 反向拨号自己的疑似公网地址
  │    └── 若都被拒绝 → 判定 NAT 后
  │
  2. 若 NAT 后 → AutoRelay: 发现公共 relay 节点, 预约中转
  │    └── relay 节点 = RelayProvider=true 的公网节点 (自建 L4 节点)
  │    └── Circuit Relay v2: relay 转发 inbound/outbound 连接
  │    └── 资源限制: 每节点最多 N 个 relay 连接, 每个 T 秒/B 字节
  │
  3. DCUtR: 经 relay 协调, 双方同时拨号打洞
  │    └── A 经 relay 发 Connect(自己的 addr) 给 B
  │    └── B 经 relay 回 Connect(自己的 addr)
  │    └── A 测 RTT, 等 RTT/2, 直连拨号 B
  │    └── B 收到 A 的 Sync, 同时拨号 A
  │    └── 同时拨号 → NAT 映射洞口已开 → 直连成功
  │
  4. 打洞成功 → 后续流量走直连 (低延迟)
     打洞失败 → 走 relay 中转 (兜底, 延迟 +20-50ms)
```

```go
// libp2p host 构造时启用 NAT 穿透栈
func NewEdgeHost(identity *NodeIdentity, cfg Libp2pConfig) (host.Host, error) {
    return libp2p.New(
        libp2p.Identity(identity.PrivKey),
        libp2p.ListenAddrStrings(cfg.Listen...),
        libp2p.NATTraversal(
            nat.AutoNATService(true),      // 提供 AutoNAT 探测服务
            nat.AutoRelay(true),           // NAT 后自动预约 relay
            nat.AutoRelayInterval(5*time.Minute),
        ),
        // Circuit Relay v2: 公网节点作 relay provider
        libp2p.EnableRelay(relay.WithResources(relay.DefaultResources())),
        libp2p.EnableHolePunching(),      // DCUtR
        libp2p.ConnectionGater(&EdgeConnectionGater{...}),  // §3.2
    )
}
```

**对带宽规划的影响**（详见 README.md §9.4）：
- relay provider（公网自建节点）承担社区节点中转流量，需纳入出口带宽规划
- DCUtR 打洞成功率：对称 NAT 较低（约 60%），其他 NAT 类型 > 85%
- 打洞失败的社区节点走 relay 兜底，relay 带宽成本按社区节点数 × 平均流量估算

**relay 角色配置**：见 §2.3 `relay_provider` 能力。公网自建节点默认启用 `relay_provider=true`；社区节点 NAT 后无法作 relay，配置 `relay_provider=false`。


---


---
