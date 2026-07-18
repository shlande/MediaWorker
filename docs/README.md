# DASH 网盘流媒体系统 — 主架构文档

> 本文档为系统主文档，覆盖全局架构、多域交互点、端到端场景串联。领域细节请参阅：
> - **分发方向**（读路径）：[`distribution/README.md`](distribution/README.md)（缓存 + 回源 + 路由 + 容灾）
> - **分发域 — 网络与准入层**：[`distribution/network.md`](distribution/network.md)（libp2p + JWT + 节点发现 + ConnectionGater + GossipSub 评分 + NAT 穿透）
> - **数据存储方向**（段冗余 + 元数据 + 接入层）：[`storage/README.md`](storage/README.md)
> - **入库链路**（预分片 + 冗余上传）：[`ingest/README.md`](ingest/README.md)
> - **链路策略控制**（Backend 抽象 + 用量采集 + 回源/上传权重）：[`policy/README.md`](policy/README.md)
> - **能力诉求与架构承接度分析**：[`../能力诉求与架构承接度分析.md`](../能力诉求与架构承接度分析.md)

---

## 目录

1. [系统概述与设计目标](#1-系统概述与设计目标)
2. [总体架构](#2-总体架构)
3. [两域交互点](#3-两域交互点)
4. [端到端场景串联](#4-端到端场景串联)
5. [容灾恢复目标（RTO）](#5-容灾恢复目标rto)
6. [部署拓扑](#6-部署拓扑)
7. [技术选型清单](#7-技术选型清单)
8. [容量规划概要](#8-容量规划概要)
9. [目标态 vs 实现态（target state vs implemented state）](#9-目标态-vs-实现态target-state-vs-implemented-state)

---

## 1. 系统概述与设计目标

### 1.1 业务场景

本系统面向一个私有的视频点播（VOD）流媒体平台。视频内容预先转码为多码率 DASH 分片，媒体段文件存储于**多个消费者网盘厂商**上，利用多账号冗余实现"近乎免费"的分布式存储和带宽汇聚。客户端为符合 MPEG-DASH 标准的播放器，通过边缘节点获取 MPD 清单和媒体段，实现自适应码率播放。

### 1.2 规模与 SLO

| 指标 | 目标值 | 说明 |
|------|--------|------|
| 并发观众 | 50+ / 边缘节点 | 单个边缘节点承载 |
| 边缘节点数 | 3-5 个 L4 节点 + 弹性非 L4 节点 | 地理分散部署，社区可运营非 L4 节点 |
| 视频总量 | 10 万+ | 持续增长 |
| **首帧延迟 P95** | **< 3s** | 从用户点击播放到首帧渲染 |
| **段缓存命中率（热数据）** | **> 80%** | 最近 24h 访问过的视频 |
| **回源成功率** | **> 99%** | L4 节点数据面到网盘 |
| 账号池健康率 | > 80% | healthy 状态占比 |

### 1.3 SLA 边界与不可恢复场景

上述 SLO 覆盖**独立故障**（单机崩溃、单节点网络抖动、单账号被封、单厂商全挂），由 2+ 冗余 + 健康检查 + 熔断器 + K=2 跨厂商冗余保障，RTO 见 §5。

本系统**不覆盖**以下**相关故障**，发生时服务中断，不自动恢复，需人工介入：

| 不可恢复场景 | 触发条件 | 影响 | 检测 |
|-------------|---------|------|------|
| 区域级 L4 全部故障 | 机房出口 IP 段被网盘厂商批量封禁 | 该区域非 L4 节点回源全部失败，客户端 503 | `RegionalL4AllDown` 告警 |
| 区域级 L4 全部故障 | 区域网络分区（BGP 故障、运营商互联故障） | 同上 | 同上 |
| 区域级 L4 全部故障 | 控制面级联下发错误凭证/健康事件，所有 L4 同时失效 | 同上 | 同上 |

> **注**：节点角色已从离散的 `L2+L4` / `L2-only` 改为能力模型（详见 [`distribution/README.md §1`](distribution/README.md)）。"L4 节点"指 `L4Backhaul=true` 的节点（通常是自建公网节点）。调度层单点故障**不再属于不可恢复场景**——节点发现已去中心化（PSK 私有网络 + DHT + GossipSub PeX），调度层失联时节点用本地 PeerStore 缓存继续运行，RTO 5-10min（详见 [`distribution/README.md §12.5`](distribution/README.md)）。

### 1.4 设计原则

**网盘不可靠假设**：不信任任何单一账号、单一厂商。冗余、熔断、降级是核心骨架。

**最终一致**：`blob_location` 写入是原子事务，跨厂商副本允许秒级差异。多节点间的账号限流协调、健康状态共享均采用最终一致模型，不依赖强一致协调服务。

**代理回源统一**：所有网盘到客户端的数据流经过"接入层数据面"代理。接入层是**逻辑分层**——控制面集中部署，数据面下沉到 L4 节点本地。

**节点能力分层**（取代原角色枚举，详见 [`distribution/README.md §1`](distribution/README.md)）：
- **L4 节点**（`L4Backhaul=true`，token 授权，通常自建公网）：边缘分发 + 网盘回源，本地运行完整数据面，回源流量从本机出口直连网盘
- **非 L4 节点**（`L4Backhaul=false`，社区或自建）：仅边缘分发，缓存未命中时通过 libp2p stream 拉取同区 L4 节点

角色是能力配置，同一套二进制按 JWT（由控制面签发）启用不同能力。详见 [`distribution/README.md §1`](distribution/README.md)。

**最小冷启动**：入库时主动推送 prefix 到边缘节点，首帧大概率不需要回源。推送策略已根据 P0-C13 修订为区域代表性推送 + 被动拉取 + 动态段数。详见 [`distribution/README.md §9`](distribution/README.md)。

---

## 2. 总体架构

系统从两个角度观测：**分发方向**（读路径）和**数据存储方向**（写路径/存储）。两者通过"段位置元数据"解耦。

```
              ┌───────────────────────────────────────────────────┐
              │              L1  客户端层 (DASH Player)             │
              └──────────────────────┬────────────────────────────┘
                                     │
              ┌──────────────────────┴────────────────────────────┐
              │              L2  边缘节点层 (能力模型, 非角色枚举)    │
              │                                                     │
               │   L4 节点 (L4Backhaul=t)    非 L4 节点 (L4Backhaul=f)│
               │   ┌─────────────┐         ┌─────────────┐          │
               │   │ 缓存+分发    │         │ 缓存+分发    │          │
               │   │ ★本地数据面★│         │ (无Driver)  │          │
               │   │ Drivers×5   │         │ libp2p→L4   │          │
               │   │ AccountPool │         │ +NAT穿透    │          │
               │   │ LinkPool    │         │ +DCUtR/relay│          │
               │   │ 限流/熔断    │         └─────────────┘          │
               │   │ +Relay v2   │                                  │
               │   └──────┬──────┘                                  │
              └──────────┼─────────────────────────────────────────┘
                         │ 网盘直连(本地出口IP)    libp2p stream
                         │                          │
              ┌──────────┴──────────┐  ┌───────────┴──────────────┐
              │  L4-C 控制面 (中心)  │  │  L3 调度层                │
              │  账号主库/凭证下发   │  │  DNS+302 / 健康聚合       │
              │  健康汇总/PinOrch    │  │  (节点发现已去中心化)      │
              │  Ingest编排          │  └──────────────────────────┘
              │  SyncBroadcaster     │
              │  ★DHT bootstrap + PSK│
              │  ★JWT签发│
              └──────────┬───────────┘
                         │
              ┌──────────┴──────────────────────────────────────────┐
              │  L5 元数据服务                                       │
              │  PostgreSQL(主从) + Redis(哨兵)                      │
│  [存储域] blob / blob_location / 账号健康              │
│  [元数据模块] content / content_blob (编排)            │
│  [分发域] content_popularity 热度表                    │
│  分发域通过 content_id (元数据模块) + blob_hash (storage) 关联键解耦           │
              └──────────┬──────────────────────────────────────────┘
                         │
              ┌──────────┴──────────────────────────────────────────┐
              │  L6 网盘存储层                                       │
              │  115×5  百度×10  夸克×5  OneDrive×3  阿里×3         │
              │  K=2 跨账号跨厂商冗余副本                            │
              └─────────────────────────────────────────────────────┘
```

**两个观测角度**：

| 角度 | 文档 | 核心问题 | 数据流向 |
|------|------|---------|---------|
| **分发方向** | [`distribution/README.md`](distribution/README.md) | 缓存有限的节点选择、回源策略、账号/网盘选择 | client → 边缘 → 兄弟/网盘 |
| **数据存储方向** | [`storage/README.md`](storage/README.md) | 段文件冗余存储、元数据管理、接入层设计 | ingest写入 → 网盘存储 → 元数据记录 |
| **入库链路** | [`ingest/README.md`](ingest/README.md) | 预分片、冗余上传、事务保证、prefix触发 | 源视频 → 分片 → 多账号上传 → 元数据 |
| **链路策略控制** | [`policy/README.md`](policy/README.md) | Backend 抽象、用量采集、回源/上传权重计算 | 用量上报 → 权重计算 → 下发 → 影响读+写 |

---

## 3. 两域交互点

分发方向和存储方向通过以下交互点协作。这些交互点是系统设计的关键解耦点。

### 3.1 回源时查段位置（分发 → storage 内容寻址层）

```
distribution 层                          storage 层 (内容寻址层)
L4 节点本地缓存 miss                  blob 位置表 (blob_location)
    │                                        │
    │  GetBlobLocations(blob_hash)           │
    ├───────────────────────────────────────>│
    │  [(115:acct_03,fid_xxx),              │
    │   (baidu:acct_07,fid_yyy)]            │
    │<───────────────────────────────────────┤
    │                                        │
    │  选账号(VendorProfile权重)             │
    │  取链接(LinkPool)                      │
    │  本地出口请求网盘                      │
    │  流式返回客户端                        │
```

- 分发层的 L4 节点本地缓存 blob 位置 hot subset（由 storage 层控制面 SyncBroadcaster 推送，以 `blob_hash` 为 key）
- 本地 miss 时查 storage 层的 PG 从库，单键 `blob_hash` 查询
- 选账号逻辑用 storage 层的 VendorProfile 权重 + 健康状态 + 限流令牌（详见 [`storage/README.md §4`](storage/README.md)）

> **层次说明**：storage 域采用内容寻址，`blob_hash = SHA-256(文件内容)`，全局唯一、不携带业务语义。客户端发起请求前，先经元数据模块（`content` + `content_blob` 关联表）解析"播放视频 X 的 720p 段 3"得到对应的 `blob_hash`，再用此 `blob_hash` 走分发链路。元数据模块理解 content 是什么（DASH 视频 / 图床 / PDF），把码率、段序、缩略图尺寸等业务语义编码在 `content_blob.role` / `sort_order` / `business_meta` 中。详见 [`storage/README.md §9.1`](storage/README.md) 的分层 schema。

### 3.2 入库事件通知（入库 → 分发）

入库完成后，ingest 域发布 `ContentIngestedEvent`（含 ContentID、ContentType、Blobs 及其 BlobType）。ingest 域不给出任何 pin 建议，pin 决策完全由分发域策略层根据 BlobType + 全局流行度自行决定。

```
ingest 层                                distribution 层
入库完成                                  PinOrchestrator
    │                                        │
    │  ContentIngestedEvent                  │
    │  {blob_hash, content_type,            │
    │   blob_hashs, pin_hints}                 │
    ├───────────────────────────────────────>│  按内容类型决策:
    │                                        │    DASH: pin init+前N段
    │                                        │    图床: pin 缩略图
    │                                        │    文档: pin 第一页
    │                                        │
    │                                        │  PinPlan → 推送代表节点
    │                                        │  其他节点首次访问时被动拉取
```

- ingest 域只产出 blob + BlobType 分类，不参与 pin 决策。分发域策略层基于全局信息（流行度、缓存空间）自行决定 pin 什么、pin 多少
- 通知是异步的（通过 SyncBroadcaster），不阻塞入库响应
- 新增内容类型时，ingest 实现自己的 ContentIngester 产出 blob + BlobType，分发域注册对应 PinStrategy
- 详见 [`ingest/README.md §4`](ingest/README.md) 和 [`distribution/README.md §9`](distribution/README.md)

### 3.3 流行度反馈（分发域内部闭环）

热度数据完全属于分发域，不依赖 storage 域，也不依赖元数据模块。分发域通过 `content_id`（content 维度热度，与元数据模块关联）和 `blob_hash`（blob 维度本地热度，与 storage 域关联）作为双关联键。

```
distribution 域 (内部闭环)                    元数据模块 / storage 域
                                                  │
  本地 gossip 热度 (30s, 分钟级, blob 维度)        │
  ┌─────────────────────────┐                    │
  │ 节点A热度 ──┐            │                    │
  │ 节点B热度 ──┼─> 合并视图  │                    │
  │ 节点C热度 ──┘  (取max)   │                    │
  │ key=blob_hash             │                    │
  └─────────────────────────┘                    │
        │                                         │
        │  驱动本地逐出决策 (1min内生效)           │
        │                                         │
        │  ReportAccess(gRPC, 异步, content_id+blob_hash)
        v                                         │
  PG content_popularity (分发域表, content 维度)  │
  pg_cron 每小时重算 window_24h                   │
        │                                         │
        │  驱动 PinOrchestrator (10min重算)        │
        │  热门→扩展prefix, 冷门→降级prefix        │
        │  PinPlan 事件 (增量下发)                 │
        v                                         │
  更新本地 PinManifest                            │
                                                  │
  ──────── 双关联键 ──────────────────────────────┤
  content_id (元数据模块 content 表)              │
  blob_hash  (storage 域 blob / blob_location)    │
  (分发域只传 ID, 不读写其他域的表)               │
                                                  │
  回源时: distribution 用 blob_hash ──────────────┤
  查 storage 的 blob_location                    │
                                                  │
  PinOrchestrator 决策时: 用 content_id ──────────┤
  查元数据模块 content_blob 拿到该 content 的所有 │
  blob + role/sort_order, 然后下发 PinPlan       │
```

- **分钟级**：节点间 gossip 热度同步（blob 维度），驱动本地逐出和预取决策（延迟 < 1min）
- **小时级**：PG `content_popularity` 的 `window_24h`（content 维度），驱动控制面 PinOrchestrator 做全局 prefix/pin 决策
- **关联键**：
  - `content_id`（content 维度热度表主键）—— 用于 PinOrchestrator 按 content 维度决策，并查元数据模块拿该 content 的 blob 列表
  - `blob_hash`（blob 维度本地热度 + 回源查位置）—— 用于本地缓存逐出决策和回源时查 `blob_location`
  - 元数据模块的 `content_blob` 关联表把两个键桥接起来
- 热度表 Schema 和 gRPC 接口详见 [`distribution/README.md §8.1`](distribution/README.md)
- 回源查段位置（用 `blob_hash` 关联 storage 域）见 [`storage/README.md §10`](storage/README.md)

### 3.4 账号健康传播（存储内部 → 分发）

```
storage 层 (L4 节点本地数据面)          distribution 层 (回源决策)
    │                                          │
    │  本地探测 (30s)                           │
    │  ReportAccountHealth                     │
    ├────> 控制面 HealthAggregator             │
    │      汇总全局视图                         │
    │                                          │
    │  SyncBroadcaster 增量下发 (<1s)          │
    ├──────────────────────────────────────────>│  本地 AccountPool 更新健康状态
    │  BAN 事件 (增量, <1s)                    │  回源选账号时跳过 banned 账号
    ├──────────────────────────────────────────>│  本地 CircuitBreaker 强制 OPEN
```

- 健康探测在 storage 层的 L4 节点本地完成（每 30s）
- 控制面汇总后通过同步协议下发全局视图
- BAN 事件（封号）通过增量通道 < 1s 下发
- 分发层的回源决策读取本地 AccountPool 的健康状态（由 storage 层同步协议维护）
- 最终一致窗口 6-10s，对封号场景可接受
- 详见 [`storage/README.md §8`](storage/README.md) 和 [`storage/README.md §9`](storage/README.md)

### 3.5 链路策略控制（独立域，影响回源 + 上传）

网盘账号和本地持久存储统一抽象为 **Backend**。控制面的 **PolicyController** 根据各 Backend 的用量指标动态计算**回源权重**（影响分发域 SelectForRead）和**上传权重**（影响 ingest 域 SelectK），通过 SyncBroadcaster 下发。这是一个独立领域，不属于 storage（只管存储和元数据）、distribution（只管缓存和分发）或 ingest（只管入库流程）。

```
L4 节点 (数据面本地采集)               控制面 (PolicyController)
┌──────────────────────────┐             ┌──────────────────────────┐
│ CloudDriveBackend        │             │                          │
│  - reqCounter (API 次数)  │  30s 上报   │  聚合各节点用量           │
│  - readBytes (回源流量)   │──────────> │  按 BackendType 计算:     │
│  - storageUsed (存储量)   │             │    网盘: 次数+流量+风控+存储│
│                          │             │    本地: IOPS+流量+存储   │
│ LocalStorageBackend      │             │                          │
│  - diskIOPS (磁盘利用率)  │             │  输出两类权重:            │
│  - readBytes (回源流量)   │             │    ReadWeight  → 分发域   │
│  - storageUsed (存储量)   │             │    UploadWeight → 入库域  │
└──────────────────────────┘             │    Writable → 入库域      │
                                        └────────────┬─────────────┘
                                                      │
                                         SyncBroadcaster 增量下发
                                         BACKEND_WEIGHT_UPDATE
                                                      │
                          ┌───────────────────────────┴───────────────────┐
                          v                                                 v
          distribution 域 (回源选择)                          ingest 域 (上传选择)
          SelectForRead 读 ReadWeight                        SelectK 读 UploadWeight + Writable
          score = load / read_weight                         按 upload_weight 降序 + 跨厂商
```

**关键设计**：
- **独立领域**：策略控制横跨读路径和写路径，不属于任何单一域
- **统一抽象**：网盘账号和本地持久存储都是 Backend，PolicyController 对所有 Backend 统一计算
- **双权重**：回源权重（关注流量/次数/风控）和上传权重（关注存储量/风控）独立计算，阈值不同
- **本地存储扩展**：未来新增本地持久存储时，只需实现 Backend 接口 + 注册，无需改 PolicyController 核心逻辑

详见 [`policy/README.md`](policy/README.md)。

---

## 4. 端到端场景串联

以下场景跨分发和存储两个域，展示完整的数据流向。

### 场景 A：首帧播放 — Prefix Cache 命中

```
 客户端(Player)          节点(L4或非L4)     元数据服务      (无回源)
     │                     │                       │                │
     │  GET /mpd/abc123    │                       │                │
     ├────────────────────>│                       │                │
     │                     │  GetMPD("abc123")     │                │
     │                     ├──────────────────────>│                │
     │                     │  MPD XML              │                │
     │                     │<──────────────────────┤                │
     │  HTTP 200 MPD       │                       │                │
     │<────────────────────┤                       │                │
     │                     │                       │                │
     │  GET /seg/abc123/init/720p   (init segment)│                │
     ├────────────────────>│                       │                │
     │                     │ [prefix cache HIT]    │                │
     │  HTTP 200 (init)    │  (PinManifest 管理的 pinned 段)        │
     │<────────────────────┤                       │                │
     │                     │                       │                │
     │  GET /seg/abc123/1/720p    (segment 1)     │                │
     ├────────────────────>│                       │                │
     │                     │ [prefix cache HIT]    │                │
     │  HTTP 200 (seg1)    │                       │                │
     │<────────────────────┤                       │                │
     │                     │                       │                │
     │  ███ 首帧渲染 < 500ms ███                   │                │
```

prefix cache 由 PinOrchestrator 动态管理（详见 [`distribution/README.md §9`](distribution/README.md)）。

### 场景 B：缓存未命中 → 流式回源

#### B1：L4 节点本地回源

```
 客户端          L4 节点              元数据(查段位置)      网盘(115)
     │              │                       │                   │
     │ GET seg3     │                       │                   │
     ├─────────────>│                       │                   │
     │              │ [本地缓存 MISS]        │                   │
     │              │ [兄弟节点 MISS]        │                   │
     │              │                       │                   │
     │              │ ★ 调用本地数据面 ★    │                   │
     │              │ GetBlobLocations      │                   │
     │              ├──────────────────────>│                   │
     │              │ [(115,acct3,fid_xxx), │                   │
     │              │  (baidu,acct7,fid_yyy)]│                   │
     │              │<──────────────────────┤                   │
     │              │                       │                   │
     │              │ [VendorProfile 权重选账号: 115-acct3]     │
     │              │ [本地链接池: 命中]     │                   │
     │              │ HTTP GET (本机出口 IP)│                   │
     │              ├──────────────────────────────────────────>│
     │              │  200 OK, streaming...                     │
     │              │<──────────────────────────────────────────┤
     │  HTTP 200    │ [边收边写本地缓存]     │                   │
     │  chunk1...   │                       │                   │
     │<─────────────┤                       │                   │
     │  ...         │                       │                   │
     │              │                       │                   │
     │  端到端首字节 ≈ 140ms (本地查缓存+兄弟HEAD+元数据+网盘首字节)         │
```

回源账号选择逻辑详见 [`storage/README.md §4`](storage/README.md)，stream-through 详见 [`distribution/README.md §4`](distribution/README.md)。

#### B2：非 L4 节点代理拉取

```
 客户端        非 L4 节点        L4 节点(同区)      元数据         网盘
     │              │                   │                  │             │
     │ GET seg3     │                   │                  │             │
     ├─────────────>│                   │                  │             │
     │              │ [本地缓存 MISS]    │                  │             │
     │              │ [兄弟节点 MISS]    │                  │             │
     │              │                   │                  │             │
     │              │ ★ gRPC 拉取 ★     │                  │             │
     │              │ FetchFromL2L4(seg3)│                  │             │
     │              ├──────────────────>│                  │             │
     │              │                   │ (同场景 B1)      │             │
     │              │                   ├─────────────────────────────>│
     │              │                   │  200 OK streaming             │
     │              │                   │<─────────────────────────────┤
     │              │ gRPC stream        │                  │             │
     │              │<──────────────────┤                  │             │
     │  HTTP 200    │ [边收边写本地缓存] │                  │             │
     │<─────────────┤                   │                  │             │
     │  ...         │                   │                  │             │
     │              │                   │                  │             │
     │  端到端首字节 ≈ 155ms (多一跳区域内 gRPC)                            │
```

### 场景 C：账号熔断 → 冗余切换

```
 非 L4 节点         L4 节点              元数据             115网盘
     │                   │                       │                  │
     │  FetchFromL2L4(   │                       │                  │
     │    blob_hash=     │                       │                  │
     │    "abc_4")       │                       │                  │
     ├──────────────────>│                       │                  │
     │                   │ [本地数据面接管]       │                  │
     │                   │ GetBlobLocations      │                  │
     │                   ├──────────────────────>│                  │
     │                   │ [(115,acct2,fid_a),   │                  │
     │                   │  (baidu,acct5,fid_b)] │                  │
     │                   │<──────────────────────┤                  │
     │                   │                       │                  │
     │                   │ [VendorProfile: 115-acct2 权重高, 优先]  │
     │                   │ [本地熔断器: OPEN! (5×403, 熔断至 12:35)] │
     │                   │ [本地配额广播: 已收到 ban 事件]            │
     │                   │                       │                  │
     │                   │ [降级: baidu-acct5]   │                  │
     │                   │ [本地链接池: 命中]     │                  │
     │                   │ HTTP GET (本机出口IP, Cookie+UA)          │
     │                   ├──────────────────────────────────────────>│
     │                   │  200 OK streaming                        │
     │                   │<──────────────────────────────────────────┤
     │  gRPC chunks...   │                       │                  │
     │<──────────────────┤                       │                  │
     │                   │                       │                  │
     │  [对客户端透明]   │ [ban 事件异步上报控制面 → 下发其他节点]    │
```

熔断器详见 [`storage/README.md §6`](storage/README.md)，冗余切换在 L4 节点本地完成，对非 L4 节点和客户端透明。

### 场景 D：新视频入库

```
 管理后台        控制面(Ingest编排)      Ingest Worker        元数据        网盘(×K)
      │                   │                   │                 │             │
      │  POST /ingest     │                   │                 │             │
      │  {video_file}     │                   │                 │             │
      ├──────────────────>│                   │                 │             │
      │                   │ 分发任务给 Worker  │                 │             │
      │                   ├──────────────────>│                 │             │
      │                   │                   │ [1. 转码为5码率DASH]          │
      │                   │                   │ [2. 生成MPD XML]              │
      │                   │                   │                               │
      │                   │                   │ [3. 并发上传到K=2个冗余账号]  │
      │                   │                   │  goroutine 1: Put(115-acct3) │
      │                   │                   ├──────────────────────────────>│
      │                   │                   │  goroutine 2: Put(baidu-acct5)│
      │                   │                   ├──────────────────────────────>│
      │                   │                   │  ...并行上传所有 blob...      │
      │                   │                   │                               │
      │                   │                   │ [4. 事务写入元数据]           │
      │                   │                   │  BEGIN TRANSACTION            │
      │                   │                   │   INSERT blob (去重)          │
      │                   │                   │   INSERT blob_location (K=2)  │
      │                   │                   │   INSERT content              │
      │                   │                   │   INSERT content_blob         │
      │                   │                   │  COMMIT                       │
      │                   │                   ├────────────────>│             │
      │                   │                   │                 │             │
      │                   │                   │ [5. 发布 ContentIngestedEvent]│
      │                   │                   │  → 分发域 PinOrchestrator 监听│
      │                   │                   │  → 按内容类型决定 pin 哪些blob│
      │                   │                   │  → 推送到区域代表节点 (异步)  │
      │  HTTP 201 {content_id}                  │                 │             │
      │<──────────────────┤                   │                 │             │
```

入库流程详见 [`ingest/README.md`](ingest/README.md)，prefix 推送策略详见 [`distribution/README.md §9`](distribution/README.md)。

---

## 5. 容灾恢复目标（RTO）

| 故障场景 | RTO | 影响范围 | 详见 |
|---------|-----|---------|------|
| 单账号被封 | **0s**（本地熔断器自动切换冗余账号） | 该账号请求增加 ~1ms 选择延迟 | [storage §6](storage/README.md) |
| 单厂商全挂 | **< 30s**（健康检查 → degraded → 广播） | 降级到其他厂商冗余副本 | [storage §10](storage/README.md) |
| 单 L4 节点挂 | **< 30s**（GossipSub 评分衰减 + DHT Advertise TTL 过期，自动从 PeerStore 摘除） | 该节点客户端重试到同区其他 L4 | [distribution §12](distribution/README.md) |
| 调度层失联（节点发现入口不可达） | **5-10min**（节点用 PeerStore 缓存继续运行；恢复后重新连接 DHT bootstrap + 续签 JWT） | 新节点无法加入；已有节点继续服务，JWT 过期前未续签则降级 | [distribution §12.5](distribution/README.md) |
| 整区域 L4 全挂（相关故障） | **不覆盖** | 该区域非 L4 节点回源全部失败，客户端 503 | [§1.3](#13-sla-边界与不可恢复场景) |
| 控制面挂（凭证/PinPlan 下发） | **5-10min**（凭证本地 TTL 5min 内正常运转，PinPlan 停止但已有 pin 保持） | 新凭证轮换暂停，已有凭证继续工作 | [storage §10](storage/README.md) |
| PG 主库挂 | **< 2min**（Patroni 自动 failover）—— **目标态**：写暂不可用，读走 Redis 不受影响。**实现态**：Redis 缓存未采纳（见 §9.1），读路径实际退化为控制面 `GET /v1/blob-locations/{hash}` + PG 从库直查，PG 主库挂时读仍可用（从库承担），仅写（新入库）暂不可用 | 写暂不可用，读延迟可能 +10ms（从库直查无 Redis 缓存） | [storage §10](storage/README.md) |
| Redis 挂 | **< 30s**（哨兵自动 failover）—— **目标态**。**实现态**：Redis 未采纳（见 §9.1），不存在 Redis 故障场景；该行仅在重新评估采纳 Redis 后生效 | 部分读回退 PG 从库，延迟 +10ms | [storage §10](storage/README.md) |

---

## 6. 部署拓扑

```
┌────────────────────────────────────────────────────────────────────┐
│                          INTERNET                                  │
│   ┌──────┐    ┌──────┐    ┌──────┐    ┌──────┐                     │
│   │Player│    │Player│    │Player│    │Player│    ...               │
│   └──┬───┘    └──┬───┘    └──┬───┘    └──┬───┘                     │
│      │    DNS GeoIP → nearest edge (按区域, 不区分能力)              │
└──────┼───────────┼───────────┼───────────┼──────────────────────────┘
       │   ┌───────┴───────┐   │           │
       v   v               v   v           v
┌────────────────┐  ┌─────────────────┐  ┌─────────────────┐
│ L4-01 北京     │  │ L4-03 广州      │  │ L4-05 新加坡    │
│ (分发+回源+relay)│  │ (分发+回源+relay)│  │ (分发+回源+relay)│
│ Prefix NVMe    │  │ Prefix NVMe     │  │ Prefix NVMe     │
│ Warm SSD       │  │ Warm SSD        │  │ Warm SSD        │
│ Cold HDD       │  │ Cold HDD        │  │ Cold HDD        │
│ ★本地数据面★   │  │ ★本地数据面★    │  │ ★本地数据面★    │
│ Drivers×5      │  │ Drivers×5       │  │ Drivers×5       │
│ AccountPool    │  │ AccountPool     │  │ AccountPool     │
│ libp2p host    │  │ libp2p host     │  │ libp2p host     │
│ +DHT bootstrap │  │ +DHT bootstrap  │  │ +DHT bootstrap  │
│ +Relay v2      │  │ +Relay v2       │  │ +Relay v2       │
└───────┬────────┘  └───────┬─────────┘  └───────┬─────────┘
        │                   │                    │
   ┌────┴───────┐  ┌──────────────┐  ┌──────────┴──────┐
   │L4-02 北京  │  │ 纯边缘 ×N    │  │ 纯边缘 ×N       │
   │(备用)      │  │ 华北社区/CDN  │  │ 华南/海外 CDN    │
   │            │  │ (NAT后, 经    │  │ (NAT后, 经       │
   │            │  │  relay接入)   │  │  relay接入)      │
   └────────────┘  └──────────────┘  └─────────────────┘
        │                                       │
        │       网盘 (出口 IP = 各 L4 节点本机, 仅 L4 接触凭证)
        v                                       v
┌──────────────────────────────────────────────────────────────┐
│                       网盘账号池 (共享)                        │
│  115 ×5   百度 ×10   夸克 ×5   OneDrive ×3   阿里 ×3          │
│  (凭证由控制面仅下发给 L4Backhaul=true 的节点, 限流配额按节点数)│
└──────────────────────────────────────────────────────────────┘

         ┌─────────────────────────────┐
         │  Ingest Worker 集群          │
         │  (独立角色, 跨区域部署)       │
         │  ContentIngester 加工处理    │
         │  + 冗余上传 (全局调度)        │
         │  2+ 实例                      │
         └────────────┬────────────────┘
                      │
         ┌────────────┴────────────────┐
         │  中心控制面 (L4-C)            │
         │  (上海, 2 实例主备)           │
         │  账号主库/凭证下发            │
         │  健康汇总/PinOrch            │
         │  Ingest 编排                 │
         │  SyncBroadcaster             │
         │  ★DHT bootstrap + PSK★     │
         │  ★JWT 签发★    │
         └────────────┬────────────────┘
                      │
         ┌────────────┴────────────────┐
         │  PostgreSQL(主从) + Redis    │
         │  DNS 调度健康聚合             │
         └─────────────────────────────┘
```

> **注**：节点角色已从 `L2+L4` / `L2-only` 二分法改为能力模型（详见 [`distribution/README.md §1`](distribution/README.md)）。图中"L4 节点"= `L4Backhaul=true`（自建公网，承担回源+relay），"纯边缘节点"= `L4Backhaul=false`（社区或自建，仅分发）。调度层/控制面仍中心化部署，但仅承担 JWT 签发、DHT bootstrap、DNS 调度、PinPlan、凭证下发——**节点间发现已去中心化**（PSK 私有网络 + DHT）。无中心代理兜底，区域级 L4 全部故障不覆盖，见 [§1.3](#13-sla-边界与不可恢复场景)。

实例规格与配置文件详见各领域文档。部署配置示例见 [`storage/README.md`](storage/README.md)（控制面配置）和 [`distribution/README.md`](distribution/README.md)（节点配置）。

---

## 7. 技术选型清单

| 层级 | 组件 | 选型 | 理由 |
|------|------|------|------|
| 节点二进制 | L4 / 纯边缘节点同一份（能力模型，非角色枚举） | **自研 Go + go-libp2p** | 同一二进制按 JWT 启用不同能力。goroutine 适合流式代理模型；go-libp2p 提供节点身份、PSK 私有网络、DHT 发现、GossipSub、ConnectionGater、NAT 穿透 |
| 节点发现 | 不可信节点自动注册发现 | **PSK 私有网络 + libp2p DHT + GossipSub PeX** | PSK 编译时注入（传输层准入）；JWT 准入（Ed25519 签名 + PeerId 绑定 + 1h 有效期，过期前 5min 续签，无宽限期）；去中心化，调度层退化为入口；详见 [`distribution/README.md §10.2`](distribution/README.md) |
| 接入层数据面 | 网盘抽象 + 链接池 + 限流 + 熔断 | **自研 Go**（节点子模块） | driver 接口 + 账号池 + 熔断器，context/errgroup/rate 标准库 |
| 接入层控制面 | 账号主库 + 同步协议 + Ingest + DHT bootstrap + JWT 签发 | **自研 Go** | SyncBroadcaster 增量同步协议；DHT bootstrap 与控制面共进程，逻辑独立；JWT 签发独立 HTTP 端点 |
| 元数据存储 | 结构化数据 | **PostgreSQL 14+** | 事务保证、JSONB 存 MPD、主从复制 |
| 元数据缓存 | 热数据加速 | **Redis 7+ 集群（哨兵）** | content 元数据 / content_blob 编排 / blob_location / 账号健康缓存，自动故障转移 |
| 调度层 | DNS 调度 + 健康聚合 | **自研 HTTP 调度 + DNS GeoIP** | 仅负责客户端到节点的 DNS 调度，不参与节点间发现 |
| 监控 | 指标 + 告警 | **Prometheus + Grafana** | 所有节点原生暴露 /metrics |
| 日志 | 聚合搜索 | **Loki + Grafana** | 与 Prometheus 统一面板 |
| 容器编排 | 所有服务 | **Docker + K8s（可选）** | DaemonSet/Deployment 管理 |

---

## 8. 容量规划概要

### 假设条件
- 视频总数：100,000
- 平均视频大小：1 GB，5 码率阶梯
- 段时长：4s（~2MB/段/720p，~16MB/段/4K）
- 冗余副本数：K=2
- 并发观众：50/节点
- L4 节点：6（每区域 2），非 L4 节点：弹性

### 网盘账号需求
- 实际回源带宽（80% 命中率 + 兄弟互助折扣后）：~120 Mbps
- K=2 冗余翻倍：~240 Mbps
- 需要 ~20 个账号（2 百度 + 5 115 + 5 夸克 + 4 OneDrive + 4 阿里）
- 多节点共享，配额按节点数分配（见 [`storage/README.md §8`](storage/README.md)）

### 节点存储需求
| 缓存层 | L4 节点 | 非 L4 节点 |
|--------|-----------|-------------|
| Prefix cache (NVMe) | ~1.5 TB | ~1.5 TB |
| Warm cache (SSD) | ~17 TB | ~15 TB |
| Cold cache (HDD) | ~100 TB (可选) | — |
| **单节点总计** | ~19 TB | ~17 TB |

### 元数据存储
- blob: ~5 GB（5000 万行，内容寻址主表）
- blob_location: ~8 GB（1 亿行，K=2）
- content + content_blob: ~1 GB（content 主表 + 编排关联表，含 business_meta JSONB）
- account_health / 其他表：< 1 GB
- **合计 ~14 GB**，4C16G PG 实例 + 8G Redis 即可

详细容量计算见原架构文档 §13。

---

## 9. 目标态 vs 实现态（target state vs implemented state）

> **本节为文档-代码偏差校准表**。本系统处于 P0 评审修复阶段，多份架构文档以"目标态"形式存在，部分能力尚未落地。下表逐域列出当前实现态偏差，避免阅读者把设计稿当成已实现能力。所有标注均与 `review-remediation` plan T1-T20 完成状态对齐（截至 commit a4abcfb）。

### 9.1 逐域偏差表

| 领域 | 目标态（设计稿） | 实现态（当前代码） | 偏差说明 / 交叉引用 |
|------|------------------|--------------------|---------------------|
| **元数据缓存层 Redis** | Redis 7+ 哨兵模式作为热数据缓存（content 元数据、content_blob 编排、blob 位置、账号健康），见 [`storage/README.md §9.3`](storage/README.md) | **未采纳**：本波次（T9/T10）改为控制面查询 API `GET /v1/blob-locations/{hash}` + 能力 JWT 认证（`internal/controlplane/locationsvc`），edge 侧用 `HTTPLocationClient` 拉位置。Redis 缓存读路径**未实现**。 | T9/T10 决策（plan line 28：Redis 未采纳，user decision）。Redis 设计稿保留作为目标态资产，未来若 P95 读延迟仍不达标可重新评估。 |
| **PolicyController（链路策略控制域）** | 独立领域 `PolicyController`：Backend 抽象（CloudDriveBackend / LocalStorageBackend）、用量采集、回源权重（ReadWeight）+ 上传权重（UploadWeight）通过 SyncBroadcaster 下发，见 [`policy/README.md`](policy/README.md) | **未实现**：`docs/policy/` 为纯目标态设计，零代码实现。当前回源账号选择走 `AccountPool.SelectForRead` 的静态 `VendorProfile` 权重；上传选择走 `SelectKForUpload`（健康 + 跨厂商 + 负载最低），未接入动态权重。 | `docs/policy/README.md` 顶部已加 banner 标注目标态。 |
| **Cold Cache / FetchSegment / NAT traversal 配置字段** | `edge.cold_cache`、`access_layer.fetch_segment_server/client`、`access_layer.data_plane.subscribe_control/drivers/rate_limit_local`、`access_layer.vendor_profiles/rate_limits/health_check/cloud_accounts`、`metadata.popularity_query_interval` 等 | **配置已删除**：T17 移除全部未消费字段（`internal/config/config.go` + `controlplane.go`），保留旧 YAML 加载时只发 `slog.Warn("deprecated config key ignored")`。NAT 穿透栈（AutoRelay/AutoNAT/DCUtR）实际在 libp2p host 装配，配置项已不在 `edge:` 树下。 | 详见 T17 完成记录 + 各领域文档对应章节的"实现态偏差"批注。 |
| **OTel 分布式追踪** | 目标态提及 Prometheus + Grafana + Loki，未明确 OTel SDK 接线计划 | **未实现**：T20 仅完成 `/metrics`（Prometheus）挂载于 3 个服务（control-plane / edge-node / ingest-worker）和关键路径插桩。OpenTelemetry trace SDK 未引入，跨服务 trace 上下文未传递。 | plan line 275 明确 deferred。 |
| **L4 数据面 edge main 装配** | L4 节点 edge main 装配 `LocalDataPlane`（含 `HTTPLocationClient` + `func() string { return string(jwtClient.CurrentJWT()) }` 闭包），完整回源路径 FetchBlobLocal 可用 | **未接线**：T10 交付了 `HTTPLocationClient` 与 `JWTClient.CurrentJWT()`，但 edge main 中 `LocalDataPlane` 仍为 `nil`（plan line 185）。L4 回源路径目前走 `BackhaulManager.HandleBlobL4` 的 ICP 兜底逻辑，不走本地数据面直连网盘。 | plan line 185 标注为后续工作。 |

### 9.2 风险登记册：已识别但暂缓处置的风险项

以下 3 项风险在评审中识别，但当前部署边界（内网 / 节点数 ≤5 / 仅运营者访问）下可接受，暂缓处置。每项均标注**再评估触发条件**，触发后须重新进入修复队列。

#### 风险 #1：ingest-worker 无准入控制

- **风险描述**：`POST /ingest/{content_type}` 接受最大 10 GB multipart 上传，无任何认证或准入控制（T5 仅做了 `max_upload_bytes` 配置化 + work_dir 空闲空间启动检查，未加认证层）。
- **当前接受理由**：端口仅在内网暴露，未对外。
- **再评估触发条件**：
  1. 该端口暴露给外网之前；
  2. 测试环境晋升（test env promotion）前；
  3. 多租户接入需求出现时。

#### 风险 #2：控制面单点（JWT 签发 × 1h TTL）

- **风险描述**：JWT 签发器为控制面单实例，TTL 1h。控制面宕机 > 1h 时全网 JWT 陆续过期，节点退出 `Edge` capability 导致哈希环收缩，最终引发网络自解散（已缓解到 5-10min RTO，但 >1h 仍会全网降级，见 [`distribution/README.md §12.5`](distribution/README.md)）。
- **当前接受理由**：当前节点数 ≤5，控制面单实例主备切换 5min 内可恢复。
- **再评估触发条件**：
  1. 生产化（productionization）前；
  2. 节点数 > 5；
  3. 多区域部署需求出现时。

#### 风险 #5：客户端 `/blob/{hash}` 无访问控制

- **风险描述**：`GET /blob/{hash}` 直接返回段字节流，无 signed URL / anti-leech / token 校验。任何拿到 blob_hash 的客户端均可下载（hash 本身是不可枚举的 SHA-256，但仍是访问控制缺口）。
- **当前接受理由**：当前仅运营者内网访问，无终端用户。
- **再评估触发条件**：
  1. 对终端用户开放访问之前；
  2. 内容版权保护需求出现时；
  3. 公网暴露 edge 节点 HTTP 端口之前。
