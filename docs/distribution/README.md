# 分发方向 -- 领域文档

本文档覆盖读路径：client -> 边缘节点 -> 兄弟/网盘 的回源链路。核心问题：缓存有限的节点选择、回源策略、账号/网盘选择（后者依赖存储层元数据，见 storage/README.md）。

不含段文件存储与元数据管理（见 storage/README.md），不含入库流程（见 ingest/README.md）。

## 目录

> **网络与准入层**（节点身份、JWT、libp2p 发现、ConnectionGater、GossipSub 评分、NAT 穿透）已拆分至 [network.md](network.md)。

0. [共享类型定义](#0-共享类型定义)
   - [0.1 节点身份与能力](#01-节点身份与能力libp2p-准入模型) → 详见 [network.md §1](network.md#1-共享类型节点身份与能力)
   - [0.2 数据流类型](#02-数据流类型)
1. [节点身份与能力配置](#1-节点身份与能力配置) → 详见 [network.md §2](network.md#2-节点身份与能力配置)
2. [缓存层结构](#2-缓存层结构)
3. [Prefix Cache 策略](#3-prefix-cache-策略)
4. [Stream-through 回源](#4-stream-through-回源)
   - [4.1 L4 节点：本地回源](#41-l4-节点本地回源)
   - [4.2 非 L4 节点：代理拉取](#42-非-l4-节点代理拉取)
   - [4.3 延迟分析](#43-延迟分析)
5. [预取](#5-预取)
6. [边缘节点间协作](#6-边缘节点间协作)
   - [6.1 一致性哈希段路由](#61-一致性哈希段路由)
   - 6.2-6.3 → 详见 [network.md §3](network.md#3-节点间通信与连接门控)
7. [视频感知缓存逐出](#7-视频感知缓存逐出)
   - [7.1 Content-aware LRU](#71-content-aware-lru)
   - [7.2 高延迟段保护](#72-高延迟段保护)
8. [本地流行度 GossipSub](#8-本地流行度-gossipsub带评分的不可信同步) → 详见 [network.md §4](network.md#4-本地流行度-gossipsub带评分的不可信同步)
9. [动态 Pin 与 Prefix 扩展](#9-动态-pin-与-prefix-扩展p0-c13--p1-c1-修订)
   - [9.1 节点 Pin 基础设施](#91-节点-pin-基础设施)
   - [9.2 PinStrategy 策略层](#92-pinstrategy-策略层)
   - [9.3 Prefix 推送策略](#93-prefix-推送策略)
   - [9.4 Prefix 推送策略对比](#94-prefix-推送策略对比)
10. [路由策略与控制面职责](#10-路由策略与控制面职责)
    - [10.3 缓存路由：一致性哈希 + libp2p ICP](#103-缓存路由一致性哈希--libp2p-icp)
    - 10.1-10.2, 10.4-10.7 → 详见 [network.md §5](network.md#5-路由策略与控制面职责)
11. [调度层与路由](#11-调度层与路由)
    - [11.1 客户端到节点：DNS + HTTP 302 两级调度](#111-客户端到节点dns--http-302-两级调度)
    - [11.2 边缘内部路由（按节点能力分流）](#112-边缘内部路由按节点能力分流)
    - 11.3-11.4 → 详见 [network.md §6](network.md#6-负载均衡与-nat-穿透)
12. [分发层容灾](#12-分发层容灾)
    - [12.1 L4 节点多副本](#121-l4-节点多副本无中心代理去角色化)
    - [12.2 带宽瓶颈缓解](#122-带宽瓶颈缓解)
    - [12.3 L4 能力集中](#123-l4-能力集中)
    - [12.4 冷启动长尾覆盖缓解](#124-冷启动长尾覆盖缓解)
    - [12.5 调度层失联降级](#125-调度层失联降级新增)
    - [12.6 社区节点拜占庭行为处置](#126-社区节点拜占庭行为处置新增)
    - [12.7 Relay 带宽规划](#127-relay-带宽规划新增)
13. [分发层监控指标](#13-分发层监控指标)
    - [13.1 节点缓存指标](#131-节点缓存指标)
    - [13.2 告警规则（分发相关）](#132-告警规则分发相关)

---

## 0. 共享类型定义

以下类型在分发域多个章节中使用，统一定义于此。部分类型由 ingest 域产出（标注来源），分发域消费。

### 0.1 节点身份与能力（libp2p 准入模型）

节点身份类型（PeerId、NodeCapabilities、CapabilityJWT、PeerStoreEntry）见 [network.md §1](network.md#1-共享类型节点身份与能力)。

### 0.2 数据流类型

```go
// ─── 来自 ingest 域的类型 (见 ingest/README.md) ───

// BlobDescriptor: 单个 blob 的描述 (ingest 域产出, 分发域消费)
type BlobDescriptor struct {
    BlobHash  string  // SHA-256 hash (内容寻址, 全局唯一)
    BlobType  string  // "init" | "media" | "thumbnail" | "original" | "page_1" | ...
    Size      int64
    SortOrder int
}

// ContentMeta: 内容元数据 (ingest 域产出, 分发域消费)
type ContentMeta struct {
    ContentID    string
    ContentType  string  // "dash_video" | "image" | "document" | ...
    TypeMetadata []byte  // JSON (MPD/EXIF 等, 分发域不解析)
}

// ContentIngestedEvent: 入库事件 (ingest 域发布, 分发域订阅)
type ContentIngestedEvent struct {
    ContentID   string
    ContentType string
    Blobs       []BlobDescriptor  // 含 BlobHash + BlobType + Size
    Timestamp   int64
}

// ─── 分发域内部类型 ───

// NodeSpaceInfo: 节点空间统计 (控制面从 NodeStatusReport 维护)
type NodeSpaceInfo struct {
    NodeID         string
    AvailableBytes int64  // prefix 分区剩余空间
    PinnedCount    int32  // 已 pin blob 数量
}

// NodePinPlan: 针对单个节点的 pin 计划 (策略层产出)
type NodePinPlan struct {
    NodeID      string
    ContentID   string
    PinBlobs    []string  // 该节点应 pin 的 blob_hash 列表
    UnpinBlobs  []string  // 该节点应 unpin 的 blob_hash 列表
}

// PinPlan: 下发给节点的 pin 指令 (控制面 → 节点)
type PinPlan struct {
    Seq        uint64
    TargetNode string         // 定向下发
    Updates    []PinUpdate
}

type PinUpdate struct {
    PinBlobs   []string
    UnpinBlobs []string
}

// PinSpaceInfo: 节点 pin 空间查询结果 (节点 → 控制面 RPC 响应)
type PinSpaceInfo struct {
    AvailableBytes  int64
    PinnedCount     int32
    TotalPinnedSize int64
}

// NodeStatusReport: 节点定期上报的状态 (节点 → 控制面)
// 注意: 角色信息不在上报中, 控制面从签发的 JWT 记录中查
type NodeStatusReport struct {
    NodeID       string
    PeerID       PeerId
    Capabilities NodeCapabilities  // 节点当前实际启用的能力 (供控制面对账)
    PrefixSpace  PartitionStatus
    WarmSpace    PartitionStatus
    Healthy      bool
    LastUpdate   int64
}

type PartitionStatus struct {
    TotalBytes int64
    UsedBytes  int64
    BlobCount  int32
}
```

---

## 1. 节点身份与能力配置

节点身份（libp2p PeerId）、能力模型与 JWT、节点配置文件、能力组合与部署场景、节点发现流程总览见 [network.md §2](network.md#2-节点身份与能力配置)。

## 2. 缓存层结构

```go
// 节点基础结构 (L4 和非 L4 节点共用, 能力由 JWT payload 决定)
type EdgeNode struct {
    nodeID      string
    peerID      PeerId
    cache       CacheLayer           // 多级缓存 (见下方 ASCII 图)
    pinStore    *PinStore            // pin 基础设施 (§9.1)
    hashRing    *HashRing            // 一致性哈希环 (§6.1, §10.3)
    sfGroup     *singleflight.Group  // 回源并发去重 (§4)
    fetchClient *FetchClient         // 非 L4: libp2p stream 拉取 L4 节点 (§4.2, network.md §6.1)
    localDataPlane *DataPlane        // L4: 本地数据面 (§4.1), 非 L4 为 nil
    peerStore   *PeerStore           // libp2p peer 持久化 (network.md §1, network.md §5.2)
    gossipSub   *pubsub.PubSub       // GossipSub (§8)
    peerScorer  *PeerScorer          // 节点评分 (network.md §4.3)
    // ...
}
```

```
+-----------------------------------------------------------+
|                   Edge Node Cache Hierarchy                |
|                                                            |
|  +-----------------------------------------------------+  |
|  | L1: 内存索引 (Concurrent Map, ~100MB)                 |  |
|  |     blob_hash -> (location, size, LRU_ts, pop_score)  |  |
|  +-----------------------------------------------------+  |
|                          |                                 |
|           +--------------+--------------+                  |
|           v                             v                  |
|  +--------------------+     +--------------------+        |
|  | L2: Prefix Cache   |     | L3: Warm Cache     |        |
|  | NVMe SSD, ~2TB     |     | SSD, ~50TB         |        |
|  | init + blob 1,2     |     | 完整 blob,      |        |
|  | 全部内容 x5 码率    |     | Content-aware LRU    |        |
|  | pinned, 不参与逐出  |     |                    |        |
|  +--------------------+     +--------------------+        |
|                                                            |
|  +-----------------------------------------------------+  |
|  | L4: Cold Cache (可选)                                  |  |
|  | HDD RAID0 / MinIO, ~100TB                             |  |
|  | 长尾视频完整段                                         |  |
|  +-----------------------------------------------------+  |
+-----------------------------------------------------------+
```

**L1 内存索引**不存段内容，只存元信息。IO 路径：请求 -> 查内存索引（~100ns）-> 定位到 SSD 位置 -> 直接 `sendfile(2)` 返回。

---

## 3. Prefix Cache 策略

Prefix cache 是边缘节点 NVMe 上的独立分区，存储指定内容的"优先 blob"（由 PinStrategy 策略层计算，PinStore 基础设施执行，见 §9）。该分区的内容不参与常规 LRU 逐出。

**特性**：
- Pin 住不逐出：由 PinStore（§9.1 节点 pin 基础设施）控制 pin/unpin，常规 evict() 跳过 pinned blob
- 内容类型无关：DASH 视频 pin init+前N段；图床 pin 缩略图；文档 pin 第一页。决策由 PinOrchestrator 策略层根据 BlobType + 流行度做出
- 内容来源：入库时由 PinOrchestrator 推送到区域代表节点，其他节点首次访问时被动拉取（详见 §9.2）

---

## 4. Stream-through 回源

当缓存未命中时，节点按**能力**（`L4Backhaul` true/false）走不同的回源路径，但**对客户端完全透明** -- 无论是否启用 L4，都执行"首字节到即开始转发 + 边收边写本地缓存"。

**并发去重**：多个客户端同时请求同一个 miss 的 blob 时，用 `singleflight` 合并为一次回源。第一个请求触发回源，后续请求等待结果共享，避免惊群效应。

### 4.1 L4 节点：本地回源

L4 节点（`L4Backhaul=true`，JWT 授权）直接调用本地数据面，回源字节流从本机出口到网盘。

```go
// L4 节点处理 blob 请求
func (e *EdgeNode_L4) HandleBlob(w http.ResponseWriter, r *http.Request, blobHash string) {
    // 1. 本地缓存
    if data, ok := e.cache.Get(blobHash); ok { w.Write(data); return }
    // 2. 兄弟节点 (network.md §3.1 libp2p stream)
    if data, ok := e.fetchFromPeer(r.Context(), blobHash); ok { e.cache.Put(blobHash, data); w.Write(data); return }
    // 3. ★ singleflight 去重 + 本地数据面回源
    //    多个请求同时 miss 同一 blob 时, 只有第一个触发回源, 其余等待共享结果
    result, err, _ := e.sfGroup.Do(blobHash, func() (interface{}, error) {
        ctx := r.Context()
        stream, err := e.localDataPlane.FetchBlobLocal(ctx, blobHash)
        if err != nil { return nil, err }
        // streamThrough: 边收边写 (第一个请求执行, 后续请求从缓存读取)
        return e.streamThrough(w, stream, blobHash)
    })
    if err != nil {
        http.Error(w, "blob not found", 503)
        return
    }
    // singleflight 等待者: 第一个请求已将数据写入缓存, 直接从缓存返回
    if result != nil {
        // 第一个请求已通过 streamThrough 返回, 无需再写
        return
    }
    // 等待者: 数据已在缓存中
    if data, ok := e.cache.Get(blobHash); ok { w.Write(data); return }
    http.Error(w, "blob not found", 503)
}
```

### 4.2 非 L4 节点：代理拉取

非 L4 节点（`L4Backhaul=false`，社区节点默认）没有本地数据面，缓存 miss 后通过 libp2p stream 向 L4 节点拉取（network.md §6.1）。同样使用 singleflight 去重。所有 L4 节点不可达时，回源失败返回 503。

```go
// 非 L4 节点处理 blob 请求
func (e *EdgeNode_NoL4) HandleBlob(w http.ResponseWriter, r *http.Request, blobHash string) {
    // 1. 本地缓存
    if data, ok := e.cache.Get(blobHash); ok { w.Write(data); return }
    // 2. 兄弟节点 (network.md §3.1 libp2p stream, 任何 PeerICP=true 的节点)
    if data, ok := e.fetchFromPeer(r.Context(), blobHash); ok { e.cache.Put(blobHash, data); w.Write(data); return }
    // 3. ★ singleflight 去重 + libp2p stream 拉取 L4 节点
    //    所有 L4 不可达时返回 503
    result, err, _ := e.sfGroup.Do(blobHash, func() (interface{}, error) {
        ctx := r.Context()
        stream, err := e.fetchClient.FetchFromL4Node(ctx, blobHash)  // network.md §6.1
        if err != nil {
            return nil, fmt.Errorf("all L4 nodes unreachable: %w", err)
        }
        return e.streamThrough(w, stream, blobHash)
    })
    if err != nil {
        http.Error(w, "blob not found (L4 unavailable)", 503)
        return
    }
    if result != nil { return }
    // 等待者: 数据已在缓存中
    if data, ok := e.cache.Get(blobHash); ok { w.Write(data); return }
    http.Error(w, "blob not found", 503)
}
```

`streamThrough` 是两种能力路径共享的工具函数：tee 到客户端 + 本地缓存文件 + 完成后更新索引。singleflight 确保同一 blob_hash 的并发请求只触发一次回源，其余请求等待第一个请求写入缓存后直接读取。

### 4.3 延迟分析

| 步骤 | L4 节点（本地回源） | 非 L4 节点（代理拉取） |
|------|---------------------|------------------------|
| 查本地缓存 | < 0.1ms | < 0.1ms |
| 查兄弟节点（libp2p stream HEAD） | 3-8ms（持久连接多路复用） | 3-8ms |
| 路径初始化 | 本地数据面查元数据 + 取链接 10-50ms | libp2p stream 到 L4 节点 1-5ms（复用已建立连接） |
| 网盘请求 | L4 本机 -> 网盘 50-200ms | L4 本机 -> 网盘 50-200ms（在 L4 端发生） |
| 首字节到本节点 | 5-20ms（本机数据面） | + L4 -> 非 L4 的 libp2p stream 延迟 5-20ms（NAT 后经 relay +20-50ms） |
| 本节点 -> 客户端 | < 1ms | < 1ms |
| **端到端冷启动首字节** | **~100-300ms** | **~110-330ms**（多一跳 libp2p stream；NAT 后 +20-50ms relay） |

非 L4 节点额外延迟约 10-30ms（一跳区域内 libp2p stream）。NAT 后走 relay 兜底时额外 +20-50ms（DCUtR 打洞失败场景）。

---

## 5. 预取

预取策略由**内容类型专属的 PrefetchStrategy** 决定。系统定义通用预取接口，各内容类型各自实现：

```go
// PrefetchStrategy: 内容类型专属的预取策略
type PrefetchStrategy interface {
    // 给定当前请求的 blob_hash, 返回建议预取的 blob_hash 列表
    SuggestPrefetch(currentBlobHash string, contentMeta ContentMeta) []string
}
```

**通用预取条件**（所有策略共享）：
1. 建议预取的 blob 未在本地缓存中
2. 当前边缘回源带宽利用率 < 70%

```go
func (e *EdgeNode) maybePrefetch(currentBlobHash string, contentMeta ContentMeta) {
    strategy := e.prefetchStrategies[contentMeta.ContentType]
    if strategy == nil { return }

    suggestions := strategy.SuggestPrefetch(currentBlobHash, contentMeta)
    for _, blobHash := range suggestions {
        if e.cache.Has(blobHash) { continue }
        if e.backhaulUtilization() > 0.7 { return }

        go func(id string) {
            data, err := e.fetchBlobLowPrio(id)
            if err == nil {
                e.cache.Put(id, data)
            }
        }(blobHash)
    }
}
```

**DASH 视频预取策略示例**：

```go
type DashPrefetchStrategy struct{}
func (s *DashPrefetchStrategy) SuggestPrefetch(currentBlobHash string, meta ContentMeta) []string {
    // 解析 blob_hash: "seg_720p_3" → representation="720p", number=3
    rep, num := parseDashBlobHash(currentBlobHash)
    return []string{
        fmt.Sprintf("seg_%s_%d", rep, num+1),
        fmt.Sprintf("seg_%s_%d", rep, num+2),
    }
}
```

**图床预取策略**：通常无需预取（图片无顺序依赖），`SuggestPrefetch` 返回空列表。

---

## 6. 边缘节点间协作

### 6.1 一致性哈希段路由

```
blob_hash -> crc32(blob_hash) -> hash_ring -> 主节点
```

同一区域内 N 个边缘节点组成哈希环。`hash(blob_hash)` 落到节点 A，则节点 A 为该 blob 的"主节点"。所有节点在处理 blob 请求时，优先尝试哈希主节点。

效果：同一 blob 在同一区域内只会回源一次（被主节点缓存后，其他节点命中主节点即可）。

**哈希环成员来源**（修订）：原设计由调度层广播 `NODE_LIST_UPDATE`，节点被动重建。新设计改为节点从本地 **PeerStore**（持久化在 BadgerDB，见 network.md §1）重建。PeerStore 的来源：
- 启动时从 DHT `FindPeers("edge")` 获取初始集合
- 运行时由 GossipSub PeX（prune 消息捎带对端 peer）增量补充
- 每 5min 重新 `FindPeers` 校正（与 DHT re-Advertise 同节奏）
- 调度层不再广播节点列表（详见 network.md §5.2）

```go
// 哈希环从 PeerStore 重建, 非控制面广播
func (n *EdgeNode) rebuildHashRing() {
    peers := n.peerStore.ActivePeers()
    // ActivePeers: 过滤 Score >= GraylistThreshold 且 !Stale 的 peer
    newRing := consistenthash.New()
    for _, p := range peers {
        if p.Capabilities.PeerICP {  // 只有 peer_icp 能力的节点进环
            newRing.Add(string(p.PeerID))
        }
    }
    n.hashRing.Replace(newRing)  // 原子替换
}

// PeerStore 变更时触发重建 (注册新 peer / peer 过期 / 评分降级)
func (n *EdgeNode) OnPeerStoreChange() {
    n.hashRingRebuildCh <- struct{}{}  // 防抖, 1s 内多次变更合并为一次重建
}
```


## 7. 视频感知缓存逐出

### 7.1 Content-aware LRU

**逐出规则**：

```
当缓存满时逐出:
  1. 先选 lowest-popularity 的视频
  2. 在该视频内，选 highest-bitrate 的段逐出
  3. 保证 prefix cache 不受影响（prefix 是独立的 pinned 分区）
```

```go
type VideoMeta struct {
    BlobHash    string
    Popularity   float64  // 过去 24h 请求次数
    Segments     []*SegmentCache
}

type SegmentCache struct {
    BlobHash        string
    Bitrate      int      // bps
    Size         int64
    LastAccess   time.Time
    CachedAt     time.Time
}

// 逐出选择
func (e *EdgeNode) evict() *SegmentCache {
    // 先选流行度最低的视频 (优先读 gossip 近 5min 热度, fallback 到 PG window_24h)
    videos := e.getVideosSortedByPopularity()
    for _, v := range videos {
        // 从最高码率段开始逐出
        segs := v.getSegmentsSortedByBitrateDesc()
        for _, s := range segs {
            if !s.IsPrefix { // 不逐出 prefix (prefix 由 PinPlan 管理,见 §9)
                return s
            }
        }
    }
    return nil
}
```

### 7.2 高延迟段保护

回源时记录每次回源延迟（`last_fetch_latency_ms`），逐出决策对高延迟回源段降低逐出优先级——跨厂商回源 300ms 的段比本地缓存命中 0.1ms 的段更值得保留。

**延迟反哺逐出**：`reportFetchLatency` 将回源延迟写入段的缓存元信息（`last_fetch_latency_ms`）。Content-aware LRU 逐出时，对"高延迟回源段"降低逐出优先级（跨厂商回源 300ms 的段比本地缓存命中 0.1ms 的段更值得保留）。

```go
// §4.7 evict() 修订: 增加 latency 因素
func (e *EdgeNode) evict() *SegmentCache {
    videos := e.getVideosSortedByPopularity()  // 先按流行度(含 gossip)
    for _, v := range videos {
        segs := v.getSegmentsSortedByBitrateDesc()
        for _, s := range segs {
            if e.pinStore.IsPinned(s.BlobHash) { continue }  // pin 检查 (§9.1 基础设施)
            // ★ 新增: 高延迟回源段降低逐出优先级
            // 正常段直接返回(可逐出), 高延迟段跳过(保留)
            if s.LastFetchLatencyMs > 200 { continue }
            return s
        }
    }
    // 如果所有段都是高延迟段, 仍需逐出 (取延迟最低的高延迟段)
    return e.evictHighLatencyFallback(videos)
}
```

> **注意**：`LastFetchLatencyMs` 由数据面回源路径写入。回源账号选择（SelectForRead）的权重由独立的策略控制域计算并下发，见 [`policy/README.md`](../policy/README.md)。此处仅关注逐出决策侧的使用。

---

## 8. 本地流行度 GossipSub（带评分的不可信同步）

GossipSub topic 格式、带评分的合并策略、PeerScorer (AppSpecificScore)、热度数据模型见 [network.md §4](network.md#4-本地流行度-gossipsub带评分的不可信同步)。

> **注**：本节内容（节点身份、JWT、ConnectionGater、GossipSub 评分、DHT 发现、NAT 穿透）已拆分至 [network.md](network.md)。README.md 保留缓存策略、回源、动态 Pin、路由调度、容灾监控。
## 9. 动态 Pin 与 Prefix 扩展

> **隔离性保证**：PinStrategy 是 CDN 分发的**可选优化层**，不是核心分发回源的必经路径。即使 PinStrategy 完全失效（策略未注册、策略 panic、PinPlan 下发失败），核心的 缓存命中 → 兄弟节点 → L4 节点回源 链路仍然正常工作。Pin 失败的唯一影响是首帧体验退化为普通缓存 miss + 流式回源（延迟从 <500ms 退化为 ~150ms），不影响正确性。

### 9.1 节点 Pin 基础设施

节点提供 pin 能力作为 distribution 域的**底层基础设施**。无论是否有 PinStrategy 产出决策，节点都能独立执行 pin/unpin/查询操作。PinStore 状态持久化在嵌入式 KV（BadgerDB）中，重启可恢复。

```go
// 节点侧: Pin 存储管理 (基础设施, 不含策略逻辑)
// 存储后端: BadgerDB (嵌入式 KV) + NVMe prefix 分区
type PinStore struct {
    db      *badger.DB        // pin 状态持久化 (见 §9.1.1)
    storage *PrefixPartition   // NVMe prefix 分区 (blob 数据)
    nodeID  string

    // 内存索引 (从 BadgerDB 重建, 供快速查询)
    // key = blob_hash, value = PinEntry
    index   sync.Map  // map[string]*PinEntry

    // 增量缓冲 (供上报, 见 §9.1.1)
    deltaBuffer *DeltaBuffer
}

type PinEntry struct {
    BlobHash  string    // blob 的 SHA-256 hash (内容寻址, 全局唯一)
    BlobType  string    // "init" | "media" | "thumbnail" | ...
    Size      int64
    PinnedAt  time.Time
    Ready     bool      // blob 是否已拉取到本地 prefix 分区
}

// ─── 基础设施 API (供 PinPlan 下发和逐出逻辑调用) ───

// 执行 pin 指令: 写入 BadgerDB + 更新内存索引/bloom filter + 追加 delta + 异步拉取 blob
// 调用方: PinPlan 事件处理 (来自控制面策略层)
// 注意: 每次只 pin 一个 blob (blob 级别 pin, 非 content 级别)
func (ps *PinStore) ApplyPin(blobHash string, blobType string, size int64) {
    entry := &PinEntry{
        BlobHash:  blobHash,
        BlobType:  blobType,
        Size:      size,
        PinnedAt:  time.Now(),
        Ready:     false,
    }

    // 1. 持久化到 BadgerDB (重启可恢复)
    ps.dbUpdate(func(txn *badger.Txn) error {
        return txn.Set([]byte("p:"+blobHash), encodePinEntry(entry))
    })

    // 2. 更新内存索引
    ps.index.Store(blobHash, entry)

    // 3. 追加增量 delta (供日志, 不上报)
    ps.deltaBuffer.Append(PinDelta{Type: DELTA_PIN, BlobHash: blobHash, BlobType: blobType})

    // 5. 异步拉取 blob 到本地 prefix 分区
    go ps.fetchPinnedBlob(blobHash)
}

// 执行 unpin 指令: 删除 KV 记录 + 更新索引/bitmap + 追加 delta + 删除 prefix 分区数据
func (ps *PinStore) ApplyUnpin(blobHash string) {
    // 1. 从 BadgerDB 删除
    ps.dbUpdate(func(txn *badger.Txn) error {
        return txn.Delete([]byte("p:"+blobHash))
    })

    // 2. 从内存索引删除
    ps.index.Delete(blobHash)

    // 3. 从 bitmap 移除
    hash := xxhash3.Hash64String(blobHash)
    ps.bitmap.Remove(hash)

    // 4. 追加增量 delta
    ps.deltaBuffer.Append(PinDelta{Type: DELTA_UNPIN, BlobHash: blobHash})

    // 5. 删除 prefix 分区中的 blob 数据
    ps.storage.RemoveContent(blobHash)
}

// 执行部分 unpin (blob 级别: 直接 unpin 单个 blob)
// 在 blob 级别 pin 模型下, ApplyPartialUnpin 等价于 ApplyUnpin
func (ps *PinStore) ApplyPartialUnpin(blobHash string) {
    ps.ApplyUnpin(blobHash)
}

// 查询: blob 是否被 pin (供逐出逻辑调用, 纯内存查询)
func (ps *PinStore) IsPinned(blobHash string) bool {
    _, ok := ps.index.Load(blobHash)
    return ok
}

// 查询: pinned blob 是否已在本地 (供回源路径判断)
func (ps *PinStore) IsReady(blobHash string) bool {
    val, ok := ps.index.Load(blobHash)
    if !ok { return false }
    entry := val.(*PinEntry)
    if !entry.Ready { return false }
    return ps.storage.Has(blobHash)
}

// 异步拉取 pinned blob (基础设施内部方法)
func (ps *PinStore) fetchPinnedBlob(blobHash string) {
    defer func() {
        if r := recover(); r != nil {
            log.Warn("fetchPinnedBlob panicked", "blob_hash", blobHash, "panic", r)
        }
    }()

    data, err := ps.fetchBlob(blobHash)  // 走正常回源路径
    if err != nil {
        log.Warn("fetch pinned blob failed", "blob_hash", blobHash, "err", err)
        return  // 失败跳过, Ready 保持 false
    }
    ps.storage.Put(blobHash, data)

    // 拉取完成, 标记 Ready
    if val, ok := ps.index.Load(blobHash); ok {
        entry := val.(*PinEntry)
        entry.Ready = true
        ps.dbUpdate(func(txn *badger.Txn) error {
            return txn.Set([]byte("p:"+blobHash), encodePinEntry(entry))
        })
    }
}
```

**基础设施保证**：
- `IsPinned` / `IsReady` 查询不依赖网络，纯本地内存/磁盘操作
- `ApplyPin` / `ApplyUnpin` 幂等，重复调用无副作用
- `fetchPinnedBlob` 失败不阻塞任何路径，blob 未就绪时回源路径正常工作
- PinStore 为空时（无任何 PinPlan 下发），系统退化为纯 LRU 缓存，功能完整

#### 9.1.1 节点 Pin 状态本地存储

PinStore 状态持久化在嵌入式 KV（BadgerDB）中，重启可恢复。**节点不上报 pin 列表到控制面**——控制面通过按需 RPC 查询（见 network.md §5.4）。

```go
// PinStore 使用 BadgerDB 存储 pin 状态
// key: "p:{blob_hash}" → PinEntry (protobuf)
// key: "meta:space"    → PrefixPartitionStatus

// 查询: prefix 分区空间 + pin 数量 (供控制面 RPC 查询, 见 network.md §5.4)
func (ps *PinStore) QuerySpace() PinSpaceInfo {
    return PinSpaceInfo{
        AvailableBytes:  ps.storage.Available(),
        PinnedCount:     ps.pinCount(),
        TotalPinnedSize: ps.totalPinnedSize(),
    }
}

// 节点重启时从 BadgerDB 重建内存索引
func (ps *PinStore) Restore() error {
    // 遍历 "p:" 前缀的 key, 重建 index (sync.Map)
    // 不需要从控制面拉取 pin 列表
}
```

### 9.2 PinStrategy 策略层

策略层是**可选的优化组件**，负责计算"哪些 blob 应该被 pin"，产出**按节点差异化**的 PinPlan 下发给各节点执行。策略层不直接操作节点存储。

策略层做决策需要三类输入：
1. **内容元数据**：BlobType、blob 大小（来自 ingest 域的 ContentIngestedEvent）
2. **全局流行度**：window_24h（来自 distribution §8 热度表）
3. **节点空间状态**：各节点 prefix 分区剩余空间（来自 NodeStatusReport，见 network.md §5.3）

**控制面不维护节点 pin 列表**。需要精确 pin 空间时按需 RPC 查询（见 network.md §5.4）。

```go
// PinStrategy: 内容类型专属的 pin 决策策略
type PinStrategy interface {
    // 新内容入库时决定初始 pin
    // blobs: 内容的 blob 列表 (含 BlobHash + BlobType + Size)
    // nodeSpaces: 各节点的空间统计 (来自 NodeStatusReport, 非全量 pin 列表)
    DecideInitialPin(content ContentMeta, blobs []BlobDescriptor, nodeSpaces []NodeSpaceInfo) []NodePinPlan

    // 定时重算时根据流行度调整 pin
    // 返回: 按节点差异化的 pin 调整计划
    AdjustPin(content ContentMeta, popularity int64, nodeSpaces []NodeSpaceInfo) []NodePinPlan
}

// NodePinPlan: 针对单个节点的 pin 计划
type NodePinPlan struct {
    NodeID    string
    BlobHash string
    PinBlobs  []string   // 该节点应 pin 的 blob_hash 列表
    UnpinBlobs []string  // 该节点应 unpin 的 blob_hash 列表 (增量调整)
}
```

**按节点差异化的决策逻辑**：

策略层根据各节点上报的剩余空间，为不同节点产出不同的 pin 计划：

```go
// DASH 视频 PinStrategy 示例
type DashPinStrategy struct{}

func (s *DashPinStrategy) DecideInitialPin(
    content ContentMeta, blobs []BlobDescriptor, nodeSpaces []NodeSpaceInfo,
) []NodePinPlan {
    var plans []NodePinPlan

    // 1. 按内容语义选出候选 blob (所有节点共用的基础选择)
    initBlobs := filterByBlobType(blobs, "init")
    mediaBlobs := filterByBlobType(blobs, "media")
    // 按 sortOrder 排序, 取前 N 段
    sort.Slice(mediaBlobs, func(i, j int) bool {
        return mediaBlobs[i].SortOrder < mediaBlobs[j].SortOrder
    })

    for _, ns := range nodeSpaces {
        // 2. 按节点剩余空间决定 pin 多少
        var pinBlobs []string
        pinBlobs = append(pinBlobs, extractIDs(initBlobs)...)  // init 必 pin

        // 计算可 pin 的 media 段数
        // 空间充足(>50%): pin 前 5 段
        // 空间一般(20%-50%): pin 前 2 段
        // 空间紧张(<20%): 不 pin media 段
        avail := ns.PrefixPartition.AvailableBytes
        total := ns.PrefixPartition.TotalBytes
        ratio := float64(avail) / float64(total)

        mediaCount := 0
        if ratio > 0.5 {
            mediaCount = min(5, len(mediaBlobs))
        } else if ratio > 0.2 {
            mediaCount = min(2, len(mediaBlobs))
        }
        // mediaCount = 0 时仅 pin init

        for i := 0; i < mediaCount; i++ {
            // 检查空间是否够放
            if mediaBlobs[i].Size > avail { break }
            pinBlobs = append(pinBlobs, mediaBlobs[i].BlobHash)
            avail -= mediaBlobs[i].Size
        }

        if len(pinBlobs) > 0 {
            plans = append(plans, NodePinPlan{
                NodeID:    ns.NodeID,
                BlobHash: content.BlobHash,
                PinBlobs:  pinBlobs,
            })
        }
    }

    return plans
}
```

**PinOrchestrator** 负责调用 PinStrategy 计算决策并按节点下发 PinPlan。所有操作在独立 goroutine 中执行，panic 被 recover 捕获，不影响控制面其他模块。

```go
// 控制面侧: 策略调度器
type PinOrchestrator struct {
    metadata    MetadataClient
    broadcaster *SyncBroadcaster
    strategies  map[string]PinStrategy

    // 节点 pin 状态缓存 (来自各节点 30s 上报, §9.1.1)
    nodeSpaces map[string]NodeSpaceInfo  // key = node_id
    nsMu         sync.RWMutex
}

// 接收节点上报的 pin 状态
func (po *PinOrchestrator) OnNodeStatusReport(report NodeStatusReport) {
    po.nsMu.Lock()
    po.nodeSpaces[report.NodeID] = NodeSpaceInfo{NodeID: report.NodeID, AvailableBytes: report.PrefixSpace.AvailableBytes, PinnedCount: report.PrefixSpace.BlobCount}
    po.nsMu.Unlock()
}

// 获取所有节点的当前状态快照 (供策略层使用)
func (po *PinOrchestrator) getNodeSpaces() []NodeSpaceInfo {
    po.nsMu.RLock()
    defer po.nsMu.RUnlock()
    statuses := make([]NodeSpaceInfo, 0, len(po.nodeSpaces))
    for _, s := range po.nodeSpaces {
        statuses = append(statuses, s)
    }
    return statuses
}

// 监听入库事件 — 独立 goroutine, panic-safe
func (po *PinOrchestrator) OnContentIngested(evt ContentIngestedEvent) {
    defer func() {
        if r := recover(); r != nil {
            log.Warn("PinOrchestrator.OnContentIngested panicked, pin skipped",
                "blob_hash", evt.BlobHash, "panic", r)
        }
    }()

    content, err := po.metadata.GetContentMeta(evt.BlobHash)
    if err != nil { return }

    strategy := po.strategies[content.ContentType]
    if strategy == nil { return }

    nodeSpaces := po.getNodeSpaces()
    nodePlans := strategy.DecideInitialPin(content, evt.Blobs, nodeSpaces)

    // 按节点下发差异化 PinPlan
    for _, np := range nodePlans {
        po.sendNodePinPlan(np)
    }
}

// 每 10min 重算 (panic-safe)
func (po *PinOrchestrator) Run(ctx context.Context) {
    ticker := time.NewTicker(10 * time.Minute)
    defer ticker.Stop()
    for {
        select {
        case <-ctx.Done(): return
        case <-ticker.C:
            po.safeRebalance(ctx)
        }
    }
}

func (po *PinOrchestrator) safeRebalance(ctx context.Context) {
    defer func() {
        if r := recover(); r != nil {
            log.Warn("PinOrchestrator.rebalance panicked, skipped this round", "panic", r)
        }
    }()
    po.rebalance(ctx)
}

func (po *PinOrchestrator) rebalance(ctx context.Context) {
    popular, err := po.metadata.GetTopContents(ctx, 5000)
    if err != nil { return }

    nodeSpaces := po.getNodeSpaces()

    for _, c := range popular {
        strategy := po.strategies[c.ContentType]
        if strategy == nil { continue }

        nodePlans := strategy.AdjustPin(c, c.Window24h, nodeSpaces)
        for _, np := range nodePlans {
            po.sendNodePinPlan(np)
        }
    }
}

// 按节点下发 PinPlan (不再全局广播, 而是定向发送)
func (po *PinOrchestrator) sendNodePinPlan(np NodePinPlan) {
    plan := PinPlan{
        Seq:     po.nextSeq(),
        TargetNode: np.NodeID,  // ★ 定向下发, 非全局广播
        Updates: []PinUpdate{{
            BlobHash:   np.BlobHash,
            PinBlobs:    np.PinBlobs,
            UnpinBlobs:  np.UnpinBlobs,
        }},
    }
    po.broadcaster.SendToNode(np.NodeID, PIN_PLAN_UPDATE, plan)
}
```

**节点收到定向 PinPlan 后的执行**：

```go
// 节点侧: 接收定向 PinPlan, 委托 PinStore 基础设施执行
func (n *EdgeNode) handlePinPlan(plan PinPlan) {
    // plan.TargetNode == 本节点 (由 SyncBroadcaster 保证)
    for _, update := range plan.Updates {
        if len(update.PinBlobs) > 0 {
            n.pinStore.ApplyPin(update.BlobHash, update.PinBlobs)  // §9.1 基础设施
        }
        if len(update.UnpinBlobs) > 0 {
            n.pinStore.ApplyPartialUnpin(update.UnpinBlobs[0])
        }
    }
}
```

**内容类型 pin 策略示例**（按节点空间差异化）：

| 内容类型 | BlobType 分类 | 空间充足 (>50%) | 空间一般 (20-50%) | 空间紧张 (<20%) |
|---------|-------------|----------------|------------------|----------------|
| DASH 视频 | init / media | init + 前 5 段 media | init + 前 2 段 media | 仅 init |
| 图床 | original / thumbnail | thumbnail + original | thumbnail | 最小 thumbnail |
| 文档 | page_1 / page_2 / ... | 前 3 页 | 前 2 页 | 仅 page_1 |

**信息流**：

```
节点 A (30s 上报) ──┐
节点 B (30s 上报) ──┼──> PinOrchestrator.nodeSpaces (全局视图)
节点 C (30s 上报) ──┘
                            │
ContentIngestedEvent ──────>│
                            │
                            v
                    PinStrategy.DecideInitialPin(content, blobs, nodeSpaces)
                            │
                    ┌───────┼───────┐
                    v       v       v
                节点A计划  节点B计划  节点C计划
                (pin 5段)  (pin 2段)  (pin 仅init)
                    │       │       │
                    v       v       v
                定向下发  定向下发  定向下发
```

**隔离性保证**：

| 失败场景 | 系统行为 | 用户影响 |
|---------------------|---------|---------|
| 内容类型未注册 PinStrategy | `OnContentIngested` 直接 return，无 PinPlan 下发 | 该内容走普通缓存 miss + 流式回源 |
| 策略 panic | recover 捕获，log warn，跳过该次 pin | 仅该内容不受 pin 加速 |
| 节点状态上报失败（30s 内） | 策略层用上次缓存的状态决策，可能略有偏差 | pin 段数可能略多或略少，无正确性影响 |
| 节点状态从未上报（新节点） | 策略层看到该节点状态为空，按空间=0 处理 | 新节点暂不 pin，等首次上报后正常 |
| PinPlan 定向下发失败 | 节点 PinStore 不更新，保持旧状态 | 该节点走旧 pin 状态或普通缓存 |
| `fetchPinnedBlob` 拉取失败 | PinStore 记录存在但 `IsReady` 返回 false | 首次访问走正常回源，后续缓存命中后正常 |
| PinOrchestrator 整体不可用 | 控制面其他模块不受影响 | 全部内容走普通缓存，系统功能完整 |

### 9.3 Prefix 推送策略

原设计的"入库时全量推送 init+前2段到所有节点"改为**区域代表性推送 + 被动拉取**：

```
入库完成
  |
  +-- 1. 控制面 PinOrchestrator 计算 prefix 计划 (动态段数)
  |
+-- 2. 主动推送: 推送到若干 L4 代表节点 (全网选 2-3 个)
|      (worker pool 限制并发 20, 失败进 retry queue, 指数退避 3 次)
|
+-- 3. 其他节点 (非 L4 + 其余 L4):
         被动拉取 -- 首次请求该视频时, 从代表节点拉取 prefix 段
         GET /prefix/{blob_hash}/{seg} from 代表节点
         拉取后写入本地 prefix 分区
```

```go
// 修订后的 prefix 推送
func (po *PinOrchestrator) pushPrefix(ctx context.Context, blob_hash string, dashDir string) {
    // 1. 计算该视频的 prefix 段数 (动态)
    duration := getVideoDuration(blob_hash)
    n := computePrefixSegments(duration, DEFAULT_WINDOW)  // 入库时用默认热度
    files := selectPrefixFiles(dashDir, n)  // init + 前 N 段

    // 2. 选 2-3 个 L4 代表节点 (全网, 不按区域; 后续可基于 IP 地理位置优化)
    representatives := po.scheduler.GetL4Nodes(2)  // 从 PeerStore 筛选 L4Backhaul=true, 取 2 个

    // 3. 并发推送 (worker pool 限制 20)
    g, ctx := errgroup.WithContext(ctx)
    g.SetLimit(20)
    for _, node := range representatives {
        node := node
        g.Go(func() error {
            return po.pushToNode(ctx, node, blob_hash, files)
            })
    }
    // 失败的进 retry queue (不在入库热路径上, 异步)
    go po.retryFailed(ctx, g.Wait())
}

// 边缘节点: 被动拉取接口
// GET /prefix/{blob_hash}/{blobHash} -> 返回段数据 (供兄弟节点拉取)
func (e *EdgeNode) HandlePrefixPull(w http.ResponseWriter, r *http.Request) {
    blob_hash := r.PathValue("blob_hash")
    blobHash := r.PathValue("blobHash")
    // 1. 查本地 prefix 分区
    if data, ok := e.prefixCache.Get(blob_hash, blobHash); ok {
        w.Write(data)
        return
    }
    // 2. 本地没有 -> 回源拉取并缓存 (L4 本地回源, 非 L4 走代表节点)
    data, err := e.fetchAndCache(blob_hash, blobHash)
    if err != nil {
        http.Error(w, "not found", 404)
        return
    }
    w.Write(data)
}

// prefix 段数动态计算
func computePrefixSegments(durationSec int, window24h int64) int {
    // 热门视频: 每 10s 播放推 1 段, 最多 5 段
    // 冷门视频 (window24h < 10): 仅 init
    if window24h < 10 {
        return 0  // 仅 init, 不推 media 段
    }
    n := (durationSec + 9) / 10  // ceil(duration / 10)
    if n > 5 { n = 5 }
    if n < 2 { n = 2 }  // 最少 2 段 (保证首帧 + 第一个媒体段)
    return n
}
```

### 9.4 Prefix 推送策略对比

| 维度 | 全量推送（已弃用） | 区域代表 + 被动拉取（当前） |
|------|-------|--------|
| 推送目标 | 所有节点 (N 个) | 每区域 2 个代表节点 |
| 推送方式 | 一次性 fire-and-forget | worker pool + retry queue |
| 段数 | 固定 2 段 | 动态: `min(ceil(duration/10), 5)`, 冷门仅 init |
| 非代表节点获取 | 入库时被动接收 | 首次访问时主动拉取 |
| 动态调整 | 无 | PinOrchestrator 每 10min 根据流行度重算 |
| pin 粒度 | 硬编码 init+前2段 | PinPlan 消息动态指定 BlobHashes |

---

## 10. 路由策略与控制面职责

### 10.3 缓存路由：一致性哈希 + libp2p ICP

一致性哈希路由逻辑不变（§6.1 已描述），但**主节点判定从 nodeID 改为 PeerId**，302 跳转变更为 libp2p stream 转发。

```
客户端请求 blob_hash
  │
  │  DNS → 一致性哈希(blob_hash) % N → 主节点 PeerId
  v
主节点查本地缓存
  ├── hit → 直接返回
  └── miss → 查兄弟节点 (libp2p stream HEAD, 10ms 超时, network.md §3.1)
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

**哈希环更新机制**：节点加入/离开时，**无中心广播**。各节点通过 DHT 周期性 Advertise/FindPeers + GossipSub PeX 增量更新本地 PeerStore，PeerStore 变更触发哈希环重建（§6.1）。

```go
// 节点启动时: 从 DHT 拉取 peer 列表, 初始化 PeerStore + 哈希环
func (n *EdgeNode) initHashRing() error {
    // 1. DHT bootstrap + Advertise + FindPeers (network.md §5.2)
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
| 新节点加入 | DHT Advertise → 其他节点 FindPeers 发现 → PeerStore 写入 → 触发重建 | 新节点承担 ~1/N 的 blob 路由；原主节点的部分 blob 不再是主节点，缓存仍有效但不再被路由到 |
| 节点离开 | DHT Advertise TTL 过期 (15min) → FindPeers 不再返回 → PeerStore 标记 Stale → 触发重建 | 该节点的 blob 路由到新主节点 → miss → 回源 → 重新缓存 |
| 节点短暂抖动 | GossipSub 评分衰减 (network.md §4.3) → 低于 GraylistThreshold 时哈希环剔除 | 抖动期间请求转发到不可达节点 → 超时 → 客户端重试 → DNS 降级 |
| 节点恢复 | 评分回升 + DHT Advertise 仍有效 → 重新进环 | 缓存仍有效，恢复正常路由 |

**虚拟节点配置**：

```yaml
# node-config.yaml
hash_ring:
  replicas: 150          # 每个物理节点的虚拟节点数 (默认 150)
  # 心跳由 DHT advertise_interval (5min) + JWT refresh_interval (5min) 承担, 不再单独配置
```

### 10.4 控制面职责与策略层

控制面退化为入口的设计原则、DHT 发现机制、pin/unpin 下发、控制面节点信息、策略层按需查询、数据量对比见 [network.md §5](network.md#5-路由策略与控制面职责)。
## 11. 调度层与路由

### 11.1 客户端到节点：DNS + HTTP 302 两级调度

**首次请求（MPD 请求）路由流程**：

```
客户端 DNS 解析 content.example.com
  -> DNS 返回 GeoIP 最近的边缘节点 IP（CDN DNS 调度）
  -> 边缘若健康，返回 MPD
  -> 边缘若不健康（超时 1s），客户端 follow HTTP 302
    -> 302 Location: edge-02.example.com
  -> 备用边缘返回 MPD
```

DNS 层使用分区域解析：华北 -> 北京边缘、华南 -> 广州边缘、海外 -> 新加坡边缘。DNS 健康检查同步自调度层每 10s 上报的边缘健康状态（调度层在此仍是中心化聚合点，但仅用于 DNS 调度，不影响节点间发现）。

### 11.2 边缘内部路由（按节点能力分流）

原设计按"角色"（L2+L4 / L2-only）分流，新设计改为按**能力**（L4Backhaul true/false）分流。能力来自 JWT payload，节点运行时读取。

```
blob_hash 请求到达任意边缘节点:

  1. 查本地 prefix cache（内存索引，O(1)）
     -> hit: 直接返回
     -> miss: 进入步骤 2

  2. 查本地温缓存（SSD，bloom filter 加速）
     -> hit: 返回 + 更新 LRU
     -> miss: 进入步骤 3

  3. 一致性哈希 -> 查兄弟节点 (network.md §3.1 libp2p stream)
     hash(blob_hash) % N -> 主节点 PeerId
     -> libp2p stream HEAD 兄弟节点 /edge/blob/head/1.0.0（10ms 超时）
     -> 兄弟有: libp2p stream GET 拉取, 写入本地温缓存, 返回客户端
     -> 兄弟无: 进入步骤 4

  4. 按本节点 L4Backhaul 能力分流:
     +-- L4Backhaul=true (JWT 授权) ---------------------+
     | 本地数据面回源                                      |
     | FetchBlobLocal(blob_hash)                          |
     |   -> 查本地段位置 hot subset (miss 查元数据 PG)      |
     |   -> 选账号 + 取本地链接池链接                        |
     |   -> 本机出口 IP 请求网盘                            |
     |   -> 流式返回 + 边写本地缓存                         |
     +----------------------------------------------------+
     +-- L4Backhaul=false (无 JWT 授权) ------------------+
     |  libp2p stream 拉取 L4 节点                          |
     |  FetchFromL4Node(blob_hash)                        |
     |   -> 从 PeerStore 筛选 L4Backhaul=true 的 peer       |
     |      (来源: DHT 发现, network.md §5.2)                  |
     |   -> round-robin + least-conn 选择                  |
     |   -> 首个可达节点接管 (节点内部走 L4 流程)            |
     |   -> 全部不可达: 回源失败, 返回 503                  |
     |   -> 流式返回 + 边写本地缓存                         |
     +----------------------------------------------------+
```

一致性哈希保证同一 blob 在不同节点之间总被路由到同一个"主节点"，减少重复回源。兄弟互助查询用 libp2p stream 做轻量探测，不区分节点能力（任何 `PeerICP=true` 的节点都可作兄弟提供缓存命中）。

### 11.3 负载均衡与 NAT 穿透

非 L4 节点到 L4 节点的负载均衡、NAT 穿透栈（AutoNAT/AutoRelay/DCUtR）见 [network.md §6](network.md#6-负载均衡与-nat-穿透)。
## 12. 分发层容灾

### 12.1 L4 节点多副本（无中心代理，去角色化）

```
   +-----------+  +-----------+  +-----------+
   | 纯边缘节点 |  | 纯边缘节点 |  | L4 节点   |  ...
   | (社区/自建)|  | (社区/自建)|  | (自建公网) |
   +------+------+  +------+------+  +------+------+
          | libp2p stream    |                | 本地回源
          v                  v                v
   +------------------------------------------------+
   |  L4 节点池 (主回源路径, DHT 发现)          |
   |  +--------+  +--------+  +--------+            |
   |  |L4-01   |  |L4-02   |  |L4-03   |  ...       |
   |  |本地数据面| |本地数据面| |本地数据面|            |
   |  |+relay  |  |+relay  |  |         |            |
   |  +----+---+  +----+---+  +----+---+            |
   +------+-----------+-----------+-----------------+
          |           |           |
          v           v           v
   +------------------------------------------------+
   |  PostgreSQL + Redis (元数据服务)                |
   +------------------------------------------------+
          |
   +------+-----------------------------------------+
   |   网盘账号池 (仅 L4 节点接触凭证)                |
   +------------------------------------------------+
```

- **L4 节点**：每区域至少 2 个（自建公网），互为冗余。非 L4 节点缓存 miss 时按 round-robin + least-conn 选择（从 PeerStore 筛选，network.md §6.1）。单节点故障 → GossipSub 评分衰减 + DHT Advertise TTL 过期 → 自动从 PeerStore 摘除。
- **Ingest Worker**：独立角色集群（非分发层角色），承担 ContentIngester 加工处理 + 冗余上传。详见 [storage/README.md §1](../storage/README.md)。
- **节点列表来源**：节点本地 PeerStore（DHT 发现 + GossipSub PeX 增量），非调度层推送。调度层仅运行 DHT bootstrap 作为入口。
- **L4 节点无状态**：所有持久状态在元数据服务 + 网盘端，节点本地只缓存凭证/链接/段位置副本。扩缩容无需数据迁移，新节点上线后连接 DHT bootstrap + Advertise + FindPeers 即可服务（< 30s 拿到 peer 列表）。
- **relay provider**：L4 节点（公网自建）默认承担 relay 角色，为 NAT 后社区节点提供中转。

### 12.2 带宽瓶颈缓解

**风险**：所有客户端流量都经过"接入层数据面"代理，带宽成本集中。

**缓解**：

1. **L4 节点本地出口**。L4 节点的回源流量从本机出口直发网盘，不绕中心，带宽天然分散到各节点。
2. **限流保底**。每节点到 L4 节点的 libp2p stream 连接数有上限（受 JWT 的 `BandwidthQuota` 约束 + libp2p resource manager 限制），防止单节点打爆 L4 节点。
3. **兄弟节点互助**。同一区域节点之间可互相拉取缓存段（任何 `PeerICP=true` 的节点），减少集中回源（见 §6 节）。
4. **L4 节点水平扩展**。无状态，新节点连接 DHT bootstrap 后 < 30s 即可服务。

### 12.3 L4 能力集中

**风险**：某区域 L4 节点全部故障时，该区域非 L4 节点失去回源能力。

**处置**：不设中心代理兜底。区域级 L4 全部故障（相关故障）不覆盖，发生时服务中断，见主文档 [network.md §2.3](../README.md#13-sla-边界与不可恢复场景)。独立故障由 2+ 冗余 + DHT Advertise TTL 过期 + GossipSub 评分降级覆盖（30s 内评分衰减触发摘除，15min 内 TTL 过期正式移除）。

> **注**：当前 FindPeers 使用 libp2p 默认机制返回全网 peer，不按区域划分。非 L4 节点可能路由到远端 L4 节点（跨区域延迟）。后续可基于 IP 地理位置和节点质量推荐优化 peer 选择，使非 L4 节点优先选择近端 L4。

### 12.4 冷启动长尾覆盖缓解

**风险**：prefix cache 只覆盖每个视频前 2 段，长视频尾部段仍然冷启动回源。

**缓解**：

1. Prefix cache 保证首帧 < 3s，后续段由 stream-through 流式回源，对用户几乎不可感知（边下边播）。
2. 热门视频（周访问 > 100 次）触发全量预推，所有段主动同步到节点温缓存。
3. 冷视频的额外延迟控制在 500ms-1.5s（回源 RTT + 网盘取链 + 代理拉流），远低于用户感知阈值。

### 12.5 调度层失联降级（新增）

**风险**：调度层（含 DHT bootstrap + JWT 签发服务）整体不可达——网络分区、机房故障、误操作。

**原设计**：调度层是节点发现权威，失联 → 全网节点列表停止更新 → 新节点无法加入 → 已有节点 30s 后心跳超时被摘除但无人通知。

**新设计**：调度层退化为入口，失联时节点间发现仍可工作。

| 调度层失联时间 | 系统行为 | 用户影响 |
|--------------|---------|---------|
| 0-5min | 节点用 PeerStore 缓存继续路由；DHT re-Advertise 失败但 TTL 未过期；JWT 续签失败但未过期（续签重试中） | 无感 |
| 5-15min | 部分 peer 的 DHT Advertise TTL 过期，PeerStore 标记 Stale；GossipSub PeX 仍能增量补充 | 哈希环逐步收缩，少数 blob 路由偏移 |
| 15-55min | JWT 续签持续失败但 JWT 尚未过期（各节点 JWT 签发时间不同，过期时间分散） | 部分节点 JWT 逐个过期，过期节点立即降级退出 |
| 55-65min | 多数节点 JWT 过期（1h 有效期），过期节点拒绝新连接、仅服务已有客户端 | 哈希环大幅收缩，客户端 DNS 302 重定向到未过期节点 |
| > 65min | 几乎所有 JWT 过期，全网节点降级 | 大规模服务降级，需调度层恢复后节点重新续签 JWT |

**恢复后**：调度层恢复 → 节点重新连接 DHT bootstrap + 向 JWT 服务续签 JWT → PeerStore 重新同步 → 哈希环重建。RTO 5-10min（节点重新连接 bootstrap + JWT 续签窗口）。

**关键设计**：
- PeerStore 持久化在 BadgerDB，节点重启不丢失已知 peer
- GossipSub PeX 在调度层失联期间仍工作（peer 间直接交换）
- JWT 的本地缓存策略：节点在 JWT 过期前 5min 向 JWT 服务请求续签，续签失败立即重试（指数退避）；JWT 过期时未续签成功则立即降级（停止接受新连接，仅服务已有客户端），**无宽限期**
- PSK 不依赖调度层：PSK 编译时注入，调度层失联不影响 PSK 握手，节点间连接仍可建立

### 12.6 社区节点拜占庭行为处置（新增）

**风险**：社区节点可能恶意行为——投毒热度数据、谎报缓存命中、DoS 兄弟节点、私接未授权流量。

**三层防御**（详见 network.md §3.2 防御层级说明）：

| 防御层 | 机制 | 触发 | 处置 |
|--------|------|------|------|
| 传输层 | PSK 编译时注入 (network.md §5.2) | 无 PSK 的节点连接 | TCP/QUIC 层拒绝, 不进入 libp2p 安全层 |
| 认证层 | JWT 签发 + ConnectionGater 验签 (network.md §5.2) | 同 IP 限速 1次/h；JWT 1h 过期 | 限速拒绝；JWT 过期且未续签成功 → 立即 PeerStore Stale → 自然退出（无宽限期） |
| 行为层 | GossipSub AppSpecificScore (network.md §4.3) + GraylistThreshold | ICP 超时率高 / 投毒 / 冒名 | 评分降级 → 哈希环剔除 + ConnectionGater 拒绝 + GossipSub 屏蔽 |

**典型攻击与防御**：

| 攻击 | 检测 | 防御 |
|------|------|------|
| 热度投毒（虚报 blob X 高热度） | 其他节点对账：上报热度与多数节点偏差 > 5x | `RecordMisbehavior` 降分 -5.0；加权合并降低单节点影响 (network.md §4.2) |
| Sybil 攻击（同 IP 多 PeerId） | GossipSub `IPColocationFactor` 内置评分 | 自动降分；同 IP 多个 PeerId 都被惩罚 |
| ICP 拒服务（HEAD 不响应） | `RecordICPTimeout` 累积 | 评分 -1.0/次；超阈值后哈希环剔除 |
| 凭证窃取尝试（非 L4 节点请求网盘凭证） | SyncBroadcaster 下发时查 L4 白名单 | 凭证根本不下发给非 L4 节点；无攻击面 |
| stream 滥用（大量拉取不缓存） | 监控单节点 stream 量 vs cache hit 比率 | `BandwidthQuota` 限速；超限降分 |

**L4 认证的信誉辅助**：L4 能力虽需人工认证（network.md §2.2），但控制面管理员审核时可参考该 PeerId 的**历史信誉数据**作为决策依据。信誉数据来自该节点以纯边缘身份运行期间累积的指标：

| 信誉指标 | 数据来源 | 辅助审核意义 |
|---------|---------|------------|
| 在线时长 | JWT 续签记录 (控制面) | 长期稳定在线 → 运营者认真 |
| GossipSub 评分均值 | PeerScorer 聚合 | 高分 → 无恶意行为 |
| ICP 服务成功率 | 兄弟节点 RecordICPSuccess 聚合 | 高 → 节点健康 |
| 带宽贡献 | BandwidthQuota 用量统计 | 持续贡献 → 非薅羊毛 |
| 投毒/DoS 记录 | RecordMisbehavior 审计日志 | 零记录 → 可信 |
| 公网可达性 | RelayProvider 探测结果 | 可达 → 能承担 L4 出口 |

**注意**：信誉数据**辅助**人工决策，**不替代**人工认证。即使信誉满分，L4 仍需管理员审核运营者身份（防止"养号"攻击——攻击者长期维持高信誉后申请 L4 窃取凭证）。人工认证材料示例：运营者实名、联系方式、服务器归属证明、过往运营经历。

**L4 吊销与降级**：L4 节点行为异常时，控制面可：
1. **软吊销**：下次 JWT 续签时签发 `L4Backhaul=false`，节点 5min 内（JWT 续签周期）降级为纯边缘。本地缓存凭证 5min TTL 内仍可用，但无法获取新凭证。
2. **硬吊销**：立即通过 SyncBroadcaster 下发 `L4_REVOKE` 指令，节点强制销毁本地凭证缓存。拒绝执行 → 评分降至 GraylistThreshold 以下 → 哈希环剔除 + ConnectionGater 拒绝。
3. **白名单移除**：将该 PeerId 从 L4 白名单永久移除，即使后续信誉恢复也不再签发 L4 JWT。

### 12.7 Relay 带宽规划（新增）

**风险**：relay provider（公网 L4 节点）承担 NAT 后社区节点的中转流量，带宽成本可能超预期。

**估算模型**：

```
relay 带宽需求 = Σ (社区节点数 × 平均流量 × (1 - DCUtR 打洞成功率))

假设:
- 社区节点 50 个, 平均流量 10 Mbps
- DCUtR 打洞成功率 80% (对称 NAT 外)
- relay 兜底比例 20%

relay 带宽 = 50 × 10 Mbps × 20% = 100 Mbps
分摊到 2 个 L4 relay 节点 = 50 Mbps/节点
```

**缓解**：
1. **优先 DCUtR 打洞**：libp2p 默认尝试打洞，成功后不走 relay
2. **relay 资源限制**：Circuit Relay v2 内置限制（每节点最大 relay 连接数、字节配额、时长），见 network.md §2.3 配置
3. **relay 角色可关闭**：带宽紧张的 L4 节点可配置 `relay_provider: false`，仅作 DCUtR 协调方
4. **监控告警**：relay 中转流量超过阈值时告警，提示扩容或限制社区节点接入数

**relay 流量区分**：relay 中转流量与 L4 节点自身回源流量独立计量，Prometheus 指标分开（`edge_relay_bytes_total` vs `edge_backhaul_bytes_total`），便于成本归因。


---

## 13. 分发层监控指标

所有指标通过 Prometheus 采集，Grafana 仪表盘展示。

### 13.1 节点缓存指标

```promql
# 段缓存命中率 -- 热数据（目标 > 80%）
sum(rate(edge_cache_hit_total{cache_type="warm"}[5m])) /
sum(rate(edge_cache_request_total{cache_type="warm"}[5m]))

# 段缓存命中率 -- 全局（含 prefix）
sum(rate(edge_cache_hit_total[5m])) /
sum(rate(edge_cache_request_total[5m]))

# 首字节延迟 P95（目标 < 3s）
histogram_quantile(0.95,
  sum(rate(edge_ttfb_seconds_bucket[5m])) by (le))

# 兄弟节点互助命中率
sum(rate(edge_peer_hit_total[5m])) /
sum(rate(edge_peer_request_total[5m]))

# 回源带宽利用率
edge_backhaul_bandwidth_bytes / edge_backhaul_capacity_bytes
```

> **注意**：L4 节点数据面指标（回源成功率、回源延迟 P95、账号健康分布、链接池命中率等）属于数据面 / 存储层范畴，见 storage/README.md。

### 13.2 告警规则（分发相关）

```yaml
# prometheus-alerts.yml
groups:
  - name: cloud_dash_critical
    rules:
      # 回源成功率骤降
      - alert: HighBackhaulFailureRate
        expr: |
          sum(rate(access_backhaul_success_total[5m])) /
          sum(rate(access_backhaul_request_total[5m])) < 0.95
        for: 3m
        annotations:
          summary: "网盘回源成功率 < 95%（当前 {{ $value | humanizePercentage }}）"

      # 边缘缓存命中率低于警戒线
      - alert: LowCacheHitRate
        expr: |
          sum(rate(edge_cache_hit_total{cache_type="warm"}[15m])) /
          sum(rate(edge_cache_request_total{cache_type="warm"}[15m])) < 0.60
        for: 10m
        annotations:
          summary: "温缓存命中率 < 60%，回源压力过大"

      # 接入层实例故障
      - alert: L4NodeDown
        expr: up{job="l4-node"} == 0
        for: 1m
        annotations:
          summary: "L4 节点实例 {{ $labels.instance }} 宕机"

      # 区域级 L4 全部不可达（相关故障，不自动恢复）
      - alert: RegionalL4AllDown
        expr: |
          count by (region) (up{job="l4-node"} == 0) ==
          count by (region) (up{job="l4-node"})
        for: 1m
        annotations:
          summary: "区域 {{ $labels.region }} 所有 L4 节点不可达，需人工介入"

      # 调度层失联（节点发现入口不可达，可恢复）
      - alert: SchedulerPartition
        expr: |
          count(edge_jwt_refresh_success_total[5m]) == 0
          and on() (time() - edge_jwt_refresh_last_success_timestamp > 300)
        for: 2m
        annotations:
          summary: "JWT 续签连续 5min 失败，调度层可能不可达，节点已进入 PeerStore 缓存降级模式"

      # 社区节点拜占庭行为
      - alert: PeerScoreGraylist
        expr: |
          count(edge_peer_score < -10) by (region) > 5
        for: 5m
        annotations:
          summary: "区域 {{ $labels.region }} 有 >5 个节点评分低于 GraylistThreshold，疑似协同攻击或网络异常"

      # 首帧延迟过高
      - alert: HighTTFB
        expr: |
          histogram_quantile(0.95,
            sum(rate(edge_ttfb_seconds_bucket[5m])) by (le)
          ) > 3
        for: 5m
        annotations:
          summary: "首帧延迟 P95 > 3s（当前 {{ $value }}s）"
```

> **注意**：账号大规模 ban、厂商全挂等告警属于数据面 / 存储层范畴，见 storage/README.md。

---

## 跨文档索引

| 涉及概念 | 文档 |
|---------|------|
| 段文件存储（blob_location 表）、段索引 | storage/README.md |
| 元数据服务（PostgreSQL + Redis 设计） | storage/README.md |
| 数据面回源（Driver、BackendPool、LinkPool、CircuitBreaker） | storage/README.md |
| Backend 抽象与链路策略控制（PolicyController 回源/上传权重） | policy/README.md |
| 入库流程（Ingest 编排、K=2 冗余上传） | ingest/README.md |
| 系统总体架构、SLO、设计原则 | 主文档 README.md |
| SyncBroadcaster 同步协议 | 主文档 README.md |
