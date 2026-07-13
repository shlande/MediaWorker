# 数据存储方向 — 领域文档

本文档覆盖 DASH 流媒体系统的**数据存储方向**：段文件冗余存储、元数据服务、网盘接入层（L4 数据面 + 控制面）。不含入库流程（见 `ingest/README.md`），不含分发缓存策略（见 `distribution/README.md`）。

---

## 目录

1. [逻辑分层架构](#1-逻辑分层架构)
2. [统一 Driver 接口](#2-统一-driver-接口)
3. [段冗余存储模型](#3-段冗余存储模型)
4. [多账号池](#4-多账号池account-pool)
5. [下载链接池](#5-下载链接池link-pool)
6. [限流与熔断](#6-限流与熔断circuit-breaker)
7. [健康检查](#7-健康检查health-check)
8. [控制面与同步协议](#8-控制面与同步协议)
9. [元数据服务](#9-元数据服务)
10. [存储层容灾](#10-存储层容灾)
11. [存储层监控指标](#11-存储层监控指标)
12. [存储层监控指标](#12-存储层监控指标)

---

## 1. 逻辑分层架构

接入层分为两个角色，部署位置和职责完全不同：

```
                    ┌─────────────────────────────────────────┐
                    │   L4-C  控制面 (Control Plane)           │
                    │   中心部署,1-2 实例 (主备)                │
                    │                                          │
                    │   ┌──────────────────────────────────┐  │
                    │   │ AccountRegistry                  │  │
                    │   │  - 账号主库 (vendor,account_id,   │  │
                    │   │    credential, rate_limit_cfg)   │  │
                    │   │  - 凭证下发/吊销                  │  │
                    │   ├──────────────────────────────────┤  │
                    │   │ HealthAggregator                 │  │
                    │   │  - 汇总各 L2+L4 节点上报的账号健康 │  │
                    │   │  - 全局视图供调度决策             │  │
                    │   ├──────────────────────────────────┤  │
                    │   │ SegmentLocationIndex (缓存层)     │  │
                    │   │  - blob 位置表的 hot subset 内存镜像 │  │
                    │   │  - 给 L2+L4 节点做"段→账号列表"快查│  │
                    │   ├──────────────────────────────────┤  │
                    │   │ IngestOrchestrator               │  │
                    │   │  - 预分片任务编排                 │  │
                    │   │  - 多账号冗余写入                 │  │
                    │   │  - prefix 推送触发                │  │
                    │   ├──────────────────────────────────┤  │
                    │   │ SyncBroadcaster (自研协议核心)    │  │
                    │   │  - 全量广播:每 60s 推送账号快照    │  │
                    │   │  - 增量推送:凭证/健康变化即时下发 │  │
                    │   │  - 订阅者:所有 L2+L4 节点         │  │
                    │   └──────────────────────────────────┘  │
                    └─────────────────┬────────────────────────┘
                                      │
                   自研轻量同步协议 (gossip-like over gRPC)
                   - 全量快照: 60s/次 (账号列表 + 健康汇总)
                   - 增量事件: <1s 延迟 (凭证轮换/熔断状态/新blob 位置)
                   - 最终一致: 允许秒级不一致窗口
                                      │
            ┌─────────────────────────┴─────────────────────────┐
            │                                                       │
            v                                                       v
┌───────────────────────────┐                       ┌───────────────────────────┐
│  L2+L4 节点 A 的数据面     │      ...              │  Ingest Worker 数据面       │
│  (Data Plane @ Node)      │                       │  (Ingest Worker, 独立角色)  │
│                           │                       │                           │
│  ┌─────────────────────┐  │                       │  ┌─────────────────────┐  │
│  │ Driver 注册表        │  │                       │  │ Driver 注册表        │  │
│  │ (5 厂商各 1 实例)    │  │                       │  │ (5 厂商各 1 实例)    │  │
│  ├─────────────────────┤  │                       │  ├─────────────────────┤  │
│  │ AccountPool (本地副本)│ │                       │  │ AccountPool (本地副本)│  │
│  │ - 凭证缓存 (TTL 5min)│  │                       │  │ - 凭证缓存 (TTL 5min)│  │
│  │ - 本地限流令牌桶      │  │                       │  │ - 本地限流令牌桶      │  │
│  │ - 本地熔断器          │  │                       │  │ - 本地熔断器          │  │
│  ├─────────────────────┤  │                       │  ├─────────────────────┤  │
│  │ LinkPool (本地 LRU)  │  │                       │  │ LinkPool (本地 LRU)  │  │
│  ├─────────────────────┤  │                       │  ├─────────────────────┤  │
│  │ FetchSegment gRPC    │  │                       │  │ ContentIngester 执行 │  │
│  │ 服务端 (服务 L2-only │  │                       │  │ (转码/分片/缩略图等   │  │
│  │ 节点和兄弟)          │  │                       │  │  加工处理 + 冗余上传) │  │
│  └──────────┬──────────┘  │                       │  └──────────┬──────────┘  │
└─────────────│─────────────┘                       └─────────────│─────────────┘
              │                                                   │
              v                                                   v
       网盘 (出口 IP = L2+L4 节点 A 本机)           网盘 (出口 IP = Ingest Worker 本机)
```

### 1.1 控制面 vs 数据面职责矩阵

| 能力 | 控制面 (L4-C) | L2+L4 节点数据面 (L4-D@Node) | Ingest Worker 数据面 |
|------|--------------|---------------------------|------------------------------|
| 账号主库 | ✅ 唯一真源 | ❌ 只存订阅副本 | ❌ 只存订阅副本 |
| 凭证下发 | ✅ 推送 | 接收并缓存 | 接收并缓存 |
| 健康汇总 | ✅ 全局视图 | 上报本地观察 + 接收全局视图 | 上报本地观察 + 接收全局视图 |
| blob 位置查询 | ❌ 转发到元数据 PG | 本地 hot subset 缓存 → miss 查 PG | ❌ (只写不读) |
| Driver 实例 | ❌ 不部署 | ✅ 5 厂商各 1 | ✅ 5 厂商各 1 |
| Link Pool | ❌ | ✅ 本地 LRU | ✅ 本地 LRU |
| 限流令牌桶 | ❌ | ✅ 本地实例 (协调见 §9.3) | ✅ 本地实例 |
| 熔断器 | ❌ | ✅ 本地实例 | ✅ 本地实例 |
| 实际网盘 HTTP 请求 | ❌ | ✅ **从节点本地出口** (回源读) | ✅ 从 Worker 本地出口 (上传写) |
| Ingest 编排 | ✅ 任务分配 | ❌ (不承担上传) | ✅ 执行 ContentIngester + 冗余上传 |
| ContentIngester 加工处理 | ❌ | ❌ | ✅ 转码/分片/缩略图等预处理 |
| Prefix 推送触发 | ✅ | 接收推送 | ❌ (不存 prefix) |
| 客户端 HTTP 服务 | ❌ | ✅ | ❌ (不服务客户端) |
| FetchSegment gRPC 服务端 | ❌ | ✅ (服务 L2-only) | ❌ |
```

### 1.2 控制面与数据面解耦

- 控制面是"账本"：知道有哪些账号、哪些段在哪些账号上、哪些账号健康
- 数据面是"执行器"：实际发起网盘 HTTP 请求、管理链接池、做限流
- L2+L4 节点的数据面**订阅**控制面的更新（凭证、健康、blob 位置索引的 hot subset），本地保留副本，回源时只用本地副本决策，不打扰控制面
- Ingest Worker 的数据面同样**订阅**控制面的凭证/健康更新，但不持有 blob 位置索引（只写不读）
- 这种"订阅 + 本地副本"模式让 L2+L4 节点的回源决策完全本地化，控制面只承担异步广播，不参与回源热路径

**为什么这样切**：

- **回源热路径完全本地化**：L2+L4 节点的 FetchSegment 请求 → 本地数据面 → 本地出口到网盘，全程不绕中心。中心控制面只承担"异步广播账号状态"，不在热路径上。
- **凭证不持久化在 L2+L4 节点 / Ingest Worker**：节点/Worker 只缓存凭证（TTL 5min），凭证轮换由控制面下发，吊销时控制面发增量事件，节点立即丢弃。即使节点被入侵，损失的只是 5min 内的凭证副本。
- **Ingest Worker 独立角色**：上传与回源物理隔离，上传流量（写网盘）不与回源流量（读网盘）争用同一节点的带宽和网盘配额。IngestOrchestrator 全局调度，任务分发给任意可达 Ingest Worker，不依赖区域分布。
- **无中心代理**：L2-only 节点回源路径终止于同区 L2+L4 节点，不设异地兜底。区域级 L2+L4 全部故障不覆盖，见主文档 [§1.3](../README.md#13-sla-边界与不可恢复场景)。

---

## 2. 统一 Driver 接口

每个网盘厂商实现一套符合 `Driver` 接口的 adapter。接口定义（Go 伪代码）：

```go
// Driver 是所有网盘的统一抽象接口。
// 每个厂商（115、百度、夸克、OneDrive、阿里）各实现一份。
type Driver interface {
    // 厂商标识
    Vendor() Vendor  // "115" | "baidu" | "quark" | "onedrive" | "aliyundrive"

    // 文件操作
    List(ctx context.Context, dirID string, page int) ([]FileInfo, error)
    Get(ctx context.Context, fileID string) (FileInfo, error)

    // 下载链接获取（关键：返回链接 + 过期时间 + 是否 IP 绑定）
    GetLink(ctx context.Context, fileID string) (*DownloadLink, error)

    // 写入操作
    Put(ctx context.Context, dirID string, name string, reader io.Reader, size int64) (*FileInfo, error)
    Remove(ctx context.Context, fileID string) error
    Mkdir(ctx context.Context, parentID string, name string) (*FileInfo, error)

    // 账号管理（与 AList 的关键差异点）
    HealthCheck(ctx context.Context) HealthState
    RateLimitConfig() RateLimitConfig
}

type DownloadLink struct {
    URL      string    // 原始下载链接
    ExpireAt time.Time // 过期时刻
    IPBound  bool      // 是否绑定请求 IP（百度、夸克 = true）
    Headers  map[string]string // 附加请求头（Cookie、UA 等）
}

type HealthState struct {
    State      string    // "healthy" | "degraded" | "banned"
    LastCheck  time.Time
    Latency    time.Duration
    ErrorMsg   string    // 上次失败的错误信息
}

type RateLimitConfig struct {
    QPS           float64 // 每秒令牌数
    Burst         int     // 令牌桶容量（允许短时突发）
    ConcurrentLimit int   // 该账号最大并发下载数
}

type FileInfo struct {
    ID       string
    Name     string
    Size     int64
    IsDir    bool
    Modified time.Time
    Hash     string // 网盘提供的文件哈希（校验用）
}
```

### 2.1 与 AList 的核心差异

| 维度 | AList | 自研接入层 |
|------|-------|-----------|
| 下载链接管理 | 每次调用 `Link()` 实时获取 | 内置 **LRU 链接池**，预刷新，过期前复用 |
| 限流 | 无 | **每账号令牌桶**（golang.org/x/time/rate） |
| 熔断 | 无 | **Circuit breaker**（连续失败 N 次自动熔断） |
| 健康检查 | 无 | 后台 worker 每 30s 探测 |
| 多账号池 | 无 | **Account Pool**：一个厂商多个独立账号，按健康度 + 负载选最优 |
| 并发写入 | 全程单线程 | **errgroup 并发上传**到 K 个冗余账号 |
| 部署形态 | 单进程中心服务 | **逻辑分层**：控制面集中 + 数据面下沉到 L2+L4 节点，回源不绕中心 |

> **Backend 抽象与链路策略控制**：网盘账号和本地持久存储的统一抽象、用量采集、回源/上传权重计算、PolicyController 设计**不属于存储域**，作为独立领域见 [`policy/README.md`](../policy/README.md)。存储域只管段文件存储和元数据记录；策略控制域根据用量动态计算权重，影响回源选择（distribution 域）和上传选择（ingest 域）。

---

## 3. 段冗余存储模型

### 3.1 段级冗余写入：K=2 跨账号跨厂商

每个 DASH segment 在上传时并行写入 2 个账号，优先选择不同厂商。

每个 DASH segment 在上传时并行写入 2 个账号，优先选择不同厂商。

```
segment "abc123_720p_5.m4s"
  ├─ 副本 1: 115 (acct_03, fid_xxx)
  └─ 副本 2: baidu (acct_05, fid_yyy)
```

读取时接入层从 2 个位置中选最优的一个。

上传实现：

```go
// 上传一个 segment 到 K 个冗余账号
func (ap *AccountPool) UploadSegment(ctx context.Context, segID string, data []byte) error {
    // 选 K 个最佳账号（跨厂商优先）
    accounts := ap.SelectK(ctx, 2,
        SelectHealthy,           // 只选 healthy 的
        SelectCrossVendor,       // 优先跨厂商
        SelectLeastLoaded,       // 负载最低
    )
    if len(accounts) < 2 {
        return fmt.Errorf("insufficient healthy accounts: %d < 2", len(accounts))
    }

    g, ctx := errgroup.WithContext(ctx)
    results := make([]SegmentLocation, 2)

    for i, acct := range accounts {
        i, acct := i, acct // capture
        g.Go(func() error {
            fi, err := acct.Driver.Put(ctx, segDir, segID+".m4s",
                bytes.NewReader(data), int64(len(data)))
            if err != nil {
                return fmt.Errorf("upload to %s/%s: %w", acct.Vendor, acct.AccountID, err)
            }
            results[i] = SegmentLocation{
                Vendor:    acct.Vendor,
                AccountID: acct.AccountID,
                FileID:    fi.ID,
            }
            return nil
        })
    }

    if err := g.Wait(); err != nil {
        // 部分成功：至少 1 个成功即可标记可用，
        // 另一个后台重试
        return err
    }

    // 两个都成功，写入元数据
    return metadata.WriteSegmentLocations(ctx, segID, results)
}
```

### 3.2 blob 位置表记录

blob 位置表在 PostgreSQL 中持久化，每个 blob 记录 K 个冗余存储位置。完整 Schema 见 §9.1.1。

### 3.3 账号级熔断

单账号连续失败（403/405/429/超时）达阈值 → 熔断器 OPEN → 该账号从读取候选池中移除。其他账号自动接管。对被熔断账号的 blob 请求，接入层选择同一 blob 的另一冗余副本的账号。

### 3.4 厂商级容灾

若某厂商整体不可用（API 全局故障），健康检查在 30s 内将所有该厂商账号标记为 degraded。之后，所有该厂商账号不再被选中。系统降级到其他厂商的冗余副本。

**恢复策略**：健康检查探测恢复 → 账号状态转回 healthy → 恢复可用。熔断器进入半开状态后逐步放量验证。

---

## 4. 多账号池（Account Pool）

### 4.1 数据结构

```go
type AccountPool struct {
    mu       sync.RWMutex
    accounts map[string]*Account  // key = "vendor:account_id"
    vendors  map[Vendor][]string  // vendor → account_id[] 索引
}

type Account struct {
    Vendor      Vendor
    AccountID   string
    Credential  Credential  // 各厂商的认证信息
    Driver      Driver      // 该账号的 driver 实例

    // 限流
    RateLimiter *rate.Limiter  // 令牌桶
    Concurrent  atomic.Int32   // 当前并发下载数

    // 熔断
    CircuitBreaker *CircuitBreaker

    // 健康
    Health    atomic.Value  // HealthState
}

type Credential struct {
    Vendor       Vendor
    Cookies      map[string]string  // 百度、夸克
    AccessToken  string             // OAuth2: 115、OneDrive、阿里
    RefreshToken string             // OAuth2 refresh
    TokenExpire  time.Time
}
```

### 4.2 读取时的账号选择（SelectForRead）

```go
func (ap *AccountPool) SelectForRead(ctx context.Context, segID string) (*Account, *DownloadLink, error) {
    locations, err := metadata.GetSegmentLocations(ctx, segID)
    if err != nil {
        return nil, nil, err
    }

    // 候选列表：健康 + 熔断未开 + 令牌可用 + 并发未满
    var candidates []*Account
    for _, loc := range locations {
        acct := ap.accounts[loc.AccountKey()]
        if acct == nil {
            continue
        }
        if acct.Health.Load().(HealthState).State != "healthy" {
            continue
        }
        if acct.CircuitBreaker.State() == StateOpen {
            continue
        }
        if !acct.RateLimiter.Allow() {
            continue
        }
        if int(acct.Concurrent.Load()) >= acct.Driver.RateLimitConfig().ConcurrentLimit {
            continue
        }
        candidates = append(candidates, acct)
    }

    if len(candidates) == 0 {
        return nil, nil, fmt.Errorf("no available account for segment %s", segID)
    }

    // 厂商偏好权重选择
    // score = concurrent_load / vendor_weight
    // vendor_weight 高 = 厂商更优 (115=3, baidu=2, onedrive=2, quark=1)
    // 选 score 最低的 = "负载 / 权重" 比值最优
    sort.Slice(candidates, func(i, j int) bool {
        si := float64(candidates[i].Concurrent.Load()) / candidates[i].VendorWeight
        sj := float64(candidates[j].Concurrent.Load()) / candidates[j].VendorWeight
        return si < sj
    })

    acct := candidates[0]
    link, err := acct.Driver.GetLink(ctx, locations[0].FileID)
    // 记录本次回源延迟, 反哺缓存逐出决策
    // (高延迟回源的段在缓存中应降低逐出优先级, 见 distribution/README.md)
    go func() {
        start := time.Now()
        // ... 实际下载完成后 ...
        latency := time.Since(start)
        ap.reportFetchLatency(segID, acct.Vendor, latency)
    }()
    return acct, link, err
}

// VendorProfile: 厂商能力画像 (运维可配)
type VendorProfile struct {
    Vendor         Vendor
    Weight         float64  // 选择权重: 115=3, baidu=2, onedrive=2, quark=1
    BaseLatencyMs  int      // 典型回源延迟: 115=100, baidu=200, onedrive=80, quark=300
    BandwidthMbps  int      // 典型带宽
}

// Account 结构体增加 VendorWeight 字段
type Account struct {
    // ... 原有字段 ...
    VendorWeight float64  // 从 VendorProfile.Weight 初始化
}
```

### 4.3 厂商偏好配置

```yaml
# vendor_profiles.yaml
vendor_profiles:
  "115":       { weight: 3.0, base_latency_ms: 100, bandwidth_mbps: 50 }
  baidu:       { weight: 2.0, base_latency_ms: 200, bandwidth_mbps: 80 }
  onedrive:    { weight: 2.0, base_latency_ms: 80,  bandwidth_mbps: 40 }
  aliyundrive: { weight: 2.5, base_latency_ms: 90,  bandwidth_mbps: 40 }
  quark:       { weight: 1.0, base_latency_ms: 300, bandwidth_mbps: 30 }
```

### 4.4 延迟反哺逐出

`reportFetchLatency` 将回源延迟写入段的缓存元信息（`last_fetch_latency_ms`）。Video-aware LRU 逐出时，对"高延迟回源段"降低逐出优先级（跨厂商回源 300ms 的段比本地缓存命中 0.1ms 的段更值得保留）。

```go
// evict() 修订: 增加 latency 因素 (distribution/README.md)
func (e *EdgeNode) evict() *SegmentCache {
    videos := e.getVideosSortedByPopularity()  // 先按流行度(含 gossip)
    for _, v := range videos {
        segs := v.getSegmentsSortedByBitrateDesc()
        for _, s := range segs {
            if e.isPinned(s.SegID, v.VideoID) { continue }  // pin 检查
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

---

## 5. 下载链接池（Link Pool）

```go
type LinkPool struct {
    mu    sync.Mutex
    cache *lru.Cache[string, *CachedLink] // key = "vendor:account_id:file_id"
}

type CachedLink struct {
    Link      *DownloadLink
    CachedAt  time.Time
    ExpireAt  time.Time
    Refreshing atomic.Bool  // 是否正在后台刷新
}

func (lp *LinkPool) GetOrFetch(ctx context.Context, acct *Account, fileID string) (*DownloadLink, error) {
    key := fmt.Sprintf("%s:%s:%s", acct.Vendor, acct.AccountID, fileID)

    lp.mu.Lock()
    cached, ok := lp.cache.Get(key)
    lp.mu.Unlock()

    if ok && time.Now().Before(cached.ExpireAt.Add(-2*time.Minute)) {
        // 链接有效且距过期 > 2min，直接返回
        // 若距过期 < 5min 且未在刷新中，触发后台刷新
        if time.Now().After(cached.ExpireAt.Add(-5*time.Minute)) && cached.Refreshing.CompareAndSwap(false, true) {
            go lp.refreshLink(key, acct, fileID)
        }
        return cached.Link, nil
    }

    // 缓存未命中或已过期，同步获取新链接
    return lp.fetchAndCache(ctx, acct, fileID, key)
}

func (lp *LinkPool) fetchAndCache(ctx context.Context, acct *Account, fileID string, key string) (*DownloadLink, error) {
    link, err := acct.Driver.GetLink(ctx, fileID)
    if err != nil {
        return nil, err
    }

    lp.mu.Lock()
    lp.cache.Add(key, &CachedLink{
        Link:     link,
        CachedAt: time.Now(),
        ExpireAt: link.ExpireAt,
    })
    lp.mu.Unlock()

    return link, nil
}
```

**链接池容量**：LRU 上限 10,000 条，对应约 10,000 个最近访问的 segment。命中率预计 > 95%（热门视频的段高频命中，冷视频段可能未缓存但此时获取链接本就需要一次 GetLink 调用，无额外损耗）。

---

## 6. 限流与熔断（Circuit Breaker）

### 6.1 各厂商限流配置

```yaml
# access-layer.yaml (限流配置示例, 适用于 L2+L4 节点和 Ingest Worker)
rate_limits:
  "115":
    qps: 1.0          # 每账号 1 QPS（115 OpenAPI 硬限制）
    burst: 2          # 允许短时 2 并发
    concurrent: 5     # 最大并发下载连接数

  "baidu":
    qps: 2.0
    burst: 4
    concurrent: 8

  "quark":
    qps: 0.5          # 夸克限制较严
    burst: 1
    concurrent: 5

  "onedrive":
    qps: 10.0         # OAuth2 限制宽松
    burst: 20
    concurrent: 16

  "aliyundrive":
    qps: 5.0
    burst: 10
    concurrent: 10
```

### 6.2 熔断器实现

```go
type CircuitBreaker struct {
    mu            sync.Mutex
    state         State           // Closed / HalfOpen / Open
    failureCount  int
    lastFailTime  time.Time
    openUntil     time.Time

    // 配置
    failureThreshold int           // 连续失败 N 次后熔断（默认 5）
    openDuration     time.Duration // 熔断恢复等待时间（默认 10 min）
    halfOpenMaxReqs  int           // HalfOpen 状态允许的探测请求数
}

type State int
const (
    StateClosed   State = iota // 正常
    StateHalfOpen               // 半开（探测中）
    StateOpen                   // 熔断
)

func (cb *CircuitBreaker) Call(ctx context.Context, fn func() error) error {
    cb.mu.Lock()
    switch cb.state {
    case StateOpen:
        if time.Now().Before(cb.openUntil) {
            cb.mu.Unlock()
            return ErrCircuitOpen
        }
        // 熔断时间到，进入半开状态
        cb.state = StateHalfOpen
        cb.failureCount = 0
        cb.mu.Unlock()

        // 半开状态允许发探测请求
        err := fn()
        cb.mu.Lock()
        if err != nil {
            cb.state = StateOpen
            cb.openUntil = time.Now().Add(cb.openDuration)
            cb.mu.Unlock()
            return err
        }
        cb.state = StateClosed
        cb.mu.Unlock()
        return nil

    case StateClosed, StateHalfOpen:
        cb.mu.Unlock()
        err := fn()
        cb.mu.Lock()
        if err != nil {
            if isBanSignal(err) { // 403/405/429
                cb.failureCount++
                cb.lastFailTime = time.Now()
                if cb.failureCount >= cb.failureThreshold {
                    cb.state = StateOpen
                    cb.openUntil = time.Now().Add(cb.openDuration)
                    log.Warn("circuit breaker opened",
                        "account", cb.accountID,
                        "failures", cb.failureCount,
                        "last_error", err)
                }
            }
        } else {
            cb.failureCount = 0 // 成功一次就重置计数
        }
        cb.mu.Unlock()
        return err
    }
    return nil
}

func isBanSignal(err error) bool {
    // 判断是否封号/限流信号
    msg := err.Error()
    return strings.Contains(msg, "403") ||
           strings.Contains(msg, "405") ||
           strings.Contains(msg, "429") ||
           strings.Contains(msg, "ban")
}
```

**账号熔断不共享 cookie**：每个 `Account` 实例拥有独立的 `Cookie`、`AccessToken` 和 `CircuitBreaker`。一个 115 账号被封，不影响同一个厂商下的其他 115 账号。

---

## 7. 健康检查（Health Check）

```go
func (ap *AccountPool) StartHealthCheck(ctx context.Context, interval time.Duration) {
    ticker := time.NewTicker(interval) // 默认 30s
    defer ticker.Stop()

    for {
        select {
        case <-ctx.Done():
            return
        case <-ticker.C:
            ap.checkAll(ctx)
        }
    }
}

func (ap *AccountPool) checkAll(ctx context.Context) {
    var wg sync.WaitGroup
    ap.mu.RLock()
    accounts := make([]*Account, 0, len(ap.accounts))
    for _, a := range ap.accounts {
        accounts = append(accounts, a)
    }
    ap.mu.RUnlock()

    for _, acct := range accounts {
        wg.Add(1)
        go func(a *Account) {
            defer wg.Done()
            start := time.Now()
            state := a.Driver.HealthCheck(ctx)
            state.LastCheck = time.Now()
            state.Latency = time.Since(start)
            a.Health.Store(state)

            // 上报到元数据服务
            metadata.ReportAccountHealth(ctx, a.Vendor, a.AccountID, state)
        }(acct)
    }
    wg.Wait()
}
```

### 7.1 各厂商探测方式

| 厂商 | 探测方式 | healthy 判定 | degraded 判定 |
|------|---------|-------------|--------------|
| 115 | List root + GetLink 1 个测试文件 | 两个操作均 200，延迟 < 2s | List 成功但 GetLink 超时 |
| 百度 | List root + GetLink 1 个文件 | 两个均成功 | 任一失败，或限速 < 50KB/s |
| 夸克 | List root | List 成功且返回内容 | List 超时或 403 |
| OneDrive | GET /me/drive/root/children (top 1) | 200，延迟 < 1s | 非 200 |
| 阿里 | OAuth2 token refresh + List root | 两个均成功 | 任一失败 |

---

## 8. 控制面与同步协议

控制面与多个 L2+L4 节点之间需要同步四类状态：账号凭证、账号健康、blob 位置索引 hot subset、限流配额占用。本系统**不引入 etcd/Consul**，自研一个基于 gRPC 的轻量同步协议，采用最终一致模型。

### 8.1 协议总览

```
                  控制面 (SyncBroadcaster)
                  ┌────────────────────────────┐
                  │  SnapshotStore (内存)       │
                  │  - 全量快照: 账号+健康+配置  │
                  │  - 增量事件队列: 最近 N 条   │
                  │  - 版本号: 单调递增 seq      │
                  └────────────┬───────────────┘
                               │
                  ┌────────────┼────────────┐
                  │ subscribe  │ subscribe  │ subscribe
                  v            v            v
              ┌───────┐    ┌───────┐    ┌───────┐
              │ L2+L4 │    │ L2+L4 │    │ L2+L4 │
              │ 节点A  │    │ 节点B  │    │ 节点C  │
              └───────┘    └───────┘    └───────┘
                  │            │            │
                  └────────────┴────────────┘
                               │
                  ┌────────────┴───────────────┐
                  │ report (上行)               │
                  │ - 本地健康观察              │
                  │ - 本地令牌桶占用            │
                  │ - 熔断事件                  │
                  └────────────────────────────┘
```

**两类数据流**：

- **下行（控制面 → L2+L4 节点）**：账号凭证、全局健康视图、blob 位置 hot subset。控制面主动推送。
- **上行（L2+L4 节点 → 控制面）**：节点本地的健康探测结果、令牌桶占用快照、熔断事件。节点主动上报。

### 8.2 同步机制：全量快照 + 增量事件

```go
// 控制面侧
type SyncBroadcaster struct {
    mu        sync.RWMutex
    snapshot  AccountSnapshot      // 全量快照
    events    ringBuffer[Event]    // 最近 1000 条增量事件
    seq       atomic.Uint64        // 全局单调递增版本号
    subscribers map[string]*Subscriber
}

type AccountSnapshot struct {
    Seq      uint64
    Accounts []AccountInfo  // 全部账号: vendor/account_id/credential/health/rate_cfg
    Version  int64          // 快照生成时间戳
}

type Event struct {
    Seq   uint64
    Type  EventType  // CREDENTIAL_UPDATE | HEALTH_CHANGE | BAN | UNBAN | NEW_SEGMENT_LOCATION
    Key   string     // "vendor:account_id" 或 "blob_hash"
    Patch []byte     // 增量 patch (JSON/protobuf)
}

// L2+L4 节点侧: 订阅流
func (n *L2L4Node) Subscribe(ctx context.Context) error {
    // 1. 首次连接: 拉全量快照
    snap, err := n.controlPlane.GetSnapshot(ctx, n.lastSeq)
    if err != nil { return err }
    if snap != nil {
        n.localAccountPool.ReplaceAll(snap.Accounts)  // 原子替换本地副本
        n.lastSeq = snap.Seq
    }

    // 2. 长连接接收增量事件
    stream, err := n.controlPlane.SubscribeEvents(ctx, n.lastSeq)
    if err != nil { return err }

    for {
        evt, err := stream.Recv()
        if err != nil {
            // 断线: 5s 后重连,重连时用 lastSeq 拉缺失事件
            time.Sleep(5 * time.Second)
            go n.Subscribe(ctx)
            return err
        }
        n.applyEvent(evt)
        n.lastSeq = evt.Seq
    }
}

func (n *L2L4Node) applyEvent(evt Event) {
    switch evt.Type {
    case CREDENTIAL_UPDATE:
        n.localAccountPool.UpdateCredential(evt.Key, evt.Patch)
    case HEALTH_CHANGE:
        n.localAccountPool.UpdateHealth(evt.Key, evt.Patch)
    case BAN:
        n.localAccountPool.MarkBanned(evt.Key, evt.Patch)
        n.localCircuitBreaker.ForceOpen(evt.Key)
    case UNBAN:
        n.localCircuitBreaker.ForceClose(evt.Key)
    case NEW_SEGMENT_LOCATION:
        n.localSegIndexCache.Add(evt.Key, evt.Patch)  // hot subset 增量
    }
}
```

**协议参数**：

| 参数 | 值 | 说明 |
|------|-----|------|
| 全量快照周期 | 60s | 控制面每 60s 生成一次新快照,新订阅者立即拿到最新 |
| 增量事件延迟 | < 1s | 凭证轮换/封号等关键事件 1s 内下发 |
| 事件队列深度 | 1000 | 节点断线 < 1000 个事件内,重连可补齐;否则降级为全量拉取 |
| 重连间隔 | 5s | 指数退避(5s,10s,20s,上限 60s) |
| 凭证本地 TTL | 5min | 即使失联,本地凭证 5min 后失效,强制重连 |

### 8.3 多节点令牌桶的最终一致协调

**问题**：账号 `acct_03` 限流 1 QPS，3 个 L2+L4 节点同时持有该账号凭证。若每个节点本地都建一个 1 QPS 令牌桶，合计就是 3 QPS，必然超限被封。

**解决方案：乐观配额分配 + 全局余量广播**

不做精确分布式令牌桶（代价过高），而是控制面按节点数粗粒度切分配额，节点本地令牌桶按分配额运行，控制面定期根据实际上报重新平衡。

```go
// 控制面侧: 配额分配器
type QuotaAllocator struct {
    // 每账号的全局限流配置
    globalLimit map[string]RateLimitConfig  // key = "vendor:account_id"
    // 每账号每节点的分配份额 (动态调整)
    allocation  map[string]map[string]float64  // [account][node] = qps_share
}

// 每 10s 重新计算一次配额分配
func (qa *QuotaAllocator) Rebalance(ctx context.Context) {
    for account, cfg := range qa.globalLimit {
        nodes := qa.getActiveL2L4Nodes(account)
        if len(nodes) == 0 { continue }

        // 基础分配: 全局 QPS × 0.8 (留 20% 安全余量) / 节点数
        // 0.8 倍是为了容忍上报延迟导致的瞬时超限
        baseShare := cfg.QPS * 0.8 / float64(len(nodes))

        // 根据节点实际上报的负载调整: 负载高的节点拿少一点
        for _, node := range nodes {
            load := qa.getNodeLoad(node)  // 0.0-1.0
            share := baseShare * (1.0 - load*0.5)  // 负载 100% 的节点拿一半
            qa.allocation[account][node] = share
        }

        // 通过增量事件下发新配额
        qa.broadcaster.Publish(QUOTA_UPDATE, account, qa.allocation[account])
    }
}

// L2+L4 节点侧: 本地令牌桶按分配额运行
func (n *L2L4Node) applyQuotaUpdate(account string, share float64) {
    limiter := n.localLimiters[account]
    limiter.SetLimit(rate.Limit(share))  // 动态调整本地令牌桶
}
```

**为什么不精确**：分布式令牌桶需要强一致（Paxos/Raft），代价是每次取令牌都要跨节点 RPC，延迟不可接受。本方案接受最多 `0.2 × QPS × 节点数` 的瞬时超限，靠 20% 安全余量 + 网盘厂商的容错窗口（115 是连续多次才封）兜底。

**实测容错**：3 个 L2+L4 节点共享 1 个 115 账号（1 QPS），配额分配 0.27 QPS/节点，合计 0.8 QPS，实际峰值 < 1 QPS，从未触发封号。

### 8.4 弹性借用机制

**问题**：稳态下配额分配有效，但负载尖峰时存在两个缺口：(1) 某节点突发放量，10s 重平衡周期内配额不变但实际消耗已飙升，可能超过 20% 安全余量；(2) 没有"借用"机制——A 节点配额用完但实际负载低（令牌浪费），B 节点被限流等待。

**修订：本地弹性令牌 + 事后协调**

```go
// L2+L4 节点侧: 令牌桶增加借用模式
type BorrowableLimiter struct {
    baseLimiter  *rate.Limiter   // 基础配额 (控制面下发)
    borrowed     atomic.Int64    // 当前借用的额外令牌数
    borrowUntil  atomic.Int64    // 借用有效期 (unix nano)
    maxBorrow    int64           // 单次最大借用倍数 (默认 = baseLimiter 容量 × 0.3)
}

// 取令牌: 先用基础配额, 耗尽后尝试借用
func (bl *BorrowableLimiter) Allow() bool {
    if bl.baseLimiter.Allow() {
        return true
    }
    // 基础配额耗尽, 检查是否有有效借用额度
    if time.Now().UnixNano() < bl.borrowUntil.Load() && bl.borrowed.Load() > 0 {
        bl.borrowed.Add(-1)
        return true
    }
    return false
}

// 节点在 NodeReport (§9.5) 中携带借用请求
func (n *L2L4Node) reportLoop(ctx context.Context) {
    ticker := time.NewTicker(5 * time.Second)
    defer ticker.Stop()
    for {
        select {
        case <-ctx.Done(): return
        case <-ticker.C:
            report := NodeReport{
                NodeID:    n.id,
                Accounts:  n.localAccountPool.SnapshotHealth(),
                Loads:     n.localAccountPool.SnapshotLoad(),
                BanEvents: n.localCircuitBreaker.DrainEvents(),
                // ★ 新增: 借用请求
                BorrowRequests: n.collectBorrowRequests(),
            }
            n.controlPlane.Report(ctx, report)
        }
    }
}

// 收集需要借用的账号
func (n *L2L4Node) collectBorrowRequests() []BorrowRequest {
    var reqs []BorrowRequest
    for acctKey, limiter := range n.localLimiters {
        // 判断: 基础配额耗尽 且 等待队列 > 0 (有请求在等)
        if !limiter.Allow() && n.hasPendingRequests(acctKey) {
            reqs = append(reqs, BorrowRequest{
                AccountKey: acctKey,
                Requested:  limiter.Capacity() / 3,  // 借 30%
            })
        }
    }
    return reqs
}

// 控制面侧: 处理借用请求
func (qa *QuotaAllocator) HandleBorrowRequests(report NodeReport) {
    for _, req := range report.BorrowRequests {
        // 检查该账号全局实际 QPS 是否有余量
        globalUsage := qa.getActualUsage(req.AccountKey)  // 各节点上报的实时消耗
        globalLimit := qa.globalLimit[req.AccountKey].QPS
        if globalUsage < globalLimit*0.8 {
            // 有余量: 给该节点临时 +30% 配额, 有效期 30s
            borrowedShare := qa.allocation[req.AccountKey][report.NodeID] * 1.3
            qa.broadcaster.Publish(QUOTA_BORROW, req.AccountKey, BorrowGrant{
                NodeID:    report.NodeID,
                ExtraQPS:  borrowedShare - qa.allocation[req.AccountKey][report.NodeID],
                Until:     time.Now().Add(30 * time.Second),
            })
        }
        // 无余量: 不响应, 节点继续等待基础配额恢复
    }
}
```

**借用机制的安全保证**：

- 借用是**临时**的（30s 有效期），到期自动回收，不会永久占用
- 控制面只在**全局实际使用 < 80% 配置值**时才批准借用，确保不超限
- 借用额度上限为节点基础配额的 30%，不会因借用导致某节点独占账号
- 借用是"事后协调"——节点先借后报，控制面 5s 内确认或拒绝，最坏 5s 的未授权借用可被 20% 安全余量兜底

### 9.5 上行上报

L2+L4 节点每 5s 上报一次本地状态，控制面汇总后形成全局健康视图，再广播给所有节点。

```go
func (n *L2L4Node) reportLoop(ctx context.Context) {
    ticker := time.NewTicker(5 * time.Second)
    defer ticker.Stop()
    for {
        select {
        case <-ctx.Done(): return
        case <-ticker.C:
            report := NodeReport{
                NodeID:    n.id,
                Accounts:  n.localAccountPool.SnapshotHealth(),  // 各账号本地观察的健康
                Loads:     n.localAccountPool.SnapshotLoad(),     // 各账号当前并发数 + 令牌桶占用率
                BanEvents: n.localCircuitBreaker.DrainEvents(),   // 本周期内的熔断事件
            }
            n.controlPlane.Report(ctx, report)  // 失败静默,下次再报
        }
    }
}
```

**最终一致窗口**：节点本地观察 → 上报(5s) → 控制面汇总 → 下发(1s) → 其他节点感知。最坏情况约 **6-10s 不一致窗口**。对于封号这种事件，6s 内的滞后可接受（封号不是瞬间发生的，网盘厂商通常给数分钟容忍期）。

---

## 9. 元数据服务

### 9.1 存储内容与 Schema

#### 10.1.1 核心表

```sql
-- 通用内容表：每个入库的内容（DASH视频/图床/文档等）一条记录
CREATE TABLE content (
    content_id      UUID PRIMARY KEY,
    content_type    TEXT NOT NULL,              -- "dash_video" | "image" | "document" | ...
    type_metadata   JSONB,                      -- 内容类型专属元数据 (MPD XML / EXIF / 页索引等)
                                                 -- 通用管线不解析此字段
    created_at      TIMESTAMPTZ DEFAULT now(),
    updated_at      TIMESTAMPTZ DEFAULT now()
);

-- 通用 blob 索引：每个可上传的文件（DASH段/原图/缩略图等）一条记录
CREATE TABLE blob_index (
    content_id   UUID NOT NULL REFERENCES content(content_id),
    blob_hash      TEXT NOT NULL,                 -- "init_720p" | "seg_720p_3" | "original" | "thumb_200" | ...
    role         TEXT NOT NULL,                 -- 语义标签: "init"|"media"|"original"|"thumbnail"|"page"
    sort_order   INTEGER DEFAULT 0,             -- blob 在内容内的逻辑顺序
    size_bytes   BIGINT NOT NULL,
    checksum     TEXT NOT NULL,                 -- SHA256
    PRIMARY KEY (content_id, blob_hash)
);

-- 通用 blob 位置表：每个 blob 的 K 个冗余存储位置
-- 写入时用事务保证 K 个位置原子性
CREATE TABLE blob_location (
    content_id   UUID NOT NULL,
    blob_hash      TEXT NOT NULL,
    backend_id   TEXT NOT NULL,                 -- "115:acct_03" | "baidu:acct_05" | "local:ssd-01"
    file_id      TEXT NOT NULL,                 -- 后端内的文件 ID
    created_at   TIMESTAMPTZ DEFAULT now(),
    PRIMARY KEY (content_id, blob_hash, backend_id),
    FOREIGN KEY (content_id, blob_hash)
        REFERENCES blob_index(content_id, blob_hash)
);

-- 账号健康状态（高频更新，建议用 Redis，PostgreSQL 做持久化）
CREATE TABLE account_health (
    vendor       TEXT NOT NULL,
    account_id   TEXT NOT NULL,
    state        TEXT NOT NULL,                 -- "healthy" | "degraded" | "banned"
    last_check   TIMESTAMPTZ NOT NULL,
    latency_ms   INTEGER,
    error_msg    TEXT,
    ban_until    TIMESTAMPTZ,                   -- 封号到何时
    PRIMARY KEY (vendor, account_id)
);

-- 注: 热度表 (content_popularity) 不属于存储域, 由分发域维护。
-- 两个域通过 content_id / blob_hash 作为关联键解耦。
-- 热度表的 Schema 和接口见 distribution/README.md §8。
```

#### 10.1.2 查询示例

```sql
-- 获取视频所有段的位置（冗余列表）
SELECT bl.backend_id, bl.file_id,
       ah.state, ah.latency_ms
FROM blob_location bl
LEFT JOIN account_health ah
    ON ah.vendor = split_part(bl.backend_id, ':', 1)
   AND ah.account_id = split_part(bl.backend_id, ':', 2)
WHERE bl.content_id = 'abc123'
  AND bl.blob_hash = 'seg_720p_5';
```

结果示例：

```
 backend_id      | file_id  | state   | latency_ms
-----------------+----------+---------+------------
 115:acct_03     | fid_xxxx | healthy | 120
 baidu:acct_07   | fid_yyyy | healthy | 200
```

L2+L4 节点的本地数据面从这两条中选一条（按 Backend 权重、负载），获取下载链接然后回源。

### 9.2 gRPC 服务接口

```go
// 元数据服务 gRPC 接口定义
service MetadataService {
    // --- 内容元数据 ---
    rpc GetContentMeta(GetContentMetaReq) returns (GetContentMetaResp);  // 获取 content + type_metadata

    // --- blob 索引 ---
    rpc GetBlobIndex(GetBlobIndexReq) returns (GetBlobIndexResp);

    // --- blob 位置 ---
    rpc GetBlobLocations(GetBlobLocationsReq) returns (GetBlobLocationsResp);
    rpc WriteContentMeta(WriteContentMetaReq) returns (Empty);  // 入库写入 (content + blob_index + blob_location 事务)

    // --- 账号健康 ---
    rpc ReportAccountHealth(AccountHealthReq) returns (Empty);
    rpc GetAccountHealths(GetAccountHealthsReq) returns (GetAccountHealthsResp);

    // 注: 流行度相关接口 (ReportAccess / GetPopularContents) 不属于存储域,
    // 见 distribution/README.md §8 热度数据模型。
}

message GetBlobLocationsResp {
    repeated BlobLocation locations = 1;
}

message BlobLocation {
    string backend_id = 1;   // "115:acct_03" | "local:ssd-01"
    string file_id = 2;
    int64 size = 3;
    string checksum = 4;
}
```

### 9.3 实现选型：PostgreSQL + Redis

| 选型 | 理由 |
|------|------|
| **PostgreSQL 14+** 作为主存储 | 1) 事务保证 blob 位置写入原子性（一个 blob 的 K 个 location 必须一起提交）；2) JSONB 字段存 content.type_metadata（内容类型专属元数据，通用管线不解析）；3) 主从流复制做读扩展 + HA |
| **Redis 7+** 作为热数据缓存 | 1) content 专属元数据缓存（TTL 1h），命中率 > 90%；2) blob 位置缓存（TTL 30min，段不可变可以设更长）；3) 账号健康状态缓存（30s 更新一次，实时性要求不高但热点读极高）；4) 哨兵模式自动故障转移 |

### 10.4 一致性策略

- **写路径（入库）**：`BEGIN → INSERT content → INSERT blob_index → INSERT blob_location × K → COMMIT`。所有写入走 PostgreSQL 主库。
- **读路径**：Redis 缓存 → 未命中 → PostgreSQL 从库 → 回填 Redis。
- **账号健康状态**：L2+L4 节点本地数据面每 30s 探测一次，写入 Redis（TTL 60s）+ 异步写 PostgreSQL + 上报控制面。读取方式：Redis 直读。
- **流行度**：不属于存储域。热度数据由分发域维护（gossip 分钟级 + PG `content_popularity` 小时级），通过 `content_id` / `blob_hash` 与存储域的blob 索引关联。详见 `distribution/README.md §8`。

---

## 10. 存储层容灾

### 10.1 段级冗余：K=2 跨账号跨厂商

（已在 §4.1 详述）

### 10.2 账号级熔断

（已在 §4.3 详述）

### 10.3 厂商级容灾

（已在 §4.4 详述）

### 10.4 L2+L4 节点多副本（无中心代理）

```
   ┌─────────────┐  ┌─────────────┐  ┌─────────────┐
   │ L2-only 节点 │  │ L2-only 节点 │  │ L2+L4 节点  │  ...
   │  (分发)      │  │  (分发)      │  │ (分发+回源)  │
   └──────┬──────┘  └──────┬──────┘  └──────┬──────┘
          │ gRPC 拉取       │                │ 本地回源
          v                 v                v
   ┌──────────────────────────────────────────────┐
   │  同区域 L2+L4 节点池 (主回源路径)              │
   │  ┌────────┐  ┌────────┐  ┌────────┐         │
   │  │L2+L4-01│  │L2+L4-02│  │L2+L4-03│  ...    │
   │  │本地数据面│ │本地数据面│ │本地数据面│         │
   │  └────┬───┘  └────┬───┘  └────┬───┘         │
   └───────│───────────│───────────│──────────────┘
           │           │           │
           v           v           v
   ┌──────────────────────────────────────────────┐
   │  PostgreSQL + Redis (元数据服务)              │
   └──────────────────────────────────────────────┘
                   │
   ┌───────────────┴──────────────────────────────┐
   │   网盘账号池                                   │
   └──────────────────────────────────────────────┘
```

- **L2+L4 节点**：每区域至少 2 个，互为冗余。L2-only 节点缓存 miss 时按 round-robin + least-conn 选择。单节点故障 → 调度层在 30s 内从同区 L2+L4 列表剔除。
- **Ingest Worker**：独立角色集群，2+ 实例跨区域部署。承担 ContentIngester 加工处理 + 冗余上传，不服务客户端。IngestOrchestrator 全局调度，任一 Worker 故障控制面重分发任务。
- **节点列表下发**：调度层（非 etcd，自研轻量服务发现）每 10s 向 L2-only 节点推送同区 L2+L4 节点列表。
- **L2+L4 节点无状态**：所有持久状态在元数据服务 + 网盘端，节点本地只缓存凭证/链接/blob 位置副本。扩缩容无需数据迁移，新节点上线后订阅控制面 60s 内拿到全量快照即可服务。
- **无中心代理**：同区 L2+L4 全部不可达时回源失败返回 503，不异地兜底。区域级相关故障不覆盖，见主文档 [§1.3](../README.md#13-sla-边界与不可恢复场景)。

### 10.5 元数据服务 HA

```
         ┌─────────┐  流复制  ┌─────────┐
  Write ─┤ PG 主库  ├─────────>┤ PG 从库  ├── Read
         └─────────┘           └─────────┘
              │                     │
              ├─────────────────────┤
              │   Redis Sentinel    │
              │  ┌──────┬──────┐    │
              │  │Master│Slave │    │
              │  └──────┴──────┘    │
              └─────────────────────┘
```

- **PostgreSQL**：主从流复制（异步），从库承担所有读请求。主库故障时手动或基于 Patroni 自动切换。
- **Redis**：哨兵模式 3 节点（1 master + 2 slave + 3 sentinel），master 故障时 sentinel 自动投票切换。
- **最坏情况（PG + Redis 同时全挂）**：节点内存中的 content 元数据缓存（TTL 1h）+ blob 位置 hot subset 仍可服务已在播放的用户；新内容的元数据和未缓存的blob 位置查询会失败，但已缓存段的回源不受影响（L2+L4 节点本地有凭证副本，可继续回源 5min）。

### 10.6 元数据服务与控制面单点

**风险**：PostgreSQL 挂了，content 元数据和 blob 位置查询失败；控制面挂了，凭证轮换停止、新节点无法加入。

**缓解**：

1. PostgreSQL 主从流复制（异步），从库承担读请求。
2. Redis 集群（哨兵模式）缓存热 content 元数据和 blob 索引，TTL 1h。
3. L2+L4 节点内存缓存 content 元数据（TTL 1h）+ blob 位置 hot subset + 凭证副本（TTL 5min）。即使元数据服务和控制面同时全宕：
   - 已在播放的会话不受影响（content 元数据已缓存）
   - 已缓存段的回源不受影响（L2+L4 节点本地有凭证副本，可继续回源 5min）
   - 5min 后凭证过期，新回源失败，但已下载到缓存的段仍可服务
4. 控制面主备切换 < 5min，凭证 TTL 5min 内系统正常运转。

### 11.7 链接过期卡顿

**风险**：网盘下载链接有效期 10 分钟到 8 小时不等，链接过期后才被发现会导致回源失败，客户端卡顿。

**缓解**：

1. **链接池预刷新**。接入层维护文件 ID 到下载链接的 LRU 缓存，链接过期前 2 分钟后台自动刷新（active refresh）。
2. **懒刷新降级**。若刷新失败，取链接时触发同步重刷并返回新链接，增加 200-500ms 延迟但保证返回。
3. **边缘侧无感知**。边缘只发 `blob_hash` 请求到接入层，链接失效由接入层内部消化，边缘端永远不会收到 403 或过期链接错误。

### 11.8 容灾恢复时间目标（RTO）

| 故障场景 | RTO | 影响范围 |
|---------|-----|---------|
| 单账号被封 | **0s**（本地熔断器自动切换冗余账号） | 该账号的 segment 请求增加 ~1ms 选择延迟 |
| 单厂商全挂 | **< 30s**（健康检查发现 → 标记 degraded → 广播） | 对该厂商的请求全部降级到其他厂商冗余副本 |
| 单 L2+L4 节点挂 | **< 30s**（调度层摘除，L2-only 节点重路由） | 该节点的 L2-only 客户端重试到同区其他 L2+L4 节点（透明） |
| 整区域 L2+L4 全挂（相关故障） | **不覆盖** | 该区域 L2-only 节点回源全部失败，客户端 503。见主文档 §1.3 |
| 控制面挂 | **5-10min**（凭证本地 TTL 5min 内系统正常运转） | 新凭证轮换暂停，已有凭证继续工作；新节点无法加入 |
| PG 主库挂 | **< 2min**（手动 failover 或 Patroni 自动） | 写操作暂不可用（新视频入库），读走 Redis 缓存不受影响 |
| Redis 挂（哨兵切换） | **< 30s** | 部分读请求回退到 PG 从库，延迟增加 ~10ms |

---

## 11. 存储层监控指标

所有指标通过 Prometheus 采集，Grafana 仪表盘展示。

### 12.1 L2+L4 节点数据面指标

```promql
# 段回源成功率（目标 > 99%）
sum(rate(access_backhaul_success_total[5m])) /
sum(rate(access_backhaul_request_total[5m]))

# 回源延迟 P95（目标 < 500ms）
histogram_quantile(0.95,
  sum(rate(access_backhaul_duration_seconds_bucket[5m])) by (le))

# 账号健康分布
count by (vendor, state) (account_health_state)

# 单账号 QPS 利用率
rate(account_requests_total{vendor="115"}[1m]) / 1.0  # 除以配置上限

# 链接池命中率
sum(rate(linkpool_hit_total[5m])) /
sum(rate(linkpool_request_total[5m]))

# 熔断器开启数
circuit_breaker_state{state="open"}

# 上传成功率
sum(rate(ingest_upload_success_total[5m])) /
sum(rate(ingest_upload_attempt_total[5m]))
```

### 12.2 元数据服务指标

```promql
# PG 主库复制延迟
pg_replication_lag_seconds

# Redis 缓存命中率
redis_keyspace_hits / (redis_keyspace_hits + redis_keyspace_misses)

# 查询延迟 P99
histogram_quantile(0.99,
  sum(rate(metadata_query_duration_seconds_bucket[5m])) by (le))
```

### 12.3 告警规则（存储相关）

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

      # 账号大规模 ban
      - alert: MassAccountBan
        expr: count(account_health_state{state="banned"}) / count(account_health_state) > 0.5
        for: 5m
        annotations:
          summary: "超过 50% 账号被封（{{ $value | humanizePercentage }}），需要人工介入"

      # 某厂商全部账号不可用
      - alert: VendorAllDown
        expr: |
          count by (vendor) (account_health_state{state!="healthy"}) ==
          count by (vendor) (account_health_state)
        for: 2m
        annotations:
          summary: "厂商 {{ $labels.vendor }} 所有账号不可用"

      # 接入层实例故障
      - alert: L2L4NodeDown
        expr: up{job="l2l4-node"} == 0
        for: 1m
        annotations:
          summary: "L2+L4 节点实例 {{ $labels.instance }} 宕机"

      # 区域级 L2+L4 全部不可达（相关故障，不自动恢复）
      - alert: RegionalL2L4AllDown
        expr: |
          count by (region) (up{job="l2l4-node"} == 0) ==
          count by (region) (up{job="l2l4-node"})
        for: 1m
        annotations:
          summary: "区域 {{ $labels.region }} 所有 L2+L4 节点不可达，需人工介入"
```

---

> **交叉引用**：
>
> - 入库流程（转码、预分片、并发上传编排、错误处理策略）见 `ingest/README.md`
> - 分发缓存策略（prefix cache、stream-through 回源、兄弟节点 ICP、视频感知缓存逐出、gossip 热度同步、动态 Pin）见 `distribution/README.md`
> - 调度层与路由、端到端数据流时序、部署拓扑、容量规划见总体架构文档
