# 链路策略控制 — 领域文档

> **本文档为目标态设计，当前零代码实现**。PolicyController / Backend 抽象 / 用量采集 / 回源权重（ReadWeight）+ 上传权重（UploadWeight）下发链路**均未落地**。当前回源账号选择走 `AccountPool.SelectForRead` 的静态 `VendorProfile` 权重（见 [`storage/README.md §4`](../storage/README.md)）；上传选择走 `SelectKForUpload`（健康 + 跨厂商 + 负载最低），未接入 PolicyController 的动态权重。本文档作为目标态设计稿保留，待决策重新激活时再与代码对齐。详见主文档 [`README.md §9.1`](../README.md#9-目标态-vs-实现态target-state-vs-implemented-state) 的"目标态 vs 实现态"逐域偏差表。

> 本文档覆盖**账号链路策略控制**：统一抽象网盘账号和本地持久存储为 Backend，根据用量指标动态计算回源权重和上传权重，通过 SyncBroadcaster 下发到 L2+L4 节点。
>
> 这是一个**独立领域**，横跨读路径（影响分发域的 SelectForRead）和写路径（影响 ingest 域的账号选择）。不属于 storage 域（storage 只管段文件存储和元数据记录），也不属于 distribution 域（distribution 只管缓存和分发）。
>
> 交叉引用：
> - 分发域回源选择（读取 Backend.Weight()）：[`distribution/README.md`](../distribution/README.md)
> - 入库域上传账号选择（读取 Backend.Writable() + 上传权重）：[`ingest/README.md`](../ingest/README.md)
> - 存储域 Driver 接口（Backend 内部委托）：[`storage/README.md`](../storage/README.md)

---

## 1. Backend 接口抽象与链路策略控制

### 1.1 Backend 接口

将"网盘账号"和"本地持久存储"统一抽象为 **Backend**——一个可回源的数据存储后端。

```go
// Backend 是所有可回源存储后端的统一抽象。
// 实现者: CloudDriveBackend (网盘账号) / LocalStorageBackend (本地持久存储)
type Backend interface {
    // 后端标识
    ID() string        // 全局唯一, 如 "115:acct_03" / "local:ssd-node-01"
    Type() BackendType // BACKEND_CLOUD_DRIVE | BACKEND_LOCAL_STORAGE

    // 文件操作 (复用 Driver 接口能力, 网盘后端委托给 Driver)
    List(ctx context.Context, dirID string) ([]FileInfo, error)
    GetLink(ctx context.Context, fileID string) (*DownloadLink, error)
    Put(ctx context.Context, dirID string, name string, reader io.Reader, size int64) (*FileInfo, error)
    Remove(ctx context.Context, fileID string) error

    // ★ 用量指标采集 (数据面本地采集, 见 3A.4)
    UsageMetrics() UsageMetrics

    // ★ 当前回源权重 (由 PolicyController 下发, 节点本地缓存, 见 3A.5)
    Weight() float64

    // ★ 是否可写入 (容量/策略判断)
    Writable() bool
}

type BackendType int
const (
    BACKEND_CLOUD_DRIVE  BackendType = iota // 网盘账号
    BACKEND_LOCAL_STORAGE                   // 本地持久存储
)

// UsageMetrics: 后端使用情况快照 (数据面本地采集)
type UsageMetrics struct {
    // 通用指标 (网盘 + 本地存储都有)
    StorageUsedBytes  int64         // 已用存储量
    StorageLimitBytes int64         // 存储上限 (网盘: 厂商配额; 本地: 磁盘容量)
    ReadBytesToday    int64         // 今日回源流量 (字节)
    ReadBytesWindow   int64         // 当前窗口回源流量 (滑动窗口, 用于限速)

    // 网盘专属指标 (本地存储为零值或忽略)
    RequestCountToday int64         // 今日 API 请求次数 (List + GetLink + Put 等)
    RequestCountWindow int64        // 当前窗口请求次数 (滑动窗口)
    LastBanRisk       float64       // 封号风险评分 (0.0-1.0, 由健康检查推断)

    // 本地存储专属指标 (网盘为零值或忽略)
    DiskIOPSUtilization float64     // 磁盘 IOPS 利用率 (0.0-1.0)
    DiskBandwidthMbps   float64     // 当前磁盘读写带宽

    Timestamp time.Time
}
```

### 1.2 两种 Backend 实现

```go
// ─── 网盘后端 (CloudDriveBackend) ───
type CloudDriveBackend struct {
    Driver      Driver          // 复用现有 Driver 接口 (§3)
    Account     *Account        // 账号信息 (凭证/限流/熔断)
    VendorProfile VendorProfile // 厂商能力画像 (静态: 延迟/带宽)

    // 用量采集 (数据面本地, 原子操作)
    metrics     atomic.Value    // UsageMetrics
    reqCounter  atomic.Int64    // 请求次数累加器
    readBytes   atomic.Int64    // 回源字节累加器
}

func (b *CloudDriveBackend) UsageMetrics() UsageMetrics {
    return b.metrics.Load().(UsageMetrics)
}

func (b *CloudDriveBackend) Weight() float64 {
    // 基础权重 = VendorProfile.Weight (静态能力画像)
    // 实际权重 = 基础权重 × PolicyController 下发的动态系数
    // 动态系数由控制面根据 UsageMetrics 计算 (见 3A.5)
    return b.dynamicWeight.Load() // 由 SyncBroadcaster 下发
}

func (b *CloudDriveBackend) Writable() bool {
    m := b.UsageMetrics()
    if m.StorageLimitBytes > 0 && m.StorageUsedBytes >= m.StorageLimitBytes*9/10 {
        return false // 容量超 90% 不可写
    }
    return b.Account.CircuitBreaker.State() != StateOpen
}

// 每次 API 调用时累加用量 (在 Driver 包装层)
func (b *CloudDriveBackend) recordRequest(bytesRead int64) {
    b.reqCounter.Add(1)
    b.readBytes.Add(bytesRead)
}

// ─── 本地持久存储后端 (LocalStorageBackend) ───
type LocalStorageBackend struct {
    RootPath    string          // 本地存储根目录
    DiskDevice  string          // 磁盘设备标识
    metrics     atomic.Value    // UsageMetrics
    dynamicWeight atomic.Value  // float64, 由控制面下发
}

func (b *LocalStorageBackend) UsageMetrics() UsageMetrics {
    return b.metrics.Load().(UsageMetrics)
}

func (b *LocalStorageBackend) Weight() float64 {
    return b.dynamicWeight.Load().(float64)
}

func (b *LocalStorageBackend) Writable() bool {
    m := b.UsageMetrics()
    if m.StorageLimitBytes > 0 && m.StorageUsedBytes >= m.StorageLimitBytes*9/10 {
        return false
    }
    return m.DiskIOPSUtilization < 0.9 // IOPS 利用率 < 90% 可写
}

// List/GetLink/Put/Remove 委托给本地文件系统操作
// GetLink 返回本地文件路径 (FilePath), 不是 URL
func (b *LocalStorageBackend) GetLink(ctx context.Context, fileID string) (*DownloadLink, error) {
    path := filepath.Join(b.RootPath, fileID)
    return &DownloadLink{FilePath: &path}, nil
}
```

**两种 Backend 的用量维度对比**：

| 指标 | CloudDriveBackend | LocalStorageBackend |
|------|-------------------|---------------------|
| 存储量 | ✅ 网盘配额限制 | ✅ 磁盘容量限制 |
| 回源流量 | ✅ 字节累计 | ✅ 字节累计 |
| API 请求次数 | ✅ **关键指标** (风控触发) | ❌ 不适用 (无 API 概念) |
| 封号风险 | ✅ **关键指标** | ❌ 不适用 |
| 磁盘 IOPS | ❌ 不适用 | ✅ **关键指标** |
| 磁盘带宽 | ❌ 不适用 | ✅ 参考指标 |

### 1.3 用量采集（数据面本地）

每个 L2+L4 节点的数据面在每次 Backend 操作时本地累加用量，定期上报控制面。

```go
// 数据面侧: Backend 操作包装器 (在 Driver 调用前后注入)
type InstrumentedBackend struct {
    inner Backend
    reqCounter  *atomic.Int64
    readBytes   *atomic.Int64
}

func (ib *InstrumentedBackend) GetLink(ctx context.Context, fileID string) (*DownloadLink, error) {
    ib.reqCounter.Add(1)  // 网盘: 计数; 本地: 不计 (LocalBackend 不走此包装)
    link, err := ib.inner.GetLink(ctx, fileID)
    return link, err
}

// 回源下载时统计流量
func (ib *InstrumentedBackend) ReadAndCount(link *DownloadLink) (io.ReadCloser, error) {
    reader, err := openReader(link)
    if err != nil { return nil, err }
    return &countingReader{inner: reader, counter: ib.readBytes}, nil
}

// 每 30s 快照用量, 上报控制面 (复用 NodeReport 通道)
func (n *L2L4Node) reportBackendUsage(ctx context.Context) {
    ticker := time.NewTicker(30 * time.Second)
    defer ticker.Stop()
    for {
        select {
        case <-ctx.Done(): return
        case <-ticker.C:
            var usage []BackendUsageReport
            for _, backend := range n.backends {
                m := backend.UsageMetrics()
                usage = append(usage, BackendUsageReport{
                    BackendID:  backend.ID(),
                    Metrics:    m,
                })
            }
            n.controlPlane.ReportBackendUsage(ctx, n.id, usage)
        }
    }
}
```

**存储量采集**：
- 网盘：定期（每 5min）通过 Driver.List 遍历根目录统计，或调厂商 API 查配额
- 本地：每 30s 执行 `statfs` 获取磁盘使用率

### 1.4 PolicyController（控制面子模块）

控制面新增 **PolicyController**，接收各 L2+L4 节点上报的 Backend 用量，计算动态回源权重，通过 SyncBroadcaster 下发。

```go
// 控制面侧
type PolicyController struct {
    mu         sync.RWMutex
    backendUsage map[string]*AggregatedUsage  // key = backend_id
    broadcaster *SyncBroadcaster
}

type AggregatedUsage struct {
    BackendID    string
    BackendType  BackendType
    // 聚合所有 L2+L4 节点的上报 (求和/取最大值)
    TotalRequestCount  int64     // 网盘: 所有节点的请求次数总和
    TotalReadBytes     int64     // 所有节点的回源流量总和
    MaxStorageUsed     int64     // 存储用量 (各节点应一致, 取最大值)
    StorageLimit       int64
    BanRisk            float64   // 封号风险 (取最大值)
    DiskIOPS           float64   // 本地: IOPS 利用率
    LastUpdate         time.Time
}

// 每 10s 重新计算权重
func (pc *PolicyController) Run(ctx context.Context) {
    ticker := time.NewTicker(10 * time.Second)
    defer ticker.Stop()
    for {
        select {
        case <-ctx.Done(): return
        case <-ticker.C:
            pc.recomputeWeights(ctx)
        }
    }
}

func (pc *PolicyController) recomputeWeights(ctx context.Context) {
    pc.mu.RLock()
    defer pc.mu.RUnlock()

    weights := make(map[string]float64)  // backend_id → weight

    for backendID, usage := range pc.backendUsage {
        weight := pc.calculateWeight(usage)
        weights[backendID] = weight
    }

    // 通过 SyncBroadcaster 增量下发权重
    pc.broadcaster.Publish(BACKEND_WEIGHT_UPDATE, "global", weights)
}

// 权重计算: 根据后端类型采用不同策略
func (pc *PolicyController) calculateWeight(usage *AggregatedUsage) float64 {
    if usage.BackendType == BACKEND_LOCAL_STORAGE {
        return pc.calculateLocalWeight(usage)
    }
    return pc.calculateCloudDriveWeight(usage)
}

// 网盘权重: 综合请求次数 + 流量 + 封号风险 + 存储使用率
func (pc *PolicyController) calculateCloudDriveWeight(usage *AggregatedUsage) float64 {
    baseWeight := pc.getVendorBaseWeight(usage.BackendID)  // VendorProfile.Weight

    // 因子 1: 请求次数衰减 (网盘 API 限流敏感)
    // 日请求 < 5000: 不衰减; 5000-10000: 线性衰减; > 10000: 权重归零
    reqFactor := 1.0
    if usage.TotalRequestCount > 10000 {
        reqFactor = 0.0  // 风控高危, 停止使用
    } else if usage.TotalRequestCount > 5000 {
        reqFactor = 1.0 - float64(usage.TotalRequestCount-5000)/5000.0
    }

    // 因子 2: 回源流量衰减
    // 日流量 < 50GB: 不衰减; 50-100GB: 线性衰减; > 100GB: 权重归零
    trafficFactor := 1.0
    readGB := usage.TotalReadBytes / (1024 * 1024 * 1024)
    if readGB > 100 {
        trafficFactor = 0.0
    } else if readGB > 50 {
        trafficFactor = 1.0 - float64(readGB-50)/50.0
    }

    // 因子 3: 封号风险衰减
    // risk < 0.3: 不衰减; 0.3-0.7: 线性衰减; > 0.7: 权重归零
    banFactor := 1.0
    if usage.BanRisk > 0.7 {
        banFactor = 0.0
    } else if usage.BanRisk > 0.3 {
        banFactor = 1.0 - (usage.BanRisk-0.3)/0.4
    }

    // 因子 4: 存储使用率衰减 (影响写入, 对读取也轻微影响)
    // 使用率 < 80%: 不衰减; 80-95%: 线性衰减; > 95%: 不可写
    storageRatio := 0.0
    if usage.StorageLimit > 0 {
        storageRatio = float64(usage.MaxStorageUsed) / float64(usage.StorageLimit)
    }
    storageFactor := 1.0
    if storageRatio > 0.95 {
        storageFactor = 0.1  // 接近满, 大幅降权
    } else if storageRatio > 0.8 {
        storageFactor = 1.0 - (storageRatio-0.8)/0.15 * 0.5  // 最多降一半
    }

    // 最终权重 = 基础权重 × min(各因子) (最短板决定)
    return baseWeight * math.Min(reqFactor, math.Min(trafficFactor, math.Min(banFactor, storageFactor)))
}

// 本地存储权重: 不考虑请求次数和封号风险, 只看存储 + IOPS + 流量
func (pc *PolicyController) calculateLocalWeight(usage *AggregatedUsage) float64 {
    baseWeight := 5.0  // 本地存储基础权重高 (延迟低、无风控)

    // 因子 1: IOPS 利用率衰减
    // < 70%: 不衰减; 70-90%: 线性衰减; > 90%: 权重归零
    iopsFactor := 1.0
    if usage.DiskIOPS > 0.9 {
        iopsFactor = 0.0
    } else if usage.DiskIOPS > 0.7 {
        iopsFactor = 1.0 - (usage.DiskIOPS-0.7)/0.2
    }

    // 因子 2: 回源流量衰减 (与网盘相同逻辑)
    trafficFactor := 1.0
    readGB := usage.TotalReadBytes / (1024 * 1024 * 1024)
    if readGB > 200 {  // 本地存储流量上限更高
        trafficFactor = 0.0
    } else if readGB > 100 {
        trafficFactor = 1.0 - float64(readGB-100)/100.0
    }

    // 因子 3: 存储使用率衰减
    storageRatio := 0.0
    if usage.StorageLimit > 0 {
        storageRatio = float64(usage.MaxStorageUsed) / float64(usage.StorageLimit)
    }
    storageFactor := 1.0
    if storageRatio > 0.95 {
        storageFactor = 0.1
    } else if storageRatio > 0.8 {
        storageFactor = 1.0 - (storageRatio-0.8)/0.15 * 0.5
    }

    return baseWeight * math.Min(iopsFactor, math.Min(trafficFactor, storageFactor))
}
```

**权重下发链路**：

```
L2+L4 节点 A ──┐
L2+L4 节点 B ──┼──> 30s 上报 BackendUsage ──> 控制面 PolicyController
L2+L4 节点 C ──┘                                    │
                                                    │ 10s 重新计算权重
                                                    │ (按 BackendType 用不同策略)
                                                    v
                                            SyncBroadcaster 增量下发
                                            BACKEND_WEIGHT_UPDATE 事件
                                                    │
                                                    v
L2+L4 节点本地更新 Backend.dynamicWeight (TTL 30s)
                                                    │
                                                    v
SelectForRead 读取 Backend.Weight() 做选择 (见 3A.6)
```

### 1.5 SelectForRead 修订（使用 Backend 权重）

原 §5 SelectForRead 使用 `VendorProfile` 静态权重，现改为读取 `Backend.Weight()`（由 PolicyController 动态下发）。

```go
// 修订后的 SelectForRead (替代 §5 的 VendorProfile 版本)
func (ap *AccountPool) SelectForRead(ctx context.Context, blobHash string) (Backend, *DownloadLink, error) {
    locations, err := metadata.GetBlobLocations(ctx, blobHash)
    if err != nil { return nil, nil, err }

    // 候选: 从 blob 位置列表中找对应的 Backend
    var candidates []Backend
    for _, loc := range locations {
        backend := ap.backends[loc.BackendKey()]  // backend_id → Backend
        if backend == nil { continue }
        // 健康检查 + 熔断检查 (网盘后端才检查)
        if cd, ok := backend.(*CloudDriveBackend); ok {
            if cd.Account.CircuitBreaker.State() == StateOpen { continue }
            if cd.Account.Health.Load().(HealthState).State != "healthy" { continue }
            if !cd.Account.RateLimiter.Allow() { continue }
        }
        candidates = append(candidates, backend)
    }

    if len(candidates) == 0 {
        return nil, nil, fmt.Errorf("no available backend for blob %s", blobHash)
    }

    // ★ 按 Backend.Weight() / concurrent_load 加权选择
    // weight 高 = 优先选择; concurrent_load 高 = 降低优先级
    sort.Slice(candidates, func(i, j int) bool {
        si := float64(candidates[i].ConcurrentLoad()) / candidates[i].Weight()
        sj := float64(candidates[j].ConcurrentLoad()) / candidates[j].Weight()
        return si < sj  // score 最低 = 最优
    })

    backend := candidates[0]
    link, err := backend.GetLink(ctx, locations[0].FileID)
    return backend, link, err
}
```

**与原 VendorProfile 的关系**：
- `VendorProfile.Weight` 仍作为**基础权重**保留（网盘的静态能力画像）
- `Backend.Weight()` = `基础权重 × PolicyController 动态系数`（基于用量衰减）
- 本地存储后端没有 VendorProfile，基础权重在 PolicyController 中硬编码（5.0）
- SelectForRead 不再直接读 VendorProfile，而是读 Backend.Weight()

### 1.6 阈值配置

```yaml
# policy-controller.yaml
backends:
  cloud_drive:
    request_count:
      safe: 5000          # 日请求 < 5000 不衰减
      danger: 10000       # 日请求 > 10000 权重归零
    read_traffic_gb:
      safe: 50            # 日流量 < 50GB 不衰减
      danger: 100         # 日流量 > 100GB 权重归零
    ban_risk:
      safe: 0.3
      danger: 0.7
    storage_ratio:
      warn: 0.8           # 80% 开始衰减
      critical: 0.95      # 95% 大幅降权
    recompute_interval: 10s

  local_storage:
    base_weight: 5.0      # 本地存储基础权重 (高于网盘)
    iops_utilization:
      safe: 0.7
      danger: 0.9
    read_traffic_gb:
      safe: 100           # 本地存储流量上限更高
      danger: 200
    storage_ratio:
      warn: 0.8
      critical: 0.95
    recompute_interval: 10s

    # 注: 本地存储不配置 request_count 和 ban_risk (不适用)
```

### 1.7 与现有组件的关系

| 现有组件 | 与 Backend 抽象的关系 |
|---------|---------------------|
| Driver 接口 (storage §3) | CloudDriveBackend 内部委托给 Driver，Driver 不变 |
| AccountPool (storage §5) | AccountPool 扩展为 BackendPool，管理 Backend 列表（网盘+本地） |
| VendorProfile (storage §5) | 保留为 CloudDriveBackend 的静态基础权重来源 |
| CircuitBreaker (storage §7) | CloudDriveBackend 的 Writable() 检查熔断器状态 |
| HealthCheck (storage §8) | 网盘健康检查结果影响 BanRisk 指标 |
| SyncBroadcaster (storage §9) | 新增 BACKEND_WEIGHT_UPDATE 事件类型 |
| SelectForRead (storage §5) | 从读 VendorProfile 改为读 Backend.Weight()（回源权重） |
| Ingest 上传账号选择 (ingest §2) | 从 SelectK 改为读 Backend.Writable() + UploadWeight()（上传权重） |

---

## 2. 上传权重控制

链路策略控制不仅影响回源（读路径），也影响上传（写路径）。当某 Backend 存储用量过高、封号风险增大时，应降低其上传权重，甚至停止写入。

### 2.1 上传权重与回源权重的区别

| 维度 | 回源权重 (Read Weight) | 上传权重 (Upload Weight) |
|------|----------------------|------------------------|
| 消费者 | distribution 域的 SelectForRead | ingest 域的 SelectK（选择 K 个冗余账号上传） |
| 影响指标 | API 次数、回源流量、封号风险、存储量 | **存储量（首要）**、封号风险、写入频率 |
| 网盘关注点 | 流量和次数（风控触发） | 存储配额（容量耗尽） |
| 本地存储关注点 | IOPS、流量 | 存储容量、写入 IOPS |
| 极端行为 | 权重归零 → 不回源该 Backend | Writable()=false → 不上传该 Backend |

### 2.2 Backend 接口扩展

```go
type Backend interface {
    // ... 原有方法 ...

    // ★ 回源权重 (distribution 域 SelectForRead 使用)
    Weight() float64

    // ★ 上传权重 (ingest 域 SelectK 使用)
    UploadWeight() float64

    // ★ 是否可写入 (容量/策略判断)
    Writable() bool
}
```

### 2.3 上传权重计算

PolicyController 在计算回源权重的同时，并行计算上传权重。两者共享用量数据，但衰减因子不同。

```go
// 控制面侧: 上传权重计算 (与回源权重的 calculateCloudDriveWeight 并行)
func (pc *PolicyController) calculateUploadWeight(usage *AggregatedUsage) float64 {
    if usage.BackendType == BACKEND_LOCAL_STORAGE {
        return pc.calculateLocalUploadWeight(usage)
    }
    return pc.calculateCloudDriveUploadWeight(usage)
}

// 网盘上传权重: 以存储量为主, 封号风险为辅
func (pc *PolicyController) calculateCloudDriveUploadWeight(usage *AggregatedUsage) float64 {
    baseWeight := pc.getVendorBaseWeight(usage.BackendID)

    // 因子 1: 存储使用率 (上传首要限制)
    // < 70%: 不衰减; 70-90%: 线性衰减; > 90%: 权重归零 (比回源更严格)
    storageRatio := 0.0
    if usage.StorageLimit > 0 {
        storageRatio = float64(usage.MaxStorageUsed) / float64(usage.StorageLimit)
    }
    storageFactor := 1.0
    if storageRatio > 0.9 {
        storageFactor = 0.0  // 90% 即停止上传 (回源是 95% 才大幅降权)
    } else if storageRatio > 0.7 {
        storageFactor = 1.0 - (storageRatio-0.7)/0.2
    }

    // 因子 2: 封号风险 (上传也可能触发风控)
    banFactor := 1.0
    if usage.BanRisk > 0.7 {
        banFactor = 0.0
    } else if usage.BanRisk > 0.3 {
        banFactor = 1.0 - (usage.BanRisk-0.3)/0.4
    }

    // 注: 上传不关心回源流量和 API 请求次数 (上传走不同的 API 端点, 限流独立)
    return baseWeight * math.Min(storageFactor, banFactor)
}

// 本地存储上传权重: 以存储容量和写入 IOPS 为主
func (pc *PolicyController) calculateLocalUploadWeight(usage *AggregatedUsage) float64 {
    baseWeight := 5.0

    // 因子 1: 存储使用率 (同网盘逻辑, 90% 停止上传)
    storageRatio := 0.0
    if usage.StorageLimit > 0 {
        storageRatio = float64(usage.MaxStorageUsed) / float64(usage.StorageLimit)
    }
    storageFactor := 1.0
    if storageRatio > 0.9 {
        storageFactor = 0.0
    } else if storageRatio > 0.7 {
        storageFactor = 1.0 - (storageRatio-0.7)/0.2
    }

    // 因子 2: 写入 IOPS 利用率 (上传会产生写入 IO)
    // 复用 IOPS 指标, 但上传更敏感 (写入比读取更容易影响其他服务)
    iopsFactor := 1.0
    if usage.DiskIOPS > 0.85 {  // 上传阈值比回源(0.9)更严
        iopsFactor = 0.0
    } else if usage.DiskIOPS > 0.6 {
        iopsFactor = 1.0 - (usage.DiskIOPS-0.6)/0.25
    }

    return baseWeight * math.Min(storageFactor, iopsFactor)
}
```

### 2.4 上传权重下发

上传权重与回源权重通过同一个 `BACKEND_WEIGHT_UPDATE` 事件下发，但 payload 中分别包含 `read_weight` 和 `upload_weight`：

```go
// SyncBroadcaster 下发的权重事件
type BackendWeightUpdate struct {
    Weights map[string]BackendWeights  // key = backend_id
}

type BackendWeights struct {
    ReadWeight   float64  // SelectForRead 使用
    UploadWeight float64  // SelectK (ingest) 使用
    Writable     bool     // 容量/策略判断
}
```

### 2.5 Ingest 域使用上传权重

ingest 域的 `SelectK`（选择 K 个冗余账号上传）从原来的"健康 + 跨厂商 + 负载最低"改为"健康 + 跨厂商 + 上传权重最高"：

```go
// ingest 域: 修订后的 SelectK (替代原 AccountPool.SelectK)
func SelectKForUpload(backends []Backend, k int) ([]Backend, error) {
    // 1. 过滤: Writable + 未熔断
    var candidates []Backend
    for _, b := range backends {
        if !b.Writable() { continue }
        if cd, ok := b.(*CloudDriveBackend); ok {
            if cd.Account.CircuitBreaker.State() == StateOpen { continue }
        }
        candidates = append(candidates, b)
    }
    if len(candidates) < k {
        return nil, fmt.Errorf("insufficient writable backends: %d < %d", len(candidates), k)
    }

    // 2. 排序: 按 UploadWeight 降序 (权重高的优先上传)
    sort.Slice(candidates, func(i, j int) bool {
        return candidates[i].UploadWeight() > candidates[j].UploadWeight()
    })

    // 3. 跨厂商优先: 从 top-N 中选不同厂商
    selected := selectCrossVendor(candidates, k)
    return selected, nil
}
```

### 2.6 上传权重阈值配置

```yaml
# policy-controller.yaml (上传权重部分)
backends:
  cloud_drive:
    upload:
      storage_ratio:
        warn: 0.7           # 70% 开始衰减 (比回源 80% 更严)
        critical: 0.9       # 90% 停止上传 (比回源 95% 更严)
      ban_risk:
        safe: 0.3
        danger: 0.7
      # 注: 上传不关心 request_count 和 read_traffic

  local_storage:
    upload:
      storage_ratio:
        warn: 0.7
        critical: 0.9
      iops_utilization:
        safe: 0.6           # 上传 IOPS 阈值比回源 0.7 更严
        danger: 0.85        # 上传 IOPS 上限比回源 0.9 更严
```

