# 入库链路 — 领域文档

> 本文档覆盖**通用内容入库管线**：ContentIngester 接口 → 预处理 → 冗余上传 → 元数据事务 → 入库事件通知。不限于 DASH 视频，支持任意内容类型（图床、文档、音频等）扩展。
>
> 交叉引用：
> - 存储域（Backend 基础设施、元数据 Schema）：[storage/README.md](../storage/README.md)
> - 策略控制域（上传权重 SelectKForUpload）：[policy/README.md](../policy/README.md)
> - 分发域（prefix/pin 由分发域决定）：[distribution/README.md](../distribution/README.md)

## 目录

1. [ContentIngester 接口](#1-contentingester-接口)
2. [Ingest Worker 部署角色](#2-ingest-worker-部署角色)
3. [通用上传管线](#3-通用上传管线)
4. [入库原子性](#4-入库原子性)
5. [入库事件通知](#5-入库事件通知)
6. [DASH 视频实现示例](#6-dash-视频实现示例)
7. [内容类型扩展指南](#7-内容类型扩展指南)

---

## 1. ContentIngester 接口

不同内容类型（DASH 视频、图床、文档等）各自实现 `ContentIngester` 接口。入库管线调度器根据内容类型路由到对应实现。

```go
// ContentIngester 是内容类型专属的入库处理器。
// 每种内容类型（DASH视频/图床/文档等）实现一个。
type ContentIngester interface {
    // 内容类型标识
    ContentType() string  // "dash_video" | "image" | "document" | ...

    // 预处理：将原始输入转换为可上传的 blob 列表 + 专属元数据
    // 例如：DASH 视频转码分片；图片生成缩略图+EXIF提取
    Process(ctx context.Context, input io.Reader, opts ProcessOptions) (*ProcessResult, error)
}

// ProcessOptions: 入库时的可选参数
type ProcessOptions struct {
    // 通用
    ContentID   string            // 调用方指定（可选），不指定则自动生成
    Metadata    map[string]string // 调用方传入的元数据（标题、标签等）

    // DASH 视频专属（通过 metadata 传递，Process 内部解析）
    // "dash_bitrates": "500k,1500k,4000k"
    // "dash_seg_duration": "4"

    // 图床专属
    // "image_thumbnail_sizes": "200,800"
    // "image_strip_exif": "true"
}

// ProcessResult: 预处理产物
type ProcessResult struct {
    ContentID   string            // 内容唯一 ID（若未指定则 Process 内生成）
    ContentType string            // 与 ContentType() 一致

    // ★ 通用 blob 列表：预处理后产生的所有可上传文件
    // 每个 blob 是一个独立的文件（DASH段/原图/缩略图/PDF页等）
    Blobs []BlobDescriptor

    // ★ 专属元数据：内容类型专属的元数据（MPD/EXIF/页索引等）
    // 序列化为 JSON，存入 content.type_metadata JSONB 字段
    // 通用管线不解析此字段，由分发域的内容类型处理器消费
    TypeMetadata []byte
}

// BlobDescriptor: 单个可上传文件的描述
type BlobDescriptor struct {
    BlobHash    string  // blob 唯一标识（如 "seg_720p_3" / "thumb_200" / "original"）
    Filename  string  // 本地临时文件路径
    Size      int64
    Checksum  string  // SHA256
    SortOrder int     // blob 在内容内的逻辑顺序（可选，用于排序）

    // blob 类型分类（供分发域策略层做 pin/预取决策时参考）
    // DASH: "init" | "media"
    // 图床: "original" | "thumbnail"
    // 文档: "page_1" | "page_2" | ...
    BlobType string
}
```

---

## 2. Ingest Worker 部署角色

Ingest 域的执行体是 **Ingest Worker**——独立部署角色，承担 ContentIngester 加工处理 + 冗余上传任务。与 L2+L4 节点（分发+回源）和 L2-only 节点（纯分发）并列的第三种节点角色。

### 2.1 角色定位

| 维度 | Ingest Worker | L2+L4 节点 | L2-only 节点 |
|------|--------------|-----------|-------------|
| 客户端 HTTP 服务 | ❌ | ✅ | ✅ |
| 缓存（Prefix/Warm/Cold） | ❌ | ✅ | ✅ |
| Driver 实例 | ✅ 5 厂商各 1 | ✅ 5 厂商各 1 | ❌ |
| 网盘凭证 | ✅ 本地缓存 (TTL 5min) | ✅ 本地缓存 (TTL 5min) | ❌ |
| LinkPool / 限流器 / 熔断器 | ✅ 本地 | ✅ 本地 | ❌ |
| 网盘出口 IP | 本机 IP | 本机 IP | 不接触网盘 |
| FetchSegment gRPC 服务端 | ❌ | ✅ | ❌ |
| ContentIngester 加工处理 | ✅ 转码/分片/缩略图 | ❌ | ❌ |
| 冗余上传 (Put) | ✅ | ❌ (不再承担) | ❌ |
| blob 位置查询 | ❌ (只写不读) | ✅ | ❌ |
| 部署门槛 | 中 (需固定出口 IP / 凭证管理 / 转码算力) | 高 (需固定出口 IP / 大带宽) | 低 |
| 适合场景 | 专用上传/转码节点 | 自建核心节点 / 大区主节点 | 第三方 CDN / 社区节点 |

**设计要点**：
- **上传与回源物理隔离**：Ingest Worker 专门承担写流量（上传到网盘），不与 L2+L4 节点的读流量（回源从网盘）争用同一节点的带宽和网盘配额。
- **不服务客户端**：Ingest Worker 不暴露客户端 HTTP 端口，不参与分发。只接收控制面 IngestOrchestrator 下发的任务。
- **全局调度**：IngestOrchestrator 把任务分发给任意可达 Ingest Worker，不绑定区域。Worker 集群跨区域部署，任一节点故障控制面重分发任务。
- **订阅控制面**：与 L2+L4 节点一样，订阅控制面的凭证/健康更新，本地保留副本。但不持有 blob 位置索引（只写不读）。

### 2.2 配置示例

```yaml
# node-config.yaml -- Ingest Worker 示例
node:
  role: "INGEST_WORKER"     # 加工处理 + 冗余上传
  grpc_listen: ":9002"      # 接收控制面任务下发

access_layer:
  data_plane:
    enabled: true           # 启用 Driver + 凭证
    subscribe_control: true # 订阅控制面凭证下发
    drivers: ["115", "baidu", "quark", "onedrive", "aliyundrive"]
    link_pool: { max_entries: 5000 }
    rate_limit_local: true

ingest:
  enabled: true             # 启用 ContentIngester 执行
  work_dir: "/data/ingest"  # 转码/分片临时目录
  ffmpeg_path: "/usr/bin/ffmpeg"
  max_concurrent_tasks: 4   # 同时处理的入库任务数
```

### 2.3 角色职责划分

- **Ingest Worker**：加工处理 + 冗余上传（写网盘）
- **L2+L4 节点**：分发 + 回源（读网盘）
- **L2-only 节点**：分发缓存

上传与回源物理隔离，不争用同一节点的带宽和网盘配额。角色总数 3（L2+L4 / L2-only / Ingest Worker）。

---

## 3. 通用上传管线

### 3.1 IngestPipeline.Ingest()

入库管线调度器接收任意内容类型的输入，路由到对应 ContentIngester 处理后，执行通用的冗余上传 + 元数据事务。

```go
type IngestPipeline struct {
    ingesters  map[string]ContentIngester  // content_type → ingester
    backends   BackendPool                  // 见 storage/README.md
    metadata   MetadataClient               // 见 storage/README.md
    eventBus   EventBus                     // 发布 ContentIngestedEvent
}

func (p *IngestPipeline) Ingest(ctx context.Context, contentType string, input io.Reader, opts ProcessOptions) (string, error) {
    // 1. 路由到对应 ContentIngester
    ingester, ok := p.ingesters[contentType]
    if !ok {
        return "", fmt.Errorf("unsupported content type: %s", contentType)
    }

    // 2. 预处理（内容类型专属：转码/分片/缩略图等）
    result, err := ingester.Process(ctx, input, opts)
    if err != nil {
        return "", fmt.Errorf("process: %w", err)
    }

    // 3. 通用冗余上传：每个 blob 并发写入 K 个 Backend
    g, ctx := errgroup.WithContext(ctx)
    g.SetLimit(10)  // 最多 10 个 blob 同时上传

    blobLocations := make([][]BlobLocation, len(result.Blobs))
    for i, blob := range result.Blobs {
        i, blob := i, blob
        g.Go(func() error {
            locs, err := p.uploadBlobToK(ctx, blob, 2)  // K=2
            if err != nil {
                return err
            }
            blobLocations[i] = locs
            return nil
        })
    }
    if err := g.Wait(); err != nil {
        return "", fmt.Errorf("blob upload: %w", err)
    }

    // 4. 事务写入元数据（通用 blob 表 + 专属元数据）
    if err := p.metadata.WriteContentMeta(ctx, result, blobLocations); err != nil {
        return "", err
    }

    // 5. 发布入库事件（通知分发域，异步）
    go p.eventBus.Publish(ContentIngestedEvent{
        ContentID:   result.ContentID,
        ContentType: result.ContentType,
        Blobs:       result.Blobs,  // 含 BlobHash + BlobType
    })

    return result.ContentID, nil
}
```

### 3.2 uploadBlobToK（blob 级冗余写入）

每个 blob 并行写入 K 个 Backend（默认 K=2），优先跨厂商。账号选择使用策略控制域的 `SelectKForUpload`。

```go
func (p *IngestPipeline) uploadBlobToK(ctx context.Context, blob BlobDescriptor, k int) ([]BlobLocation, error) {
    // 选 K 个最佳 Backend（上传权重由 PolicyController 下发）
    backends, err := SelectKForUpload(p.backends.All(), k)
    if err != nil {
        return nil, fmt.Errorf("insufficient writable backends: %w", err)
    }

    g, ctx := errgroup.WithContext(ctx)
    locations := make([]BlobLocation, k)

    for i, backend := range backends {
        i, backend := i, backend
        g.Go(func() error {
            file, err := os.Open(blob.Filename)
            if err != nil { return err }
            defer file.Close()

            fi, err := backend.Put(ctx, blobUploadDir(blob), blob.BlobHash, file, blob.Size)
            if err != nil {
                return fmt.Errorf("upload to %s: %w", backend.ID(), err)
            }
            locations[i] = BlobLocation{
                BackendID: backend.ID(),
                FileID:    fi.ID,
            }
            return nil
        })
    }

    if err := g.Wait(); err != nil {
        // 部分成功：至少 1 个成功即可标记可用
        // 完全失败：返回错误
        if countSuccessful(locations) == 0 {
            return nil, err
        }
        // 记录 warn: 部分副本缺失，后台重试补齐
    }

    return filterSuccessful(locations), nil
}
```

### 3.3 错误处理策略

- 单个 blob 上传到 Backend A 失败但 Backend B 成功：记录 warn 日志，blob 有 1 个副本可用即可继续。缺失副本由后台补齐任务重试。
- K 个副本全部失败：整个入库失败，返回错误，已上传的孤儿文件由 GC 清理。
- 元数据写入失败：所有 blob 上传成功但 PG 事务失败，已上传文件也是孤儿，GC 清理。
- GC worker 每天扫描网盘上的孤儿文件（不在 blob_location 表中的文件），标记删除。

---

## 4. 入库原子性

### 4.1 PG 事务：通用 blob 元数据 + 专属元数据的原子写入

```
content "abc123" (dash_video)
  ├─ blob: init_720p     → [115:acct_03:fid_xxx, baidu:acct_05:fid_yyy]
  ├─ blob: seg_720p_1    → [115:acct_03:fid_aaa, baidu:acct_05:fid_bbb]
  ├─ blob: seg_720p_2    → [...]
  └─ blob: init_4k       → [...]
  + type_metadata: { "mpd_xml": "...", "duration_s": 3600, "representations": [...] }
```

元数据写入使用一条 PostgreSQL 事务：

```sql
BEGIN;
  -- 通用 content 表
  INSERT INTO content (content_id, content_type, type_metadata, created_at)
  VALUES ('abc123', 'dash_video', '{"mpd_xml":"...","duration_s":3600}', now());

  -- 通用 blob 索引
  INSERT INTO blob_index (content_id, blob_hash, role, sort_order, size_bytes, checksum)
  VALUES
    ('abc123', 'init_720p', 'init', 0, 1024, 'sha256:...'),
    ('abc123', 'seg_720p_1', 'media', 1, 2097152, 'sha256:...'),
    ...;

  -- 通用 blob 位置（K=2 冗余）
  INSERT INTO blob_location (content_id, blob_hash, backend_id, file_id)
  VALUES
    ('abc123', 'init_720p', '115:acct_03', 'fid_xxx'),
    ('abc123', 'init_720p', 'baidu:acct_05', 'fid_yyy'),
    ...;
COMMIT;
```

Schema 定义见 [storage/README.md §9](../storage/README.md)（元数据服务）。

### 4.2 事务失败与 GC 清理

- **事务提交**：所有 blob 的 K 个上传完成后，一条 PG 事务批量写入 content + blob_index + blob_location。
- **事务失败回滚**：PG 事务失败则所有元数据回滚。已上传的孤儿文件由 GC worker 清理。
- **写入成功**：事务提交后，内容及所有 blob 被视为"可用"，读取路径可立即查询 blob 位置。

---

## 5. 入库事件通知

### 5.1 ContentIngestedEvent

入库完成后，管线发布 `ContentIngestedEvent` 事件。ingest 域只传递 blob 列表（含 BlobType 分类），不给出任何 pin 建议。pin 决策完全由分发域的策略层根据全局信息（流行度、缓存空间、BlobType）自行决定。

```go
type ContentIngestedEvent struct {
    ContentID   string
    ContentType string              // "dash_video" | "image" | "document" | ...
    Blobs       []BlobDescriptor    // 含 BlobHash + BlobType + Size + Checksum
    Timestamp   time.Time
}
```

### 5.2 分发域的处理

分发域的 PinOrchestrator 监听 `ContentIngestedEvent`，根据内容类型、BlobType 和全局流行度决定 pin 策略：

| 内容类型 | BlobType 分类 | 分发域策略层决策（热门） | 分发域策略层决策（冷门） |
|---------|-------------|----------------------|----------------------|
| DASH 视频 | init / media | pin init + 前 5 段 media | pin 仅 init |
| 图床 | original / thumbnail | pin thumbnail + original | pin 仅最小 thumbnail |
| 文档 | page_1 / page_2 / ... | pin 前 3 页 | pin 仅 page_1 |

**职责边界**：
- ingest 域：负责加工处理 + 产出 blob + 标注 BlobType。不关心缓存策略、流行度、pin 决策
- 分发域策略层：根据 BlobType + 流行度 + 缓存空间全局信息，自行决定 pin 什么、pin 多少
- 新增内容类型时，ingest 实现自己的 ContentIngester 产出 blob + BlobType；分发域注册对应的 PinStrategy

### 5.3 事件传输

事件通过控制面的 SyncBroadcaster 传输（复用现有同步协议，新增 `CONTENT_INGESTED` 事件类型）。延迟 < 1s。

---

## 6. DASH 视频实现示例

### 6.1 DashIngester

```go
type DashIngester struct {
    ffmpegPath string
    workDir    string
}

func (d *DashIngester) ContentType() string { return "dash_video" }

func (d *DashIngester) Process(ctx context.Context, input io.Reader, opts ProcessOptions) (*ProcessResult, error) {
    contentID := opts.ContentID
    if contentID == "" {
        contentID = uuid.New().String()
    }

    // 1. 写入临时文件
    srcPath := filepath.Join(d.workDir, contentID+"_src.mp4")
    if err := writeToFile(input, srcPath); err != nil {
        return nil, err
    }
    defer os.Remove(srcPath)

    // 2. ffmpeg 转码为 DASH 分片
    outDir := filepath.Join(d.workDir, contentID)
    bitrates := opts.Metadata["dash_bitrates"]
    if bitrates == "" { bitrates = "500k,1500k,4000k" }
    segDuration := opts.Metadata["dash_seg_duration"]
    if segDuration == "" { segDuration = "4" }

    if err := d.runFFmpeg(ctx, srcPath, outDir, bitrates, segDuration); err != nil {
        return nil, fmt.Errorf("ffmpeg: %w", err)
    }

    // 3. 扫描输出目录，构建 blob 列表
    blobs, duration, err := d.scanDashOutput(outDir)
    if err != nil {
        return nil, err
    }

    // 4. 读取 MPD XML 作为专属元数据
    mpdXML, err := os.ReadFile(filepath.Join(outDir, "manifest.mpd"))
    if err != nil {
        return nil, err
    }

    typeMeta, _ := json.Marshal(map[string]interface{}{
        "mpd_xml":       string(mpdXML),
        "duration_s":    duration,
        "seg_duration":  atoi(segDuration),
        "representations": d.extractRepresentations(mpdXML),
    })

    // 注: ingest 不决定 pin 策略, 只产出 blob + BlobType
    // 分发域策略层根据 BlobType("init"/"media") + 流行度自行决定 pin 什么

    return &ProcessResult{
        ContentID:    contentID,
        ContentType:  d.ContentType(),
        Blobs:        blobs,
        TypeMetadata: typeMeta,
    }, nil
}
```

### 6.2 ffmpeg 命令

```bash
ffmpeg -i input.mp4 \
  -map 0:v -map 0:a \
  -c:v libx264 -b:v:0 500k -s:v:0 640x360 \
  -b:v:1 1500k -s:v:1 1280x720 \
  -b:v:2 4000k -s:v:2 1920x1080 \
  -c:a aac -b:a 128k \
  -f dash -seg_duration 4 -use_template 1 -use_timeline 1 \
  -init_seg_name 'init_$RepresentationID$.m4s' \
  -media_seg_name 'seg_$RepresentationID$_$Number$.m4s' \
  /tmp/dash_output/{contentID}/manifest.mpd
```

---

## 7. 内容类型扩展指南

### 7.1 实现新 ContentIngester

新增内容类型（如图床）只需实现 `ContentIngester` 接口并注册到 IngestPipeline：

```go
// 图床 ContentIngester 示例骨架
type ImageIngester struct {}

func (i *ImageIngester) ContentType() string { return "image" }

func (i *ImageIngester) Process(ctx context.Context, input io.Reader, opts ProcessOptions) (*ProcessResult, error) {
    contentID := opts.ContentID
    if contentID == "" { contentID = uuid.New().String() }

    // 1. 保存原图
    origPath := filepath.Join(workDir, contentID+"_orig")
    writeToFile(input, origPath)

    // 2. 生成缩略图（不同尺寸）
    thumbSizes := parseThumbSizes(opts.Metadata["image_thumbnail_sizes"])  // [200, 800]
    var blobs []BlobDescriptor
    blobs = append(blobs, BlobDescriptor{
        BlobHash: "original", Filename: origPath, BlobType: "original", SortOrder: 0,
        Size: fileSize(origPath), Checksum: sha256(origPath),
    })
    for _, size := range thumbSizes {
        thumbPath := generateThumbnail(origPath, size)
        blobs = append(blobs, BlobDescriptor{
            BlobHash: fmt.Sprintf("thumb_%d", size), Filename: thumbPath,
            BlobType: "thumbnail", SortOrder: size / 100,
            Size: fileSize(thumbPath), Checksum: sha256(thumbPath),
        })
    }

    // 3. 提取 EXIF（专属元数据）
    exif := extractEXIF(origPath)
    typeMeta, _ := json.Marshal(map[string]interface{}{
        "exif": exif,
        "thumbnail_sizes": thumbSizes,
    })

    // 注: ingest 不决定 pin 策略, 只产出 blob + BlobType
    // 分发域策略层根据 BlobType("original"/"thumbnail") + 流行度自行决定 pin 什么

    return &ProcessResult{
        ContentID: contentID, ContentType: "image",
        Blobs: blobs, TypeMetadata: typeMeta,
    }, nil
}

// 注册
pipeline.RegisterIngester(&ImageIngester{})
```

### 7.2 扩展检查清单

新增内容类型时需要完成：

| 步骤 | 在哪个域 | 内容 |
|------|---------|------|
| 1. 实现 ContentIngester | ingest | Process 方法：预处理 + blob 列表（含 BlobType） + 专属元数据 |
| 2. 注册到 IngestPipeline | ingest | `pipeline.RegisterIngester(&XxxIngester{})` |
| 3. 注册 pin 决策策略 | distribution | PinOrchestrator 增加该内容类型的 pin 决策逻辑 |
| 4. 注册回源元数据查询（如需） | distribution | 如果回源时需要查专属元数据（如 DASH 的 MPD），在元数据服务中注册查询路径 |

**不需要修改的部分**：
- 通用上传管线（uploadBlobToK）
- 元数据事务（content + blob_index + blob_location）
- Backend 抽象和权重计算
- SyncBroadcaster 事件传输
