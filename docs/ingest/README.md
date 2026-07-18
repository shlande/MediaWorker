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

ContentIngester 的产出遵循**层次分离原则**：
- `BlobDescriptor.BlobHash` = SHA-256(文件内容)，纯内容寻址，不携带任何业务语义
- `BlobDescriptor.BlobType` = 二进制产出类型（格式/生产方式），如 `mp4_init_segment` / `m4s_media_segment` / `jpeg_thumbnail` / `pdf_page_image`
- 业务编排信息（码率、段号、缩略图尺寸、页码等）放在 `BlobRole` 里，由元数据模块写入 `content_blob` 关联表，**不污染 blob 主表**

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
    // 每个 blob 是一个独立的二进制文件，按内容寻址
    Blobs []BlobDescriptor

    // ★ blob 在该 content 内的编排信息（业务语义层）
    // 由元数据模块写入 content_blob 关联表, 不影响 blob 主表
    Roles []BlobRole

    // ★ 专属元数据：内容类型专属的元数据（MPD/EXIF/页索引等）
    // 序列化为 JSON，存入 content.type_metadata JSONB 字段
    // 元数据模块解析此字段, storage 域不感知
    TypeMetadata []byte
}

// BlobDescriptor: 单个可上传二进制文件的描述 (内容寻址层)
// 对应 storage 域 blob 表
type BlobDescriptor struct {
    BlobHash  string  // = SHA-256(文件内容), 全局唯一, 跨 content 去重
    Filename  string  // 本地临时文件路径
    Size      int64
    BlobType  string  // 二进制产出类型 (格式/生产方式), 不带业务语义
                      //   DASH: "mp4_init_segment" | "m4s_media_segment"
                      //   图床: "jpeg_original" | "jpeg_thumbnail" | "png_original"
                      //   文档: "pdf_page_image"
}

// BlobRole: blob 在某 content 内的编排信息 (元数据层)
// 对应 storage 域 content_blob 关联表
type BlobRole struct {
    BlobHash      string  // 关联到 BlobDescriptor.BlobHash
    Role          string  // 该 blob 在此 content 内的语义角色
                          //   DASH: "init" | "media"
                          //   图床: "original" | "thumbnail"
                          //   文档: "page"
    SortOrder     int     // blob 在 content 内的逻辑顺序
                          //   DASH: 段序号; 文档: 页码; 图床: 缩略图尺寸升序
    BusinessMeta  map[string]any  // 该关联的业务专属元数据 (可选, 写入 content_blob.business_meta JSONB)
                                  //   DASH: {"representation_id":"720p","bitrate":1500000,"width":1280,"height":720}
                                  //   图床: {"width":200,"height":150}
                                  //   文档: {"page_number":3}
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

> **实现态偏差**：下方 `access_layer.data_plane.subscribe_control` / `drivers` / `rate_limit_local` 三个字段在 `review-remediation` T17 中已从 `EdgeConfig` 删除（`internal/config/config.go`），旧 YAML 仍可加载但只发 `slog.Warn("deprecated config key ignored")`。ingest-worker 实际的数据面装配走 `IngestStorageConfig`（`storage:` 树下的 `cloud_accounts` / `vendor_profiles` / `rate_limits`，由 `accountpool.BuildFromConfig` 消费），不在 `access_layer:` 树下。`FetchSegment gRPC 服务端` 字段（`access_layer.fetch_segment_server` / `fetch_segment_client`）同样在 T17 中删除——ingest-worker 不暴露 FetchSegment 服务，回源由分发域 L4 节点承担。详见主文档 [`README.md §9.1`](../README.md#9-目标态-vs-实现态target-state-vs-implemented-state) 的逐域偏差表。

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

入库事务**跨两个层次**写入（见 [`storage/README.md §9.2`](../storage/README.md) 的 gRPC 接口分层）：
- storage 域内容寻址层：`BlobStore.WriteBlob` + `BlobStore.WriteBlobLocations`（`blob` 主表 + `blob_location` 冗余位置表）
- 元数据模块编排层：`ContentMeta.WriteContentMeta`（`content` 主表 + `content_blob` 关联表，含 role/sort_order/business_meta）

两层写入在**同一条 PG 事务**内完成，保证原子性（详见 §4.1）。

```go
type IngestPipeline struct {
    ingesters   map[string]ContentIngester  // content_type → ingester
    backends    BackendPool                  // 见 storage/README.md §4
    blobStore   BlobStoreClient              // storage 域内容寻址层 gRPC 客户端 (见 storage/README.md §9.2)
    contentMeta ContentMetaClient            // 元数据模块编排层 gRPC 客户端 (见 storage/README.md §9.2)
    eventBus    EventBus                     // 发布 ContentIngestedEvent
}

func (p *IngestPipeline) Ingest(ctx context.Context, contentType string, input io.Reader, opts ProcessOptions) (string, error) {
    // 1. 路由到对应 ContentIngester
    ingester, ok := p.ingesters[contentType]
    if !ok {
        return "", fmt.Errorf("unsupported content type: %s", contentType)
    }

    // 2. 预处理（内容类型专属：转码/分片/缩略图等）
    //    Process 产出: Blobs (内容寻址, BlobHash=SHA-256) + Roles (编排信息) + TypeMetadata
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

    // 4. 跨层事务写入元数据 (PG 事务, 保证原子性)
    //    内部按层次调用 BlobStore + ContentMeta 两个 gRPC 服务, 全部在同一 PG 事务内提交
    if err := p.writeIngestTransaction(ctx, result, blobLocations); err != nil {
        return "", err
    }

    // 5. 发布入库事件（通知分发域，异步）
    //    事件含 blobs (内容寻址) + roles (编排) 两层信息, 分发域按需消费
    go p.eventBus.Publish(ContentIngestedEvent{
        ContentID:   result.ContentID,
        ContentType: result.ContentType,
        Blobs:       result.Blobs,  // 含 BlobHash(SHA-256) + BlobType + Size
        Roles:       result.Roles,  // 含 role + sort_order + business_meta
        Timestamp:   time.Now(),
    })

    return result.ContentID, nil
}

// writeIngestTransaction: 跨层 PG 事务, 写入 blob + blob_location + content + content_blob
// 实现策略: 由 BlobStore 服务开启 PG 事务 (BEGIN), 通过 gRPC metadata 传递事务句柄,
//           ContentMeta 服务复用同一事务句柄, 任一失败则 ROLLBACK, 全部成功则 COMMIT.
//           详见 §4.1 的事务边界与 GC 策略.
func (p *IngestPipeline) writeIngestTransaction(
    ctx context.Context,
    result *ProcessResult,
    blobLocations [][]BlobLocation,
) error {
    // 1. storage 域内容寻址层: 写入 blob 主表 (ON CONFLICT DO NOTHING 实现去重)
    //    blob_hash 已是 SHA-256, 相同二进制 (如多视频共享 init segment 模板) 自动复用同一行
    blobs := make([]BlobDescriptor, len(result.Blobs))
    for i, b := range result.Blobs {
        blobs[i] = BlobDescriptor{
            BlobHash: b.BlobHash,
            BlobType: b.BlobType,
            Size:     b.Size,
        }
    }
    if err := p.blobStore.WriteBlob(ctx, WriteBlobReq{Blobs: blobs}); err != nil {
        return fmt.Errorf("blobstore.WriteBlob: %w", err)
    }

    // 2. storage 域内容寻址层: 写入 blob_location (K 个冗余位置)
    //    跨 content 共享: 同一 blob 已有位置则跳过, 仅追加缺失副本
    locReqs := make([]WriteBlobLocationReq, len(result.Blobs))
    for i, b := range result.Blobs {
        locReqs[i] = WriteBlobLocationReq{
            BlobHash:  b.BlobHash,
            Locations: blobLocations[i],
        }
    }
    if err := p.blobStore.WriteBlobLocations(ctx, locReqs); err != nil {
        return fmt.Errorf("blobstore.WriteBlobLocations: %w", err)
    }

    // 3. 元数据模块编排层: 写入 content 主表 + content_blob 关联表
    //    role / sort_order / business_meta 在 content_blob 中, 不污染 blob 主表
    if err := p.contentMeta.WriteContentMeta(ctx, WriteContentMetaReq{
        ContentID:    result.ContentID,
        ContentType:  result.ContentType,
        TypeMetadata: result.TypeMetadata,
        Blobs:        result.Blobs,  // BlobHash + BlobType + Size (供 content_blob 建立关联)
        Roles:        result.Roles, // role + sort_order + business_meta (写入 content_blob 字段)
    }); err != nil {
        return fmt.Errorf("contentmeta.WriteContentMeta: %w", err)
    }

    return nil
}
```

> **事务实现注记**：跨层 PG 事务通过 gRPC metadata 传递事务句柄（XID）。BlobStore 服务在第一次调用时 `BEGIN` 并返回 XID，后续 ContentMeta 调用携带该 XID 复用同一事务。任一步失败时返回特定错误码，调用方据此发 `ROLLBACK`；全部成功则发 `COMMIT`。若 gRPC 链路在事务中途断开，PG 的 `idle_in_transaction_session_timeout` 会自动回滚。详见 §4.2 的事务失败与 GC 清理。

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
  ├─ blob: sha256:a3f5e8... (mp4_init_segment, 720p init)  → [115:acct_03:fid_xxx, baidu:acct_05:fid_yyy]
  ├─ blob: sha256:b7c1d2... (m4s_media_segment, 720p seg1) → [115:acct_03:fid_aaa, baidu:acct_05:fid_bbb]
  ├─ blob: sha256:c9e3f4... (m4s_media_segment, 720p seg2) → [...]
  └─ blob: sha256:d1a5b6... (mp4_init_segment, 4k init)   → [...]
  + type_metadata: { "mpd_xml": "...", "duration_s": 3600, "representations": [...] }
```

元数据写入使用一条 PostgreSQL 事务，跨 storage 域（blob + blob_location）和元数据模块（content + content_blob）两层：

```sql
BEGIN;
  -- ── storage 域: 内容寻址层 (blob 主表, 跨 content 去重) ──
  -- INSERT ... ON CONFLICT (blob_hash) DO NOTHING  -- 已存在的 blob 跳过 (去重)
  INSERT INTO blob (blob_hash, blob_type, size_bytes) VALUES
    ('sha256:a3f5e8...', 'mp4_init_segment', 1024),
    ('sha256:b7c1d2...', 'm4s_media_segment', 2097152),
    ('sha256:c9e3f4...', 'm4s_media_segment', 2097152),
    ('sha256:d1a5b6...', 'mp4_init_segment', 2048)
  ON CONFLICT (blob_hash) DO NOTHING;

  -- blob 位置 (K=2 冗余, 跨 content 共享)
  INSERT INTO blob_location (blob_hash, backend_id, file_id) VALUES
    ('sha256:a3f5e8...', '115:acct_03', 'fid_xxx'),
    ('sha256:a3f5e8...', 'baidu:acct_05', 'fid_yyy'),
    ('sha256:b7c1d2...', '115:acct_03', 'fid_aaa'),
    ('sha256:b7c1d2...', 'baidu:acct_05', 'fid_bbb'),
    ...;

  -- ── 元数据模块: content 编排层 ──
  -- content 主表
  INSERT INTO content (content_id, content_type, type_metadata, created_at)
  VALUES ('abc123', 'dash_video', '{"mpd_xml":"...","duration_s":3600}', now());

  -- content_blob 关联表 (blob 在此 content 内的编排信息)
  -- role/sort_order/business_meta 都在这里, 不在 blob 主表
  INSERT INTO content_blob (content_id, blob_hash, role, sort_order, business_meta) VALUES
    ('abc123', 'sha256:a3f5e8...', 'init',  0, '{"representation_id":"720p","bitrate":500000}'),
    ('abc123', 'sha256:b7c1d2...', 'media', 1, '{"representation_id":"720p","bitrate":500000,"segment_number":1}'),
    ('abc123', 'sha256:c9e3f4...', 'media', 2, '{"representation_id":"720p","bitrate":500000,"segment_number":2}'),
    ('abc123', 'sha256:d1a5b6...', 'init',  0, '{"representation_id":"4k","bitrate":4000000}');
COMMIT;
```

Schema 定义见 [storage/README.md §9.1](../storage/README.md)（分层 schema：内容寻址层 + 编排层）。

### 4.2 事务失败与 GC 清理

- **事务提交**：所有 blob 的 K 个上传完成后，一条 PG 事务批量写入 blob + blob_location + content + content_blob。
- **事务失败回滚**：PG 事务失败则所有元数据回滚。已上传的孤儿文件由 GC worker 清理。
- **写入成功**：事务提交后，内容及所有 blob 被视为"可用"，读取路径可立即查询 blob 位置。

---

## 5. 入库事件通知

### 5.1 ContentIngestedEvent

入库完成后，管线发布 `ContentIngestedEvent` 事件。ingest 域传递：
- blob 列表（含 `BlobHash` = SHA-256 + `BlobType` = 二进制产出类型），属内容寻址层
- blob 在该 content 内的编排信息（`BlobRole`：role/sort_order/business_meta），属元数据层
- 不给出任何 pin 建议。pin 决策完全由分发域的策略层根据全局信息（流行度、缓存空间、role）自行决定

```go
type ContentIngestedEvent struct {
    ContentID   string
    ContentType string              // "dash_video" | "image" | "document" | ...
    Blobs       []BlobDescriptor    // 内容寻址层: BlobHash(SHA-256) + BlobType + Size
    Roles       []BlobRole          // 编排层: blob 在该 content 内的 role + sort_order + business_meta
    Timestamp   time.Time
}
```

### 5.2 分发域的处理

分发域的 PinOrchestrator 监听 `ContentIngestedEvent`，根据内容类型、`Role`（编排语义）和全局流行度决定 pin 策略。`BlobType` 用于辅助判断（如是否需要转码、是否能流式播放），但 pin 决策主要基于 `Role`（因为"前 N 段 media"是编排语义，不是二进制类型）：

| 内容类型 | Role 分类 | 分发域策略层决策（热门） | 分发域策略层决策（冷门） |
|---------|----------|----------------------|----------------------|
| DASH 视频 | init / media | pin init + 前 5 段 media | pin 仅 init |
| 图床 | original / thumbnail | pin thumbnail + original | pin 仅最小 thumbnail |
| 文档 | page | pin 前 3 页 | pin 仅 page_1 |

**职责边界**：
- ingest 域：负责加工处理 + 产出 blob（内容寻址）+ 标注 BlobRole（编排语义）。不关心缓存策略、流行度、pin 决策
- 分发域策略层：根据 Role + 流行度 + 缓存空间全局信息，自行决定 pin 什么、pin 多少
- 新增内容类型时，ingest 实现自己的 ContentIngester 产出 blob + BlobRole；分发域注册对应的 PinStrategy

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

    // 3. 扫描输出目录，构建 blob 列表（内容寻址层）+ blob role 列表（编排层）
    //    scanDashOutput 同时返回两份: blobs (SHA-256 + 二进制类型) 和 roles (码率/段号等业务语义)
    blobs, roles, duration, err := d.scanDashOutput(outDir)
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

    // 注: ingest 不决定 pin 策略, 只产出 blob (内容寻址) + BlobRole (编排)
    // 分发域策略层根据 Role("init"/"media") + 流行度自行决定 pin 什么

    return &ProcessResult{
        ContentID:    contentID,
        ContentType:  d.ContentType(),
        Blobs:        blobs,    // 内容寻址层: SHA-256 + "mp4_init_segment"/"m4s_media_segment"
        Roles:        roles,    // 编排层: role="init"/"media" + business_meta={representation_id,bitrate,segment_number}
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

    var blobs []BlobDescriptor
    var roles []BlobRole

    // 2. 原图 blob (内容寻址层)
    origHash := sha256sum(origPath)
    blobs = append(blobs, BlobDescriptor{
        BlobHash: origHash, Filename: origPath,
        BlobType: "jpeg_original",  // 二进制产出类型 (假设是 JPEG)
        Size: fileSize(origPath),
    })
    // 原图的编排信息 (元数据层)
    roles = append(roles, BlobRole{
        BlobHash: origHash, Role: "original", SortOrder: 0,
        BusinessMeta: map[string]any{"width": origWidth, "height": origHeight},
    })

    // 3. 生成缩略图（不同尺寸）— 每个尺寸一个 blob
    thumbSizes := parseThumbSizes(opts.Metadata["image_thumbnail_sizes"])  // [200, 800]
    for _, size := range thumbSizes {
        thumbPath := generateThumbnail(origPath, size)
        thumbHash := sha256sum(thumbPath)
        blobs = append(blobs, BlobDescriptor{
            BlobHash: thumbHash, Filename: thumbPath,
            BlobType: "jpeg_thumbnail",  // 二进制产出类型
            Size: fileSize(thumbPath),
        })
        // 缩略图的编排信息: role="thumbnail", business_meta 含尺寸
        roles = append(roles, BlobRole{
            BlobHash: thumbHash, Role: "thumbnail", SortOrder: size / 100,
            BusinessMeta: map[string]any{"width": size, "height": size},
        })
    }

    // 4. 提取 EXIF（专属元数据）
    exif := extractEXIF(origPath)
    typeMeta, _ := json.Marshal(map[string]interface{}{
        "exif": exif,
        "thumbnail_sizes": thumbSizes,
    })

    // 注: ingest 不决定 pin 策略, 只产出 blob (内容寻址) + BlobRole (编排)
    // 分发域策略层根据 Role("original"/"thumbnail") + 流行度自行决定 pin 什么

    return &ProcessResult{
        ContentID: contentID, ContentType: "image",
        Blobs: blobs, Roles: roles, TypeMetadata: typeMeta,
    }, nil
}

// 注册
pipeline.RegisterIngester(&ImageIngester{})
```

### 7.2 扩展检查清单

新增内容类型时需要完成：

| 步骤 | 在哪个域 | 内容 |
|------|---------|------|
| 1. 实现 ContentIngester | ingest | Process 方法：预处理 + blob 列表（SHA-256 + BlobType）+ BlobRole 列表（role/sort_order/business_meta）+ 专属元数据 |
| 2. 注册到 IngestPipeline | ingest | `pipeline.RegisterIngester(&XxxIngester{})` |
| 3. 注册 pin 决策策略 | distribution | PinOrchestrator 增加该内容类型的 pin 决策逻辑 |
| 4. 注册回源元数据查询（如需） | distribution | 如果回源时需要查专属元数据（如 DASH 的 MPD），在元数据模块中注册查询路径 |

**不需要修改的部分**：
- 通用上传管线（uploadBlobToK）
- 元数据事务（blob + blob_location + content + content_blob）
- Backend 抽象和权重计算
- SyncBroadcaster 事件传输
