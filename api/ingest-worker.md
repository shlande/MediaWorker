# ingest-worker（入库服务）接口文档

入口：`cmd/ingest-worker/main.go`　配置：`configs/ingest-worker.yaml`（`internal/config/ingest.go`）

## 1. 职责

入库 worker 是系统的**写入路径**（与分发节点物理隔离）：接收内容上传 → 按内容类型预处理（转码/缩略图）→ 以 K 冗余上传云盘 → 单事务写入 PostgreSQL 元数据 → 发布入库事件。当前为独立部署模式：任务通过 HTTP 直推，无队列、gRPC 派发（`docs/ingest/` 中的 `IngestOrchestrator` 属未来规划）。

## 2. HTTP API

路由注册于 `cmd/ingest-worker/main.go:74-81`，共两个。

### 2.1 `POST /ingest/{content_type}` —— 内容入库

**路径参数**：`{content_type}` —— 已注册处理器类型，当前支持 `dash_video`、`image`。

**请求**：`multipart/form-data`（总上限 10GB，64MB 内存缓冲，其余落盘）

| 字段 | 必填 | 说明 |
|---|---|---|
| `file` | 是 | 待处理文件二进制 |
| `metadata` | 否 | JSON 字符串，解析为 `map[string]string`，按键驱动处理器行为（见下表） |
| `content_id` | 否 | 指定内容 ID；缺省自动生成 UUID |

**metadata 键**：

| 键 | 适用类型 | 示例 | 默认 |
|---|---|---|---|
| `dash_bitrates` | `dash_video` | `"500k,1500k,4000k"` | 同示例 |
| `dash_seg_duration` | `dash_video` | `"4"`（秒） | `"4"` |
| `image_thumbnail_sizes` | `image` | `"200,800"`（宽度 px） | `[200, 800]` |

**响应**：

| 状态码 | 条件 | 响应体 |
|---|---|---|
| 200 | 成功 | `{"content_id":"<uuid-or-provided>"}`（`Content-Type: application/json`） |
| 400 | 缺 content_type / multipart 非法 / 缺 file 字段 / metadata JSON 非法 | 错误文本 |
| 405 | 非 POST | — |
| 500 | 处理 / 上传 / 元数据事务失败 | 错误文本 |

**鉴权**：无。

**注意事项**：上传整体先缓冲到磁盘再处理，非流式——大视频会产生较高磁盘 I/O 与首字节延迟。

### 2.2 `GET /healthz` —— 存活探针

返回 200，body `ok`。K8s liveness/readiness 探针均指向此路由（见 `deploy/ingest-worker.yaml`）。

## 3. 入库处理管线（`internal/ingest/pipeline.go`）

`IngestPipeline.Ingest()` 五个阶段：

```
HTTP POST /ingest/{content_type}
   │  multipart: file + metadata + content_id
   ▼
handleIngest（方法检查 / MaxBytesReader / ParseMultipart）
   ▼
IngestPipeline.Ingest()
   1. Route      按 content_type 查 ContentIngester，未注册 → 500
   2. Process    类型专属预处理（ffmpeg 转码 / 缩略图），产出 blobs+roles+临时文件
   3. Upload     全部 blob 并发上传（errgroup 限 10），每 blob 写 K=2 个后端，
                 容忍部分失败：至少 1 个成功即视为该 blob 上传成功
   4. Txn        PGMetadataClient.WriteIngestTransaction：
                 单事务写 blob / blob_location / content / content_blob 四表，
                 任一失败整体回滚（blob 表 ON CONFLICT DO NOTHING 去重）
   5. Publish    异步 goroutine 发布 ContentIngestedEvent
   ▼
HTTP 200 {"content_id":"..."}
```

**任务来源**：HTTP 直推（无队列/轮询）。**结果回报**：同步返回 `content_id`；事件异步发布——经 `SyncPublisher`（`internal/ingest/syncpub/`）通过 libp2p 控制通道 `/edge/control/1.0.0` 直发控制面 `SyncBroadcaster`，事件类型 `CONTENT_INGESTED`（见 T8）。启动时 `SyncPublisher.CheckConnectivity` 拨号失败即 fail-closed（拒绝服务，避免静默丢事件）。`LogPublisher` 保留供测试与降级使用，生产路径不选择。

**已修复（T3）**：冗余度由 `ingest.redundancy` 控制，经 `NewIngestPipeline(... redundancy int)` 传入并下发至 `uploadAllBlobs`（`pipeline.go`）。`<=0` 在构造器内规范化为 `2`，保留 K=2 的默认行为；调用点不再硬编码。

## 4. 已实现的内容处理器

### 4.1 `dash_video`（`internal/ingest/dash.go`）

- 输入 MP4 写临时文件 → ffmpeg 多码率转码（libx264 + AAC 128k，默认 500k/1500k/4000k，分辨率 360p/720p/1080p）→ DASH 输出（`manifest.mpd` + `init_*.m4s` + `seg_*.m4s`）。
- 扫描输出目录，逐文件计算 SHA-256（`sha256:` 前缀），生成 `BlobDescriptor` 与 `BlobRole`（角色 `init`/`media`，`business_meta` 含 `representation_id`、`segment_number`）。
- blob 类型：`mp4_init_segment` / `m4s_media_segment`；MPD XML 作为 type_metadata。

### 4.2 `image`（`internal/ingest/image.go`）

- 标准库解码 → 保存原图 → 近邻插值缩略图（JPEG q85，只缩小不放大，默认宽度 200/800）→ 简化 EXIF 方向检测。
- 角色：`original`（sort_order=0）、`thumbnail`（sort_order=宽度）。
- blob 类型：`{jpeg|png}_original`、`jpeg_thumbnail`；type_metadata 含 format/width/height/thumbnail_sizes/exif_orientation。

### 4.3 扩展方式

实现 `ContentIngester` 接口（`Process(ctx, input, opts) (ProcessResult, error)` + `ContentType() string`），在 `main.go` 中 `pipeline.RegisterIngester()` 注册即可新增类型。

## 5. 外部系统依赖

| 系统 | 用途 | 接口/实现 |
|---|---|---|
| PostgreSQL | 元数据存储 | `BlobStoreWriter.WriteIngestTransaction` → `PGMetadataClient`（`database/sql` + lib/pq，最多 10 连接） |
| 云盘 | blob 存储后端 | `driver.Driver.Put()`，已实现 Baidu（PCS API）、OneDrive（Graph API）；115/夸克/阿里云盘为 mock |
| 账号池 | 上传账号选择 | `AccountPool.SelectK()`：过滤不健康/熔断/限流账号，跨厂商冗余（每厂商最多 K/2），按并发/权重打分 |
| 熔断/限流 | 上传保护 | 每账号 `circuitbreaker`（阈值 5 失败、冷却 100ms）+ `rate.Limiter` |
| OAuth2 | 云盘鉴权 | `auth.TokenManager`（refresh_token 授权，singleflight 去重） |
| 事件总线 | 入库事件 | `EventPublisher`，生产为 `SyncPublisher`（经 `/edge/control/1.0.0` 推送 CP，见 T8）；`LogPublisher` 仅测试降级 |

## 6. 配置项（`configs/ingest-worker.yaml`）

| YAML 路径 | 必填 | 默认 | 说明 |
|---|---|---|---|
| `http.listen` | 是 | — | HTTP 监听地址，如 `:8080` |
| `http.read_timeout` / `write_timeout` | 否 | `10s` | HTTP 超时 |
| `http.max_upload_bytes` | 否 | `10737418240`（10 GiB） | 上传 body 上限（T5）；`<=0` 规范化为默认值 |
| `metadata.pg_dsn` | 是 | — | PostgreSQL DSN |
| `ingest.ffmpeg_path` | 是 | — | ffmpeg 二进制路径 |
| `ingest.work_dir` | 是 | — | 处理临时目录，如 `/data/ingest` |
| `ingest.redundancy` | 否 | `2` | 冗余度 K。生效路径：`LoadIngestWorkerConfig` 解析 → `NewIngestPipeline(... cfg.Ingest.Redundancy)` → `uploadAllBlobs`（`pipeline.go`）。`<=0` 规范化为 `2`。 |
| `control_plane.multiaddr` | 是 | — | 控制面 libp2p multiaddr（含 `/p2p/<peerID>` 后缀）。T8 启用，`SyncPublisher` 拨号目标。 |
| `control_plane.priv_key_path` | 是 | — | ingest-worker 自身 libp2p Ed25519 身份私钥（protobuf）。 |
| `storage.cloud_accounts[]` | — | — | 云盘账号列表（vendor/account_id/client_id/client_secret/refresh_token/redirect_uri/region/enabled） |
| `storage.vendor_profiles.<vendor>.{weight,base_latency_ms,bandwidth_mbps}` | 否 | weight=2.0 | 厂商画像 |
| `storage.rate_limits.<vendor>.{qps,burst,concurrent}` | 否 | 驱动默认 | 厂商限流 |

**环境变量**：`LIBP2P_PSK`（必填，32 字节 hex，私网 PSK；T8 `SyncPublisher` 用于与控制面建立私网连接）。

**HTTP 路由**（注册于 `cmd/ingest-worker/main.go`）：`POST /ingest/{content_type}`、`GET /healthz`、`GET /metrics`（T20，Prometheus 抓取，无鉴权，依赖部署层隔离）。

## 7. 部署

`deploy/ingest-worker.yaml`：K8s ConfigMap（配置）+ Deployment（1 副本，镜像 `ghcr.io/shlande/ingest-worker:latest`，端口 8080，requests 100m/256Mi、limits 2000m/2Gi，`/healthz` 探活）+ Service（80 → 8080）。

## 8. 入库事件回路（T8）

```
IngestPipeline.Ingest() 阶段 5 Publish (异步 goroutine)
   ▼
SyncPublisher.Publish(ContentIngestedEvent)
   ▼  3x 重试，100ms · 2^attempt 退避；最终失败 slog.Error + publishFailures++（不静默丢弃）
libp2p stream /edge/control/1.0.0 → 控制面 SyncBroadcaster
   ▼
PinOrchestrator.OnContentIngested → 初始 pin 计算与下发
```

启动序列关键约束：HTTP listen 之前 `SyncPublisher.CheckConnectivity(dialCtx, 10s)` 必须通过——失败即 `os.Exit(1)`，保证 worker 不会接收它无法回报的内容。
