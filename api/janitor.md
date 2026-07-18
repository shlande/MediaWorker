# janitor（独立 GC 服务）接口文档

入口：`cmd/janitor/main.go`　配置：`configs/janitor.yaml`（`internal/config/janitor.go`）　镜像：`Dockerfile.janitor`

> T13/T14 落地。Janitor 是与 edge-node / control-plane / ingest-worker 物理隔离的**独立二进制**，承担 blob 两阶段软删（mark + sweep）。**无 HTTP 端口**，仅 CLI 运行。

## 1. 职责

周期或单次扫描 PostgreSQL 元数据中的孤儿 blob（`blob.deleted_at` 非空且超过 `min_age`），执行两阶段软删：

1. **MarkOrphans**：单条 `UPDATE blob SET deleted_at = now() WHERE deleted_at IS NULL AND created_at < now() - min_age AND hash NOT IN (SELECT blob_hash FROM content_blob)`。标记后即对读路径不可见。
2. **SweepWithDryRun**：批量选取 `deleted_at < now() - grace` 的 blob，逐 blob 执行：TOCTOU 复核（`SELECT 1 FROM content_blob WHERE blob_hash=$1`，命中则 `UPDATE blob SET deleted_at=NULL` 救回）→ 取 `blob_location` 列表 → 逐副本 `Driver.Remove` → 单事务 `DELETE blob_location + blob`。

**dry-run 默认 true**：`Driver.Remove` 永不调用、PG `DELETE` 永不发出；仅 `slog.Info("would delete blob=... locations=N backends=[...]")`。**TOCTOU 救回在 dry-run 下仍执行**（rescue 不是 delete，必须重置 `deleted_at`）。

## 2. CLI 接口（无 HTTP）

| 项 | 值 |
|---|---|
| 端点 | 无 HTTP 端口，仅 CLI |
| 标志 `-config` | 配置文件路径，默认 `configs/janitor.yaml` |
| 标志 `-once` | 单次模式：跑一个 cycle 后退出（exit 0 成功 / 1 错误）；缺省看配置 `gc.once` |
| 标志 `-dry-run` | **默认 true**（plan line 221）；显式 `-dry-run=false` 才会真正删除。**覆盖配置**：以 CLI 标志为准 |

**鉴权**：N/A（无网络端点）。运行身份由部署层（K8s ServiceAccount / systemd user）控制；需 PG DSN 读写权限与云盘账号 `Driver.Remove` 权限。

## 3. 错误码（退出码）

| 退出码 | 条件 |
|---|---|
| 0 | 单次模式（`-once`）下 cycle 成功完成；或 interval 模式收到 SIGTERM/SIGINT 优雅退出 |
| 1 | 配置加载失败、PG 不可达（`metadata client: metadata: ping postgres: ...`）、`-once` 模式下 cycle 内部错误 |

interval 模式下单个 cycle 失败：`slog.Error` 记录后继续下一周期（不退出进程）；仅 `-once` 模式或启动阶段失败才会 `os.Exit(1)`。

## 4. 配置项（`configs/janitor.yaml`）

完整结构：`internal/config/janitor.go::JanitorConfig`。

| YAML 路径 | 必填 | 默认 | 说明 |
|---|---|---|---|
| `metadata.pg_dsn` | 是 | — | PostgreSQL DSN（与 control-plane / ingest-worker 共享元数据库） |
| `storage.cloud_accounts[]` | 是（≥1 enabled） | — | 云盘账号列表，复用 `IngestStorageConfig` 形态（vendor/account_id/client_id/client_secret/refresh_token/redirect_uri/region/enabled）。dry-run 下也需配置，便于 resolver 解析 backend_id |
| `storage.vendor_profiles.<vendor>.{weight,base_latency_ms,bandwidth_mbps}` | 否 | weight=2.0 | 厂商画像 |
| `storage.rate_limits.<vendor>.{qps,burst,concurrent}` | 否 | 驱动默认 | 厂商限流 |
| `gc.interval` | 否 | `1h` | interval 模式 cycle 周期（duration 字符串，如 `1h`、`30m`） |
| `gc.min_age` | 否 | `24h` | blob 进入 mark 候选的最小年龄（保护 in-flight ingest 事务窗口） |
| `gc.grace` | 否 | `24h` | soft-mark 到 hard-delete 的等待窗口；窗口内被引用即 rescue |
| `gc.batch_limit` | 否 | `500` | 单 cycle 处理的 blob_hash 数量上限 |
| `gc.dry_run` | 否 | `true`（指针 `*bool`，nil → true） | dry-run 开关。**唯一安全读法是 `cfg.GC.EffectiveDryRun()`**；直接解引用 `*cfg.GC.DryRun` 在字段省略时会 nil-panic |
| `gc.once` | 否 | `false`（指针 `*bool`） | 单次模式开关 |

**两层 dry-run 门控**：

1. **配置层** `EffectiveDryRun()`：YAML 省略 → true；显式 `dry_run: false` → false。
2. **CLI 层** `-dry-run` 标志：默认 true；显式 `-dry-run=false` → false。

**两层必须同时为 false 才会真正删除**。CLI 标志优先于配置。Janitor 永远调用 `gc.Collector.SweepWithDryRun(..., dryRun)`，**绝不调用** `gc.Collector.Sweep()`（live 删除器）；后者仅供 T13 单元测试与未来直接调用方使用。

## 5. 内部契约

- **`gc.AccountResolver`** 接口：`Resolve(backendID) → (driver.Driver, *circuitbreaker.CircuitBreaker, ok)`。生产侧由 `accountpool.AccountPool` 包装：`backend_id` 形如 `vendor:account_id`，按 `accountpool` 中注册的账号解析。
- **熔断传播**：单副本 `Driver.Remove` 失败 → `CircuitBreaker.ForceOpen()`（对本 cycle 与并发读路径同时可见）→ 本 cycle 跳过该 backend 的后续副本（`broken` map）。
- **TOCTOU 复核**：`SELECT 1 FROM content_blob WHERE blob_hash=$1 LIMIT 1`。命中即 `UPDATE blob SET deleted_at=NULL` 救回（dry-run 下也执行）。
- **per-blob 中止**：副本 N 失败后跳过副本 N+1..K；blob 行保留（`deleted_at` 不变），下个 cycle 重试。
- **NoLocations 路径**：`blob_location` 为空的软标记 blob 直接走单事务 `DELETE`（否则会永久卡死）。
- **PG 元数据访问器**：`PGMetadataClient.DB() *sql.DB`（T13 新增，T14 启用）—— docstring 警告调用方**禁止 Close**，生命周期由 `PGMetadataClient.Close()` 管理。

## 6. 部署

`Dockerfile.janitor`：`golang:1.25-alpine` 构建 + `alpine:3.20` 运行时（`ca-certificates` + `tzdata` + 非 root 用户）。**无 ffmpeg**（janitor 不做转码）、**无 EXPOSE**（无 HTTP 监听）。

推荐部署形态：

- **K8s CronJob**：`-once -dry-run=false`，每日/每周期一次。
- **K8s Deployment**：interval 模式（`gc.interval: 1h`），SIGTERM 优雅退出。
- 初次部署或版本升级后**首次运行建议 `-dry-run=true`**，核对日志中 `would delete` 输出符合预期再切换到 live 模式。

## 7. 安全考量

- **dry-run 默认值是 true，且双层门控**：防止误配置导致批量删除（plan line 221）。
- **无网络端口**：攻击面仅限于 PG 与云盘 API 凭据；运行身份需最小权限（PG: blob 表读写 + blob_location 读写；云盘: 文件删除权限）。
- **`storage.cloud_accounts`** 即便 dry-run 也需配置：`AccountResolver` 需解析 `backend_id` 才能记录"会删除哪些后端"。dry-run 不发起任何云盘 API 调用。
- **失败可观测**：`SweepResult{Marked, Rescued, Deleted, Failed, Candidates}` 经 `slog.Info` 输出；Prometheus 指标暂未暴露（janitor 无 `/metrics`）。
