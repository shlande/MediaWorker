# 规划/未接线功能与已落地项对照

> 历史台账（T1-T20 remediation），保留作审计参考。

> 本节原本罗列"代码已存在但未启用"项。经 T1-T20 remediation，多数已落地。下表逐项标注当前状态，原历史记录保留在 plan checkbox 22 与 git history。

| 功能 | 位置 | 当前状态 | 落地 todo |
|---|---|---|---|
| 哈希环路由 + 代理到主节点 | `internal/node/routing/edge_router.go` + `cmd/edge-node/main.go` | **已接线**：`EdgeRouter.HandleBlobRequest` 挂 HTTP mux，代理失败回退本地 backhaul | T11/T12 |
| DNS+302 区域调度器 | `internal/node/routing/scheduler.go` | 模拟实现，未接入 main.go（设计探索，非生产路径） | — |
| 非 L4 节点的 L4 流式回退取流 | `backhaul.HandleBlobNoL4` 中 `L4Fetcher` | **已接线（真实实现）**：T12 时以 `backhaulICPFetcher` 经 `ring.Get` + `icp.FetchFromPeer` 兜底；现由 `internal/node/l4fetch.Fetcher` 落地——经 libp2p 流协议 `/edge/l4/get/1.0.0` 向 peerstore 中 `L4Backhaul=true` 的活跃 peer 轮询拉流，服务端为 L4 节点的 `BackhaulManager.HandleBlobL4`（commit bd9fd41、2a2f078） | T12 → mwcli plan T1/T3 |
| ingest 事件经 SyncBroadcaster 实时推送 | `internal/ingest/syncpub/`（T8） | **已接线**：`SyncPublisher` 经 libp2p `/edge/control/1.0.0` 直发控制面 `SyncBroadcaster`，事件类型 `CONTENT_INGESTED`；启动 `CheckConnectivity` fail-closed | T8 |
| IngestOrchestrator gRPC 任务派发 | 仅存在于 `docs/ingest/` 设计 | 未实现，当前为 HTTP 直推（独立部署模式） | — |
| ingest 冗余度配置生效 | `pipeline.go` + `NewIngestPipeline` | **已生效**：`ingest.redundancy` 经 `NewIngestPipeline(... redundancy)` 传入 `uploadAllBlobs`，`<=0` 规范化为 `2` | T3 |
| edge-node `/metrics` 暴露 | `monitor.Metrics.HTTPHandler()` | **已暴露**：edge-node、control-plane、ingest-worker 三服务 mux 均挂 `GET /metrics`，无独立端口 | T20 |
| 部分配置项（`advertise_interval`、`peer_store.gc_interval`、JWT/NAT/cache enabled 等） | `internal/config/` | **已接线**：`advertise_interval` 入 `NewEdgeDiscovery`；`peer_store.gc_interval` 启 `PeerEntryStore.StartValueLogGC`；`*_cache.enabled` 门控构造；`nat_traversal.*` 经 `*bool` + Effective() 路径生效 | T15 |
| 配置项删除（`cold_cache`、`fetch_segment_*`、`metadata.popularity_query_interval` 等） | `internal/config/` | **已删除**：相关字段移除，`scanDeprecatedConfigKeys` 对遗留键发 `slog.Warn` | T17 |
| 位置查询控制面 API | `GET /v1/blob-locations/{hash}`（T9） | **新增**：control-plane JWT HTTP server mux 新增此路由；Bearer JWT + Edge capability 鉴权；返回 `{"locations":[...]}` | T9 |
| 位置查询客户端 | `internal/storage/dataplane/httplocclient.go`（T10） | **新增**：`HTTPLocationClient`，404 → 空切片+nil error；edge main 暂未接线（`LocalDataPlane` 保持 nil，独立后续项） | T10 |
| janitor GC 服务 | `cmd/janitor`（T13/T14） | **新增**：独立二进制；两阶段软删 + dry-run 默认 true + once/interval 双模式；详见 `docs/janitor.md` | T13/T14 |
| ingest 临时目录治理 | `pipeline.go` `ProcessResult.WorkDir` + `sweepStaleWorkDir` | **已生效**：处理成功后 `defer os.RemoveAll(result.WorkDir)`，启动扫描陈旧目录 | T4 |
| ingest 上传限额 + work_dir 空闲检查 | `http.max_upload_bytes` + `checkWorkDirDiskSpace` | **已生效**：默认 10 GiB，启动时若 free < 2×MaxUploadBytes 发 `slog.Warn` | T5 |
| JWT 策略化 + 节点续签循环 | CP `jwt_policy` + 节点 `runJWTRefreshLoop` | **已生效**：`default_capabilities` 与 `declared ∩ default` 授权；节点循环按 `min(refreshInterval, exp-now-refreshBeforeExpiry)` 续签 | T6/T7 |
| gossip 热度驱动缓存淘汰 | `WarmCache.SetPinChecker` + `SetPopSource` + `MergedPopularity.Snapshot` | **已生效**：gossip 订阅合并热度，经 `MinTrustedWeight` 过滤后驱动 `Evict` | T18 |
| 配置接线批 B（CP 侧） | `jwt_http.read/write_timeout`、`pin_orchestrator.top_contents_limit`、`sync_broadcaster.{protocol_id,send_timeout}` | **已生效**：经函数式选项或 `Run` 参数传入 | T16 |
| metrics 跨服务装配 | edge-node/CP/ingest-worker | **已暴露**：详见上文 `/metrics` 行 | T20 |
| CP/Node 包隔离边界 | `internal/controlplane/metrics/` | **已修复**：CP 专属计数器迁出 `internal/node/monitor`，恢复 `TestIsolation_ControlPlaneBinaryNoNodeCode` | T20-fix |
