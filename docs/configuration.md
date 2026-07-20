# MediaWorker 配置项参考

> 本文档汇总四个服务的配置项表（自原 api/ 接口文档迁移），按服务分节。配置加载器实现见 `internal/config/`。

## 1. control-plane（`configs/control-plane.yaml`）

| YAML 路径 | 必填 | 默认 | 说明 |
|---|---|---|---|
| `identity.priv_key_path` | 是 | — | JWT 签名 Ed25519 私钥（PEM PKCS#8） |
| `identity.libp2p_priv_key_path` | 是 | — | libp2p 身份私钥（protobuf 格式） |
| `jwt_http.listen` | 是 | — | JWT HTTP 监听地址，如 `:8443` |
| `jwt_http.read_timeout` / `write_timeout` | 否 | `10s` | HTTP 超时（T16）；空 → 10s 默认，非法字符串 → 启动失败并指名字段 |
| `jwt_policy.ttl` / `refresh_before_seconds` / `bandwidth_quota_bytes` | 否 | `1h` / `300` / `50000000` | JWT 签发策略（T6）；覆盖默认 TTL/refresh/quota |
| `jwt_policy.default_capabilities.{edge,peer_icp,relay_provider}` | 否 | edge=true, peer_icp=true, relay=false | 默认能力集（T6）；`l4_backhaul` 仅白名单授权，不受此字段影响 |
| `l4_whitelist.db_path` | 否 | — | 白名单 BadgerDB 路径 |
| `pin_orchestrator.rebalance_interval` | 否 | — | 再平衡周期，如 `10m` |
| `pin_orchestrator.top_contents_limit` | 否 | `5000` | 再平衡 top-N（T16）；零/负值回退默认 5000 |
| `dht_bootstrap.listen_addrs` | 否 | `["/ip4/0.0.0.0/tcp/9001"]` | libp2p 监听 multiaddr |
| `dht_bootstrap.namespace` | 是 | — | DHT 发现命名空间，如 `edge` |
| `dht_bootstrap.advertise_ttl` / `advertise_interval` | 否 | `15m` / `5m` | advertise TTL 与心跳 |
| `dht_bootstrap.bootstrap_peers` | 否 | `[]` | 备用引导节点 |
| `metadata.pg_dsn` | 是 | — | PostgreSQL DSN |
| `sync_broadcaster.protocol_id` | 否 | `/edge/control/1.0.0` | 控制通道协议 ID（T16）；**改单侧会导致协议分裂**，CP/节点必须同步 |
| `sync_broadcaster.send_timeout` | 否 | `30s` | 单条发送超时（T16）；空 → 默认 30s |
| 环境变量 `LIBP2P_PSK` | 否 | — | 私网 32 字节 PSK（hex） |

## 2. edge-node（`configs/node-edge.yaml` / `configs/node-l4.yaml`）

完整结构见 `internal/config/config.go` 与示例 `configs/node-edge.yaml` / `configs/node-l4.yaml`。

| YAML 路径 | 说明 |
|---|---|
| `node.identity.priv_key_path` | **必填**，libp2p 身份私钥（protobuf） |
| `node.declared_capabilities.{edge,l4_backhaul,relay_provider,peer_icp}` | 申请的能力声明 |
| `node.libp2p.listen` | multiaddr 列表，如 `/ip4/0.0.0.0/tcp/9001` + QUIC |
| `node.libp2p.private_network.{enabled,force_pnet_env}` | PSK 私网开关 |
| `node.libp2p.dht.{mode,namespace,advertise_ttl,bootstrap_peers}` | DHT 发现 |
| `node.libp2p.nat_traversal.{autonat,auto_relay,dcutr}` | NAT 穿透 |
| `node.libp2p.peer_store.{path,gc_interval}` | peerstore BadgerDB。`gc_interval` 已接线（T15）：`PeerEntryStore.StartValueLogGC(ctx, interval)` 周期调 `badger.RunValueLogGC(0.5)`；默认 1h，零/负值 no-op |
| `node.libp2p.conn_gater.{ip_rate_limit,cidr_allowlist}` | 门控 |
| `node.jwt_service.endpoint` | **必填**，控制面 JWT HTTP 地址 |
| `edge.{prefix,warm,cold}_cache.{enabled,path,size_gb}` | 三级缓存 |
| `access_layer.data_plane.{enabled,drivers,link_pool.max_entries,rate_limit_local}` | L4 数据面 |
| `access_layer.{fetch_segment_server,fetch_segment_client}.enabled` | 兄弟节点分段取流开关 |
| `access_layer.vendor_profiles.<vendor>.{weight,base_latency_ms,bandwidth_mbps}` | 厂商画像 |
| `access_layer.rate_limits.<vendor>.{qps,burst,concurrent}` | 厂商限流 |
| `access_layer.cloud_accounts[]` | 云盘账号（vendor/account_id/client_id/client_secret/refresh_token/region/enabled） |
| `hash_ring.replicas` | 哈希环虚拟节点数，默认 150 |

**环境变量**：`CONTROL_PLANE_PUBKEY`（必填，控制面 Ed25519 公钥 hex）；`LIBP2P_PSK`（私网 PSK，`force_pnet_env` 时必填）。

**命令行**：`-config`（默认 `configs/node-edge.yaml`）、`-version`。

## 3. ingest-worker（`configs/ingest-worker.yaml`）

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

## 4. janitor（`configs/janitor.yaml`）

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
