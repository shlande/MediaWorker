# 界面接口需求文档（按已生成 UI 反推）

> 依据 `ui/` 下已生成的 10 个页面（界面二 6 页 + 界面三 3 页 + 门户页）反推所需接口与后端扩展。
> 与 `next-iteration-requirements.md` 的关系：本文档是**按 UI 实际元素校准后的精确清单**，F/M 编号沿用；冲突处以本文档为准。
> 界面一（ingest 上传）按 UI 门户页范围说明不在本期，对应接口需求见 `next-iteration-requirements.md` F2，本文档不重复。
> 成本分级：**低**=纯查询面/小改动；**中**=需新表/新字段/新事件/聚合逻辑；**高**=需新子系统。

---

## 1. 页面 × 数据源 × 接口总表

| UI 页面 | 主接口 | 数据源 | 成本 |
|---|---|---|---|
| dashboard.html | `GET /v1/admin/overview` | PG + PinOrchestrator 内存 + Prometheus + 告警管线 | 中 |
| nodes.html | `GET /v1/admin/nodes` + `/{peer_id}` + whitelist 三件套 | NODE_STATUS_REPORT（**需扩展字段**）+ BadgerDB 白名单（**需元数据**） | 中 |
| accounts.html | `GET/POST/PUT /v1/admin/accounts` + ban/circuit/rotate | PG cloud_account/account_health + AccountRegistry + **新熔断事件** | 中 |
| content.html | `GET /v1/admin/contents` + `/{id}` + pin/unpin + DELETE | PG 四表 + PinOrchestrator + **pin 计数聚合** | 中 |
| policy.html | `GET /v1/admin/backends` + quota | **无数据源**（用量采集/PolicyController/allocator 接线均缺） | **高** |
| audit.html | `GET /v1/admin/audit` + 导出 | JWT JSON-lines（已有）+ **admin_audit 新表** | 低-中 |
| edge-node.html | `GET /v1/status` + `/v1/cache` | 节点本地内存/配置 | 低 |
| edge-pins.html | `GET /v1/pins` + retry | pinstore（**需加 content_id/last_error**）+ 本地 plan 日志 | 低-中 |
| edge-network.html | `GET /v1/network` + `/v1/backhaul` + `/v1/peers` | libp2p host / peerstore / accountpool | 低 |
| 全部 | 认证三件套 | **app_user 新表（F1）** | 中 |

---

## 2. A. 认证接口（F1 精简版）

UI 侧边栏已写死 `admin@mediaworker.internal · 平台管理员`——落地需要真实登录态。

```
POST /v1/auth/login   {username, password} → {token, expires_at, roles}
GET  /v1/auth/me      → {user_id, username, roles, email}
POST /v1/auth/logout
```

- 数据模型：`app_user` 表（见 next-iteration-requirements.md F1）。本期 UI 只有 admin 视角，**roles 可简化为 admin/operator/uploader 三值但只启用 admin**，其余角色随界面一/三开放。
- 所有 B 节接口挂 `Authorization: Bearer` 中间件；C 节节点本地接口用节点配置 `admin_api.token`（`X-Admin-Token` 头），不走用户体系。
- 成本：中（新表 + 中间件 + 签发/校验）。

---

## 3. B. 控制面 admin API（界面二）

### 3.1 `GET /v1/admin/overview` —— dashboard.html

| UI 元素 | 字段 | 数据源 | 成本/扩展 |
|---|---|---|---|
| SLO 四卡 | `slo.{ttfb_p95, cache_hit_rate, backhaul_success_rate, account_health_rate}` | 前三者 Prometheus 即时查询；账号健康率 PG `account_health` 聚合 | 低 |
| 在线节点 + 能力分布 | `nodes.{total, online, by_capability}` | PinOrchestrator nodeSpaces 内存 | 低 |
| ~~社区节点数~~ | — | **无 community 标记**，见调整文档 §2-dashboard | 改用 `non_l4` 计数，低 |
| prefix 空间分桶（充足/紧张/耗尽） | `nodes.space_buckets` | 按上报 prefix_space 分桶 | 低 |
| ~~灰名单计数~~ | — | **CP 无评分数据源**，见调整文档 §2 | 删除 |
| 命中率 24h 趋势 | （前端直查 Prometheus，见 §5） | — | 低 |
| 24h 热门 Top8 | `hot_contents[].{content_id, title, type, window_24h, pin_node_count, replicas}` | PG content_popularity join content；`pin_node_count` 需 CP 对下发 PinPlan 做记账 | 中 |
| 当前告警 | `alerts[].{name, severity, target, since, detail}` | **需告警管线**（§4.4） | 中 |
| PinPlan 近 1h 统计 | `pin_stats_1h.{batches, pins, unpins, manual}` | SyncBroadcaster 发送记账 | 低-中 |
| 回源带宽 | `backhaul.{used_bps, capacity_bps}` | Prometheus | 低 |

### 3.2 `GET /v1/admin/nodes` + `GET /v1/admin/nodes/{peer_id}` —— nodes.html

列表字段：`peer_id, capabilities[], prefix_space{used,total}, warm_space{used,total}, healthy, last_seen`（上报已有）+ 以下**需扩展**：

| UI 元素 | 现状 | 扩展 |
|---|---|---|
| region / version / uptime / 连接数 | NodeStatusReport 无此字段 | **扩展 NodeStatusReport**（节点本地全有）：`region, version, started_at, conn_count` — 低 |
| cold 分区（节点详情三级缓存） | 上报仅 prefix/warm | 上报加 `cold_space` PartitionStatus — 低 |
| JWT 状态（exp / 续签失败） | CP 知 exp（自己签的）；失败在节点侧 | v1：CP 返回 `jwt_exp` + "应续未续"推断（已签发 exp 将尽但未收到新申请）；节点上报续签失败数放扩展字段 — 中 |
| ~~评分/灰名单~~ | **无数据源**（评分是双向局部视图） | **删除**，见调整文档 §2-nodes |
| 详情·当前 PinPlan | CP 下发但无逐节点计划日志 | PinOrchestrator 记录 per-node 最近 N 条下发（seq/内容/指令/时间） — 中 |
| 详情·状态上报历史（近 10 次） | 只有当前值 | **新表 `node_status_history`**（上报时落库） — 中低 |

白名单三件套（UI 含"加入时间/加入人/生效状态"）：
```
GET    /v1/admin/whitelist          → [{peer_id, added_at, added_by, effective}]
POST   /v1/admin/whitelist          {peer_id}
DELETE /v1/admin/whitelist/{peer_id}
```
- `WhitelistStore` 仅持久化 peerID → **记录扩展为 {peer_id, added_at, added_by}**（BadgerDB value 从空改 JSON） — 低。
- `effective` = 该 peer 当前 JWT 是否已含 l4_backhaul（CP 有签发记录） — 低。

### 3.3 `GET/POST/PUT /v1/admin/accounts` + 操作 —— accounts.html

```
GET  /v1/admin/accounts?vendor=&state=
     → [{vendor, account_id, enabled, health:{state,latency_ms,error_msg,ban_until,last_check},
         rate_limit:{qps,burst,concurrent}, vendor_profile, base_latency_ms}]
POST /v1/admin/accounts                      {vendor, account_id, credential, rate_limit?}
PUT  /v1/admin/accounts/{vendor}/{id}        {credential?, enabled?, rate_limit?}
POST /v1/admin/accounts/{vendor}/{id}/rotate → 触发 CREDENTIAL_UPDATE（AccountRegistry 已有）
POST /v1/admin/accounts/{vendor}/{id}/ban    {reason?} → BAN 事件（MarkBanned 已有）
POST /v1/admin/accounts/{vendor}/{id}/unban  → UNBAN 事件
POST /v1/admin/accounts/{vendor}/{id}/circuit {action: force_open|force_close}
```

- 列表主体 = PG 两表 join，credential 永不返回 — 低。
- **熔断操作需新事件**：现有 8 个事件常量无熔断控制，`types.go` 新增 `CIRCUIT_FORCE_OPEN/CLOSE`（或复用 HEALTH_CHANGE 带载荷），节点侧收到后对本地熔断器 ForceOpen/ForceClose — 中低。
- **enabled 开关需生效路径**：`cloud_account.enabled=false` 需在 ACCOUNT_SNAPSHOT 生成时过滤（Registry 改动小） — 低。
- ~~存储用量列~~：无用量采集管线，**删除**，见调整文档 §2-accounts。
- ~~熔断器状态列~~：节点本地语义、CP 视图语义不清，**删除或改聚合**，见调整文档 §2-accounts。
- VendorProfile 编辑：PG `cloud_account.vendor_profile` 可写，但**节点从本地 YAML 读画像、CP 改动不传播** → v1 UI 改只读（调整文档 §2-accounts）；若要可编辑，需新增 `VENDOR_PROFILE_UPDATE` 事件或 ACCOUNT_SNAPSHOT 携带画像 — 中。

### 3.4 `GET /v1/admin/contents` + `/{id}` + pin 操作 + DELETE —— content.html

```
GET  /v1/admin/contents?sort=popularity&type=&replicas=&page=
     → [{content_id, title, content_type, total_bytes, blob_count,
         replicas:{have, want}, window_24h, pin_node_count, pending_delete}]
GET  /v1/admin/contents/{id}
     → {meta, type_metadata, blobs:[{hash, role, sort_order, business_meta, size}],
        locations:[{blob_hash, backend_id, file_id, account_health}]}
POST /v1/admin/pin    {content_id, target_nodes[], blobs?} → {seq}     （SendToNode 已有）
POST /v1/admin/unpin  同上
DELETE /v1/admin/contents/{id}  → 软删除：删 content_blob + content（blob 成孤儿 → janitor）
GET  /v1/admin/pin-plans?page=  → [{seq, target_node, pins, unpins, trigger, sent_at, ack_state}]
```

- 列表/详情主体 = PG 四表 join account_health — 低。
- `title`：**ingest metadata 无标题字段** → `POST /ingest` metadata 增加可选 `title`，存 `content.type_metadata->>'title'` 或新列 — 低。
- `replicas.have/want` = blob_location 按内容聚合（want=K） — 低。
- `pending_delete`：内容级删除后 UI 显示"待清理" → `content` 表加 `deleted_at`（删 content 时置位；janitor 硬删 blob 后行消失） — 低。
- `pin_node_count`：CP 对下发 PinPlan 记账后按内容聚合 — 中。
- `pin-plans.ack_state`：无逐条 ACK → **降级为两态**（已下发 / 待节点上报）：下发时间 < 该节点最近一次 NODE_STATUS_REPORT 时间即视为"已确认" — 低。
- 手动 pin 的"目标节点按剩余空间过滤"：PinOrchestrator nodeSpaces 内存直读 — 低。

### 3.5 `GET /v1/admin/backends` + 配额 —— policy.html

**本页大部分无数据源（用量采集/PolicyController/QuotaAllocator 接线均缺），整体方案见调整文档 §2-policy：v1 只保留"配额"一个精简视图，其余删除或整页置"规划中"。** 若保留配额：

```
GET /v1/admin/quota → {global_qps, node_count, base_share, allocations:[{peer_id, base_share}]}
```

- 前提：**QuotaAllocator 接线进 control-plane**（库已存在含测试，未 main 化；节点数取 nodeSpaces） — 中。
- `allocations[].当前用量/利用率/借用记录`：依赖用量上报与借用流水，**v1 删除**（调整文档）。

### 3.6 `GET /v1/admin/audit` —— audit.html

```
GET /v1/admin/audit?kind=&from=&to=&q=&page=  → [{ts, kind, actor, action, target, ip, result}]
GET /v1/admin/audit/export?同筛选             → JSON-lines 下载
```

- JWT 签发：现有 `auditlog.go` JSON-lines（peerID/IP/l4/quota/exp）→ 加查询面；**失败签发（403/429）目前不落审计 → 补记 result 字段** — 低。
- 手动干预/账号变更/白名单三类：**新表 `admin_audit`**（所有 B 节写操作统一埋点） — 中低。

---

## 4. C. 节点本地 status API（界面三，直连节点，`X-Admin-Token` 鉴权）

### 4.1 `GET /v1/status` —— edge-node.html

```
{peer_id, capabilities[], mode, region, version, uptime_sec, healthy,
 jwt:{exp, refresh_before, last_refresh_at, last_refresh_ok, refresh_fail_count_24h},
 score_view:{graylisted_peers},          -- 替代"自身评分"（无数据源，见调整文档 §2-edge-node）
 conn:{total, inbound, outbound},
 cache_hit_rate:{prefix, warm}, ttfb_p95_ms, relay_bytes_24h}   -- 瞬时值，趋势走 Prometheus
```

- 全部节点本地可取 — 低。**JWT 口径：exp 为签发后 1 小时**（UI 的"29 天"必改，见调整文档 §1）。

### 4.2 `GET /v1/cache` —— edge-node.html

```
{prefix:{total,used,blob_count}, warm:{...}, cold:{...},
 eviction_counters:{warm_1h, cold_1h}}    -- 逐出活动明细表 v1 删除，仅保留计数（调整文档）
```

### 4.3 `GET /v1/pins` + retry —— edge-pins.html

```
GET  /v1/pins?ready=&role=  → [{blob_hash, content_id, role, size, pinned_at, state, last_error}]
POST /v1/pins/{hash}/retry  → 202（ApplyPin 幂等，已有）
GET  /v1/pin-plans/recent   → [{seq, received_at, pins, unpins, applied}]   -- 本地 ring buffer
```

- **pinstore 扩展**：`PinEntry` 增加 `content_id`（PinPlan 载荷里有，落库即可）、`last_error`、`state`（ready/pulling/failed，替代布尔 Ready） — 低。
- ~~拉取进度 %~~：降级为三态（调整文档）。
- 本地 plan 日志：收到 PinPlan 时记录最近 N 条 — 低。

### 4.4 `GET /v1/network` + `/v1/peers` + `/v1/backhaul` —— edge-network.html

```
GET /v1/network  → {listen_addrs[], conn:{total,in,out}, dht:{mode,table_size},
                    gossipsub:{subscribed}, nat:{reachability, relay_circuits, dcutr_success_rate},
                    hash_ring:{position_pct, peers_on_ring}}     -- dcutr 需加一个计数器，低
GET /v1/peers    → [{peer_id, capabilities[], score, stale, last_seen, addrs[]}]   -- peerstore 直读，低
GET /v1/backhaul → （仅 L4，非 L4 返回 409）
                   {bandwidth:{used_bps,capacity_bps}, success_rate_24h, latency_p95_ms,
                    linkpool:{entries, hit_rate},
                    accounts:[{backend_id, health, circuit, qps:{used,limit}, inflight}]}
```

- 基本为本地组件直读 — 低。哈希环位置：hashring 模块存在（150 虚节点、防抖重建），可加位置查询 — 低；**注意环未接入请求路由**，UI 展示需加注（调整文档 §2-edge-network）。

### 4.5 节点本地操作（edge-node.html"本地操作"）

```
POST /v1/admin/reload-config   → 触发配置重载（当前无，需实现 SIGHUP 式重载或重启引导）— 中低
POST /v1/admin/flush-cache     {partitions:[warm,cold]} → 202 异步清空 — 中低（危险操作，二次确认 UI 已有）
```

---

## 5. D. Prometheus 直查清单（前端不经后端 API）

以下趋势/分布类图表建议**前端直查 Prometheus HTTP API**（`/api/v1/query_range`），不要经节点 status API 转发（节点不存时序）：

| UI 元素 | PromQL 对象 |
|---|---|
| dashboard 命中率 24h 趋势 | `edge_cache_hit_total / edge_cache_request_total` by cache_type |
| dashboard 回源带宽 | `edge_backhaul_bandwidth_bytes / capacity_bytes` |
| edge-node 命中率三序列趋势、TTFB 分布 | 同上 + `edge_ttfb_seconds` histogram_quantile |
| edge-network 回源成功率/延迟 | `storage_access_backhaul_*`、`storage_linkpool_*` |

告警管线（dashboard/edge-node 告警面板）：**Alertmanager webhook → CP 落 `alert_events` 表 → `GET /v1/admin/alerts`**；节点本地告警面板复用同一表按节点过滤，或 v1 从略（调整文档）。成本：中。

---

## 6. 支撑性后端扩展汇总（非 HTTP，但阻塞 UI 数据）

| # | 扩展 | 服务的页面 | 成本 |
|---|---|---|---|
| E1 | NodeStatusReport 加 `region/version/started_at/conn_count/cold_space/jwt_refresh_fail` | nodes 列表+详情 | 低 |
| E2 | 新表 `node_status_history`（上报落库，保留近 N 条） | nodes 详情历史 | 中低 |
| E3 | 白名单记录加 `added_at/added_by` | nodes 白名单表 | 低 |
| E4 | `content` 加 `deleted_at`；ingest metadata 加 `title` | content 列表/删除态 | 低 |
| E5 | PinOrchestrator/SyncBroadcaster 下发记账（per-node 计划日志 + pin_node_count + 1h 统计） | dashboard/nodes/content | 中 |
| E6 | `pinstore.PinEntry` 加 `content_id/state/last_error` + 节点本地 plan ring buffer | edge-pins | 低 |
| E7 | 新事件 `CIRCUIT_FORCE_OPEN/CLOSE`（types.go 常量 + 节点处理） | accounts 熔断操作 | 中低 |
| E8 | ACCOUNT_SNAPSHOT 过滤 `enabled=false` 账号 | accounts 开关 | 低 |
| E9 | 新表 `admin_audit`（所有写操作埋点）+ JWT 失败补记 result | audit | 中低 |
| E10 | `app_user` 表 + 用户令牌签发/校验中间件 | 全部 | 中 |
| E11 | QuotaAllocator 接线进 control-plane main | policy 配额视图 | 中 |
| E12 | Alertmanager webhook + `alert_events` 表 | dashboard/edge-node 告警 | 中 |

---

## 7. 成本汇总与排期建议

| 批次 | 内容 | 覆盖页面 |
|---|---|---|
| **第一批（低-中低）** | 认证三件套（E10）、accounts 主体、contents 主体、whitelist 三件套（E3）、nodes 列表（E1 扩展）、edge 三页 status/pins/network/backhaul（E6）、audit 查询（E9）、DELETE content（E4） | accounts、content、nodes、edge 三页、audit |
| **第二批（中）** | overview 聚合 + 告警管线（E12）、下发记账（E5）、上报历史（E2）、熔断事件（E7）、配额接线（E11）、节点本地操作 | dashboard、nodes 详情、policy 精简版 |
| **不做/转 UI 调整** | 用量采集、PolicyController 权重、节点评分聚合、VendorProfile 传播、拉取进度%、逐出明细、自身评分 | 见 `ui-adjustments.md` |
