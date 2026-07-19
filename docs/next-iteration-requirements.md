# 下一迭代功能补齐需求

> 本文档面向**下一次开发迭代**，把 `docs/ui-requirements.md` §10 闭环验证发现的断裂点（G1-G6）与管理 API 层，规划为可落地的功能需求。
> 每项包含：闭环定义、交互流程、API 契约、数据模型变更、与现有组件的交互点、验收标准。
> 编号对应：F1→G3、F2→G2、F3→G1、F4→G4、F5→G6、F6→G5；M 系列为管理/状态 API 层。

---

## 0. 迭代范围总览

| 编号 | 功能 | 优先级 | 服务的界面 | 预估量级 |
|---|---|---|---|---|
| F1 | 用户与权限体系 | **P0（前置）** | 全部三个 | 中 |
| F2 | Ingest 异步任务化 | **P0** | 界面一 | 大 |
| M1 | 管理查询 API（控制面只读视图） | **P0** | 界面二 | 中 |
| M2 | 节点本地 status API | **P0** | 界面三 | 中 |
| F3 | 内容访问层（MPD/段解析/试播） | **P1（建议提前评估）** | 界面一 + 业务主闭环 | 大 |
| M3 | 管理操作 API（账号/pin/BAN/审计） | **P1** | 界面二 | 中 |
| F4 | 节点吊销机制 | **P1** | 界面二 | 小 |
| F5 | 副本补齐 worker | **P2** | 界面一/二（副本健康） | 中 |
| F6 | PolicyController 权重链路 | **P2** | 界面二权重页 | 大 |

依赖关系：**F1 是所有"按 owner 过滤"的 API 的前置**；M1/M2 无前置可直接做；F2 依赖 F1（任务归属）；F3 依赖 M1 的 content 查询（可并行）；F5 复用 F2 的上传路径。

---

## F1. 用户与权限体系（P0，前置）

### 闭环定义
任何管理 API 调用都能识别"谁"、判定"能看什么/能操作什么"；content 与节点都能归属到用户。

### 角色与授权规则

| 角色 | content | 任务 | 节点 | 账号/Pin/权重 | 用户管理 |
|---|---|---|---|---|---|
| `uploader` | 自己的（CRUD） | 自己的 | — | — | — |
| `operator` | — | — | 自己的（只读+pin 重试） | — | — |
| `admin` | 全部 | 全部 | 全部 | 全部 | ✔ |

- 一个用户可同时是 uploader + operator（角色用位掩码或数组）。
- **与节点 CapabilityJWT 严格分离**：用户令牌是另一套签名密钥与载荷（避免节点凭证被拿去调管理 API）。命名建议：用户令牌 = `UserToken`（HMAC-SHA256 或 Ed25519 均可，复用 `internal/shared/jwt`）。

### 数据模型变更

```sql
CREATE TABLE app_user (
    user_id       UUID PRIMARY KEY,
    username      TEXT UNIQUE NOT NULL,
    password_hash TEXT NOT NULL,           -- bcrypt
    roles         TEXT[] NOT NULL,         -- {admin,operator,uploader}
    created_at    TIMESTAMPTZ DEFAULT now(),
    disabled      BOOLEAN DEFAULT false
);

ALTER TABLE content ADD COLUMN owner_id UUID REFERENCES app_user(user_id);
-- 历史数据：owner_id 允许 NULL，界面显示"系统导入"

CREATE TABLE node_owner (
    peer_id     TEXT PRIMARY KEY,          -- libp2p PeerID
    owner_id    UUID REFERENCES app_user(user_id),
    label       TEXT,                      -- 运营者起的名字，如 "北京-自建-01"
    claimed_at  TIMESTAMPTZ DEFAULT now()
);
```

### API 契约

```
POST /v1/auth/login        {username, password} → {token, expires_at, roles}
POST /v1/auth/logout       （服务端会话失效；若用无状态 JWT 则仅前端丢弃）
GET  /v1/auth/me           → {user_id, username, roles}

# 仅 admin
GET  /v1/admin/users
POST /v1/admin/users       {username, password, roles}
PUT  /v1/admin/users/{id}  {roles?, disabled?, password?}
POST /v1/admin/nodes/{peer_id}/assign   {owner_id, label}   -- 节点归属指派
```

### 与现有组件的交互
- 所有新增管理 API 挂在统一鉴权中间件后：`Authorization: Bearer <UserToken>`，中间件解析 → 注入 `user_id/roles` → 各 handler 做 owner 过滤（`WHERE owner_id = $1`）或角色拒绝（403）。
- 节点本地 status API（M2）用**节点级管理 token**（节点配置文件里的 `admin_token` 字段）而非用户体系——运营者直连自己节点，不需要经过控制面账号。

### 验收标准
- 无 token 调任何 `/v1/admin/*` 或 `/v1/tasks` 返回 401；uploader 访问他人 content 返回 403/404。
- admin 创建用户、禁用用户、指派节点归属后，对应视图过滤即时生效。

---

## F2. Ingest 异步任务化（P0）

### 闭环定义
上传提交后立即返回 `task_id`；任务的每个阶段状态、进度、失败原因可查；失败可重试；完成后得到 `content_id` 并出现在内容库。

### 状态机

```
PENDING → PROCESSING → UPLOADING → FINALIZING → COMPLETED
   │          │            │            │
   └──────────┴────┬───────┴────────────┴──→ FAILED --retry--> PENDING
                CANCELLED（仅 PENDING 可取消）
```

- `PROCESSING`：ffmpeg 转码/缩略图（对应现有 `ingester.Process`）
- `UPLOADING`：K 副本上传（对应 `uploadAllBlobs`），进度粒度 = 已完成 blob 数/总 blob 数
- `FINALIZING`：PG 事务 + 事件发布
- 终态：`COMPLETED`（带 content_id）/ `FAILED`（带 error + failed_stage）/ `CANCELLED`

### 数据模型变更

```sql
CREATE TABLE ingest_task (
    task_id       UUID PRIMARY KEY,
    owner_id      UUID REFERENCES app_user(user_id),
    content_type  TEXT NOT NULL,
    content_id    UUID,                    -- 完成后回填
    status        TEXT NOT NULL,           -- 状态机枚举
    params        JSONB,                   -- {dash_bitrates, dash_seg_duration, image_thumbnail_sizes}
    file_name     TEXT,
    file_bytes    BIGINT,
    staged_path   TEXT,                    -- 暂存文件路径（处理前）
    total_blobs   INT,
    done_blobs    INT,
    failed_stage  TEXT,
    error         TEXT,
    retry_of      UUID,                    -- 重试链
    created_at    TIMESTAMPTZ DEFAULT now(),
    updated_at    TIMESTAMPTZ DEFAULT now(),
    finished_at   TIMESTAMPTZ
);
CREATE INDEX idx_ingest_task_owner ON ingest_task(owner_id, created_at DESC);
```

### 交互流程

1. 客户端 `POST /v1/tasks`（multipart，同现有 `/ingest/{content_type}` 的字段）→ 服务端落盘暂存 → 建行 `PENDING` → 立即返回 `{task_id}`。
2. 后台 worker pool（复用现有 `IngestPipeline`，在五个阶段埋点写库）取任务执行，阶段推进更新 `status/done_blobs`。
3. 客户端轮询 `GET /v1/tasks/{id}`（建议 2s）或订阅 SSE。
4. `FAILED` → 客户端 `POST /v1/tasks/{id}/retry` → 新建任务行（`retry_of` 指向原任务，复用 `staged_path`，跳过已完成的阶段——v1 可简单从头重来，转码产物缓存为后续优化）。
5. `COMPLETED` → `content_id` 回填，内容库可见。

### API 契约

```
POST   /v1/tasks                 multipart: file + metadata + content_id? → {task_id}   （202 Accepted）
GET    /v1/tasks?status=&page=   列表（按 owner 过滤；admin 全量）→ [{task_id, status, progress, ...}]
GET    /v1/tasks/{id}            详情：状态机位置、done_blobs/total_blobs、逐 blob 副本结果、error、耗时
POST   /v1/tasks/{id}/retry      → {task_id}（新任务）
DELETE /v1/tasks/{id}            取消 PENDING；其他状态 409
GET    /v1/tasks/{id}/events     （可选）SSE 推送状态变更
```

### 与现有组件的交互
- `IngestPipeline.Ingest()` 拆出阶段回调（`OnStage(stage, progress)`），worker pool 复用不变；现有 `POST /ingest/{content_type}` **保留为同步兼容入口**，内部改为提交任务并阻塞等待终态（行为不变，避免破坏现有调用方）。
- 并发上限沿用现有 worker 资源约束（转码是 CPU 密集，pool size 建议 = CPU 核数/2，可配置）。
- 暂存目录容量：需监控 `work_dir` 磁盘，暂存文件在任务终态后清理（FAILED 保留至重试或 24h TTL）。

### 验收标准
- 提交 1GB 视频，HTTP 响应 < 1s 返回 task_id；轮询可见状态依次推进；COMPLETED 后 `GET /v1/contents/{content_id}` 可查。
- 杀掉 worker 进程重启后，PROCESSING/UPLOADING 中的任务标记为 FAILED（可重试），不产生元数据半提交（PG 事务保证，已有）。

---

## F3. 内容访问层（P1，业务主闭环）

### 闭环定义
给定 `content_id`，DASH 播放器能拿到 MPD 并逐段拉到字节流；图片内容能按角色/尺寸取到图。界面一的"试播"与 L1 客户端都走这条链路。**这是"上传→可播放"主闭环的最后一块。**

### 关键设计决策（推荐方案 A）

现有数据已足够：`content.type_metadata` 存 MPD XML；`content_blob` 的 `role`（init/media）+ `business_meta`（`representation_id`、`segment_number`）能把"第 N 段某码率"解析到 `blob_hash`；edge-node 已有 `GET /blob/{hash}`。**缺的是中间的解析与跳转服务。**

- **方案 A（推荐）：解析 + 302。** 新增 content-access 服务（可先在 control-plane 内实现为模块）：MPD 动态生成时把 SegmentURL 模板指向自己，段请求到来时查 `content_blob` 得 `blob_hash`，302 到 edge-node `/blob/{hash}`。edge-node 零改动。
- 方案 B：MPD 直写 edge-node SegmentURL。少一跳，但 MPD 与节点拓扑耦合，后续接 DNS 调度时返工。

### API 契约

```
GET /v1/contents/{id}/mpd
    → 200 application/dash+xml
    基于 PG 中的 MPD XML 重写 SegmentURL / BaseURL 为 /v1/contents/{id}/seg/{representation}/{number}

GET /v1/contents/{id}/seg/{representation_id}/{number}
    → 查 content_blob (role=media, business_meta 匹配) → blob_hash
    → 302 Location: http://{edge-node}/blob/{blob_hash}
    → 404 段不存在 / 410 内容已删除

GET /v1/contents/{id}/init/{representation_id}
    → 同上（role=init）

GET /v1/contents/{id}/assets/{role}?sort_order=
    → 图片等非 DASH 内容的 blob 302（role=original/thumbnail，sort_order=宽度）
```

- edge-node 选择：v1 直接 302 到任一健康 edge-node（从 NODE_STATUS_REPORT 聚合里选）；后续接 DNS+302 调度（`internal/node/routing/scheduler.go` 的规划功能）时在此处替换选择逻辑。
- 界面一试播：前端用 dash.js 指向 `/v1/contents/{id}/mpd` 即可，无需特殊处理。

### 与现有组件的交互
- 只读 PG（content / content_blob / blob）；不改 ingest 产物。
- 内容删除（软删）后 MPD/段请求返回 410——与界面一的删除语义衔接。
- 鉴权：v1 内网无鉴权；若内容需私有，后续在 content-access 加 UserToken 校验（F1 已就位）。

### 验收标准
- 用 dash.js 打开任一 `dash_video` 内容的 MPD URL，init + 前 N 段 200（prefix 命中），全片可拖进度条播放。
- 图片内容 `/assets/thumbnail?sort_order=200` 302 到可访问的 blob。
- 删除后的 content 请求返回 410。

---

## M1. 管理查询 API —— 控制面只读视图（P0）

### 闭环定义
管理员/只读用户能看到系统全貌：节点、账号、内容热度、仪表盘聚合。全部为**只读**，数据均已存在（PG + PinOrchestrator 内存 + Prometheus），纯 API 化。

### API 契约（全部挂 F1 鉴权中间件后，admin 全量、operator 按 node_owner 过滤）

```
GET /v1/admin/overview
    → {nodes: {total, healthy, l4}, accounts: {total, by_state},
       slo: {cache_hit_rate, backhaul_success_rate, ttfb_p95},   -- 来自 Prometheus 即时查询
       alerts: [...]}

GET /v1/admin/nodes?healthy=&capability=
    → [{peer_id, capabilities, prefix_space, warm_space, healthy, last_update, owner_label}]
    数据源：PinOrchestrator nodeSpaces 内存（NODE_STATUS_REPORT 聚合），join node_owner

GET /v1/admin/nodes/{peer_id}
    → 详情 + 最近 N 条状态上报历史（如需历史则新增 node_status_history 表，v1 可只给当前值）

GET /v1/admin/accounts?vendor=&state=
    → [{vendor, account_id, enabled, health:{state,latency_ms,error_msg,ban_until,last_check},
        rate_limit, vendor_profile}]（credential 字段永不返回）

GET /v1/admin/contents?sort=popularity&page=
    → [{content_id, content_type, owner, created_at, blob_count, total_bytes,
        replicas_health, window_24h}]  -- join content_popularity + blob_location 聚合
```

### 验收标准
- 仪表盘四个 SLO 指标与 Grafana 同值；节点列表与 `NODE_STATUS_REPORT` 实际上报一致；账号响应体中不出现任何凭据字段。

---

## M2. 节点本地 status API（P0）

### 闭环定义
运营者直连自己的节点，看到身份/JWT/缓存/pin/peer/网络/回源状态；唯一写操作是 pin 重试。

### 前置
- 节点配置新增 `admin_api.listen`（默认 `127.0.0.1:8081`，不对外）与 `admin_api.token`（节点级管理 token，请求头 `X-Admin-Token` 校验）。

### API 契约

```
GET /v1/status     → {peer_id, capabilities, mode(l4|edge), version, uptime,
                      jwt:{exp, refresh_before, last_refresh_success}, healthy}
GET /v1/cache      → {prefix:{total,used,blob_count}, warm:{...}, cold:{...},
                      hit_rate:{prefix,warm}, ttfb_p95}     -- 内存索引/缓存层直读
GET /v1/pins?ready=
    → [{blob_hash, role, size, pinned_at, ready}]           -- pinstore BadgerDB
POST /v1/pins/{hash}/retry → 202（重新触发 fetchPinnedBlob，幂等）
GET /v1/peers      → [{peer_id, capabilities, score, stale, last_seen, addrs}] -- peerstore
GET /v1/network    → {listen_addrs, conn_count, nat:{reachability, relay_active},
                      dht:{mode, routing_table_size}}
GET /v1/backhaul   → L4 限定：{accounts:[{id, health, circuit, inflight, tokens}],
                      linkpool:{size, hit_rate}, bandwidth:{used, capacity}}
```

### 验收标准
- 各端点返回值与节点内部状态一致（可用 `/metrics` 交叉核对）；错误 token 401；非 L4 节点调 `/v1/backhaul` 返回 409 并说明原因。

---

## M3. 管理操作 API（P1）

### 闭环定义
管理员的高频干预动作都能经 HTTP 触发并看到生效反馈。后端能力全部已存在（见闭环验证 §10），纯触发面。

### API 契约

```
# 账号（AccountRegistry 已有 CRUD + 广播）
POST /v1/admin/accounts                 {vendor, account_id, credential, rate_limit?} → 201
PUT  /v1/admin/accounts/{vendor}/{id}   {credential?（留空不改）, enabled?, rate_limit?}
POST /v1/admin/accounts/{vendor}/{id}/ban    {reason?}   → 广播 BAN，节点 MarkBanned
POST /v1/admin/accounts/{vendor}/{id}/unban              → 广播 UNBAN + ForceClose 熔断
PUT  /v1/admin/vendor-profiles/{vendor} {weight, base_latency_ms, bandwidth_mbps}
   反馈：操作返回 202 {effective: "propagating"}；账号健康视图 6-10s 内反映（最终一致语义，UI 文案配合）

# L4 白名单（WhitelistStore.Add/Remove 已存在）
GET    /v1/admin/whitelist              → [peer_id...]
POST   /v1/admin/whitelist              {peer_id}
DELETE /v1/admin/whitelist/{peer_id}    （生效=下次 JWT 续签，≤1h，UI 需明示）

# Pin 干预（PinOrchestrator.SendToNode 已存在）
POST /v1/admin/pin                      {content_id, target_node, blobs?}  → {seq}
POST /v1/admin/unpin                    同上
GET  /v1/admin/pin-plans?page=          → [{seq, target_node, pins, unpins, sent_at}]
   （需给 SyncBroadcaster 加发送记录：ring buffer 已有事件，加查询面即可）

# 审计（JWT auditlog JSON-lines 已存在）
GET  /v1/admin/audit?type=&from=&to=    → [{ts, actor, action, target, detail}]
   （v1 覆盖 JWT 签发日志；M3 操作本身也要写审计：新增 admin_audit 表）
```

### 验收标准
- BAN 一个账号后，10s 内账号健康视图变为 banned，且该账号不再被 SelectForRead 选中。
- 手动 pin 后目标节点 `/v1/pins`（M2）出现对应条目；审计页能查到操作人/时间/目标。

---

## F4. 节点吊销机制（P1）

### 闭环定义
管理员能把一个节点**降级**（摘掉 L4 能力）或**逐出**（网络层不再接纳），并看到生效状态。

### 功能设计

```sql
CREATE TABLE node_revocation (
    peer_id     TEXT PRIMARY KEY,
    mode        TEXT NOT NULL,             -- soft | hard
    reason      TEXT,
    revoked_by  UUID REFERENCES app_user(user_id),
    revoked_at  TIMESTAMPTZ DEFAULT now(),
    lifted_at   TIMESTAMPTZ                -- 解除时间，NULL=生效中
);
```

- **软吊销** = 白名单移除（M3 已有）+ 等 JWT 过期自然降级（≤1h）。仅影响 `l4_backhaul`。
- **硬吊销**：
  1. `POST /v1/node/jwt` 签发前查 `node_revocation`，命中（hard 且 lifted_at IS NULL）→ 403 `{"error":"node revoked"}`；
  2. 新增控制通道事件 `NODE_REVOKED`（types.go 加常量）广播，节点侧 gater 将该 peer 标 stale + 断开现有连接（复用已有 stale 拒绝路径 `InterceptSecured`）；
  3. 当前 JWT 最长存活 1h，硬吊销最坏 1h 内完全生效，事件通道内秒级。

```
POST   /v1/admin/nodes/{peer_id}/revoke   {mode: soft|hard, reason} → 202
DELETE /v1/admin/nodes/{peer_id}/revoke   解除（lift）
GET    /v1/admin/nodes/{peer_id}/revocation → 当前状态
```

### 验收标准
- 硬吊销后：该 peer 换 JWT 得 403；在线节点 1min 内与其断连；解除后恢复正常。

---

## F5. 副本补齐 worker（P2）

### 闭环定义
任何 blob 的可用副本数低于目标 K 时，系统自动补齐；管理员能看到待补齐/已补齐/失败清单，也能手动触发。

### 功能设计

- **检测**：周期任务（如 10min）扫 `blob_location_v2`，按 `blob_hash` 聚合，副本数 < K（K 取 ingest 冗余度配置，v1=2）且无 `deleted_at` 的进入队列。
- **修复**：从任一健康副本所在后端拉流（Driver.Get/GetLink）→ `SelectK` 排除已有后端选新目标 → Put → 写 `blob_location_v2` 新行。复用 ingest 的账号池/熔断/限流。
- **状态机**：`PENDING → REPAIRING → DONE / FAILED(attempts++, 指数退避，5 次后 DEAD)`。

```sql
CREATE TABLE repair_task (
    blob_hash   TEXT PRIMARY KEY,
    need        INT NOT NULL,              -- 还需几个副本
    status      TEXT NOT NULL,
    attempts    INT DEFAULT 0,
    next_retry  TIMESTAMPTZ,
    error       TEXT,
    created_at  TIMESTAMPTZ DEFAULT now(),
    updated_at  TIMESTAMPTZ DEFAULT now()
);
```

```
GET  /v1/admin/repairs?status=           → 队列视图
POST /v1/admin/repairs/{blob_hash}       → 手动触发/重置 DEAD
POST /v1/contents/{id}/repair            → 对内容内所有不足 K 的 blob 批量建任务
指标：repair_pending / repair_done_total / repair_failed_total
```

### 验收标准
- 人为制造 1/2 副本的 blob，一个扫描周期内进入队列并最终回到 2/2；界面一副本健康度同步变绿。

---

## F6. PolicyController 权重链路（P2）

### 闭环定义
各 Backend 用量被采集 → 控制面算出 ReadWeight/UploadWeight/Writable → 下发节点 → 回源与上传选择实际使用权重；管理员能看到数值曲线并做阈值/手动干预。

### 功能设计（对应 docs/policy）

1. **事件常量**：types.go 新增 `BACKEND_WEIGHT_UPDATE`，载荷 `{backend_id, read_weight, upload_weight, writable, ttl}`。
2. **用量上报**：L4 节点 30s 经控制通道上报 `BackendUsage`（请求数/读取字节/存储量/封号风险），新增事件 `BACKEND_USAGE_REPORT`（或并入 NodeStatusReport 扩展字段，二选一，建议独立事件保持单一职责）。
3. **PolicyController**（控制面新模块）：10s 重算，按 docs/policy 的阈值配置产出权重 → SyncBroadcaster 增量下发。
4. **节点侧**：`AccountPool.SelectForRead` 的 `score = load / read_weight` 中 read_weight 改读下发值（本地缓存，TTL 30s，无下发时用静态 VendorProfile 兜底）。
5. **API**：`GET /v1/admin/backends`（用量+权重视图）、`PUT /v1/admin/backends/{id}/weight`（手动覆盖，带过期）、`GET/PUT /v1/admin/policy/thresholds`。

### 验收标准
- 修改阈值后一个重算周期内权重变化下发到节点；人为抬高某账号用量可观测到其被选率下降；手动覆盖优先于自动计算。

---

## 附：本迭代不做 / 注意事项

1. **非 L4 的 L4 流式回退（`L4Fetcher`）**：属 edge-node 数据面核心功能而非管理面，建议与 F3 内容访问层一并立项评估（F3 的 302 会放大非 L4 节点的 404 面）。
2. **哈希环路由 / DNS+302 接入**：F3 v1 用"任一健康节点"兜底，接入调度器是 F3 的后续增强。
3. **分片/断点续传上传**：F2 v1 为整文件 multipart（10GB 上限沿用）；若 UI 评审认为大文件体验不可接受，再单独立项（staged 上传会话 + 分片合并）。
4. 所有新 API 统一：`Authorization` 头、错误体 `{"error": "..."}`、列表分页 `page/page_size`、时间 RFC3339。
5. 所有新管理操作必须写 `admin_audit`（操作人/动作/目标/参数/时间）——在 M3 落地时一并建表，F4/F5/F6 复用。
