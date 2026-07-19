# UI 调整文档

> 对照后端实际能力（代码核查，非文档口径）逐页审查 `ui/` 下 10 个页面，给出删除/调整/降级建议。
> 处理标记：**删**=无数据源且成本高，删除；**改**=数据口径或语义有误，必须修正；**降**=成本高，降级为更便宜的形态；**留**=可实现，对应接口见 `ui-api-requirements.md`。
> 总体结论：UI 与后端对齐度良好（G1/G4/G5/G6 已正确标注），但有 **4 处必改** 与一组成本项需要收敛。

---

## 1. 必改项（口径错误 / 无数据源）

| # | 位置 | 问题 | 处理 | 说明 |
|---|---|---|---|---|
| 1 | edge-node.html「JWT 生命线」、nodes.html mock 的 JWT 列 | **"29 天后过期 / 有效期 30 天"与后端矛盾** | **改** | 节点 JWT 有效期为 **1 小时**（`exp=iat+3600`，`refresh_before=300s`）。大数字倒计时应改为 **≤1h** 口径："NN 分钟后过期"；保留"上次续签 / 下次续签 / 24h 失败次数"三行（节点本地全有，已符合 1h 语义） |
| 2 | nodes.html「评分」列、dashboard「评分灰名单 ≤-20」计数、edge-node.html「节点评分 31」 | **GossipSub 评分是双向局部视图**：每个节点只知道自己给对端的评分，CP 无聚合源，节点也不知道自己被别人评多少 | **删** | 全网信誉采集属新子系统，不值得为本期建设。CP 侧改为可观测代理信号：上报中断、连接数骤降；edge-node.html 自身评分改为「**灰名单对端数**」（本地 peerstore 有，语义正确）。评分列仅在 edge-network.html 的 peer 列表保留（那里是"我给对端的评分"，语义本就正确） |
| 3 | policy.html 整页 | **用量采集管线在代码中不存在**（无 UsageMetrics/用量上报），PolicyController 不存在，QuotaAllocator 有库但未接线进 main，本地存储 Backend 不存在 | **改**（重构见 §2-policy） | 当前"用量快照=已生效"的标注不成立——整页真实可用的只有 QuotaAllocator 库（且需接线）。按 §2-policy 收敛为单视图或整页"规划中" |
| 4 | accounts.html「存储用量」列 | 同上：无用量采集 | **删** | v1 删除该列；界面一/二期不补，待 F6 权重链路一并建设 |

---

## 2. 逐页调整清单

### dashboard.html

| 元素 | 处理 | 原因 / 建议替代 |
|---|---|---|
| SLO 四卡 | 留 | Prometheus + account_health，见接口文档 §3.1 |
| 「社区节点 74」 | 改 | 无 community 标记。改为「非 L4 节点」计数（capabilities 可判） |
| 「评分灰名单 ≤-20 · 5 节点」条 | 删 | 见必改 #2。替代：「上报异常（>2 周期未见上报）」计数，NODE_STATUS_REPORT 可判 |
| 命中率 24h 趋势 | 留（改数据源） | 前端直查 Prometheus，不经聚合 API |
| 24h 热门 Top8（含 pin 节点数、副本列） | 留 | pin_node_count 需 CP 下发记账（中成本，接口文档 E5）；可先隐藏该列，记账落地后恢复 |
| 当前告警 4 条 | 留（需管线） | 需 Alertmanager webhook → alert_events 表（中成本，E12）；落地前该面板显示"告警管线建设中"空态 |
| PinPlan 近 1h 统计 | 留 | 同 E5 记账 |
| 「JWT 续签连续失败」告警条目 | 降 | CP 无法直接看到节点侧失败。改为「**应续未续**」推断（CP 有签发记录：exp 将尽但未收到新申请即告警），CP 侧可实现 |

### nodes.html

| 元素 | 处理 | 原因 / 建议替代 |
|---|---|---|
| 「评分」列 | 删 | 见必改 #2 |
| 状态细分「续签异常」 | 降 | 改为「JWT NN 分钟后过期 · 未续签」（CP 推断，见 dashboard 同项）；节点自报失败数属 E1 扩展，二期恢复细分 |
| region / 版本 / uptime / 连接数 | 留 | NodeStatusReport 扩展字段（E1，低成本，节点本地全有） |
| 详情·三级缓存含 cold 分区 | 留 | E1 扩展加 cold_space |
| 详情·当前 PinPlan | 留 | 需 CP per-node 下发日志（E5，中）；可先只显示 seq+时间两列 |
| 详情·状态上报历史 | 留 | 新表 node_status_history（E2，中低）；v1 可先显示"近 N 次上报时间"单列 |
| 白名单「加入时间/加入人」 | 留 | 白名单记录扩展元数据（E3，低） |
| 吊销按钮（禁用+说明） | 留 | 已正确对齐 G4，无需改动 |
| mock 数据 JWT「23/29 天后过期」 | 改 | 见必改 #1 |

### accounts.html

| 元素 | 处理 | 原因 / 建议替代 |
|---|---|---|
| 「熔断器」列 | 删（或改） | 熔断器是**节点本地**状态，每个 L4 节点各自一份，CP 单行聚合语义错误。v1 删除该列（熔断状态在 edge-network.html 本地页展示，语义正确）；若管理员需要全局视角，二期改为「熔断 Open 的节点数」聚合（需上报扩展） |
| 「存储用量」列 | 删 | 见必改 #4 |
| 健康状态 + error_msg + ban_until | 留 | account_health 直读 |
| 「延迟（基线/实测）」 | 留 | vendor_profile + account_health.latency_ms |
| 启用开关 | 留 | cloud_account.enabled + 快照过滤（E8，低） |
| 编辑/轮换凭据/封禁 | 留 | AccountRegistry 已有能力 |
| 手动熔断操作 | 留 | 需新事件 CIRCUIT_FORCE_OPEN/CLOSE（E7，中低） |
| VendorProfile 表「保存修改」 | 降 | **CP 改了不传播**——节点从本地 YAML 读画像。v1 改为**只读展示 + 说明文案**（"节点以本地配置为准，修改需同步节点 YAML"）；若要可编辑，需 VENDOR_PROFILE_UPDATE 事件（中成本，二期） |

### content.html

| 元素 | 处理 | 原因 / 建议替代 |
|---|---|---|
| 内容「标题」列 | 留（需小改后端） | ingest metadata 增加可选 title 字段（E4，低）；无标题时回退显示 content_id |
| 副本健康 2/2 · 1/2 · 待清理 | 留 | blob_location 聚合 + content.deleted_at（E4，低） |
| 「pin 节点」列 | 留 | 同 dashboard（E5 记账，可先隐藏） |
| PinPlan 下发记录「确认」列 | 降 | 无逐条 ACK。降级为两态：已下发 / 待节点上报（下发时间 vs 该节点最近一次上报时间比较） |
| 手动 pin/unpin + 按剩余空间过滤节点 | 留 | PinOrchestrator nodeSpaces 直读 |
| 补齐副本按钮（禁用+说明） | 留 | 已正确对齐 G6 |
| 删除内容（软删除 + janitor 说明） | 留 | 需 DELETE API（低成本）；janitor dry-run 说明文案保留 |
| 试播缺口说明 | 留 | 已正确对齐 G1 |

### policy.html（重构）

当前三 tab 的真实可用性：用量快照 ❌ 无采集 / 权重 ❌ 无 PolicyController / 阈值 ❌ 无消费者 / 配额 ⚠️ 库存在但未接线 / 本地存储 ❌ 不存在。建议：

| 元素 | 处理 | 建议 |
|---|---|---|
| Tab1「Backend 用量与权重」 | **删**（或整 tab 置"规划中"横幅，只留表头骨架） | 用量/风险/IOPS/权重/Writable 全部无数据源；"已生效/规划值"二分标注不成立，整 tab 属 G5 |
| `local:staging-01` 本地存储行 | 删 | 本地存储 Backend 在代码中不存在 |
| Tab2「阈值配置」 | 删（或禁用态） | 阈值的唯一消费者是 PolicyController；消费者不存在时保存阈值是写死信 |
| Tab3「配额管理」 | 降 | QuotaAllocator 库存在但未进 main（E11，中成本可接）。v1 只保留三张概览卡（globalQPS / baseShare / 节点数）+ 分配视图（baseShare 列）；**「当前用量/利用率/借用中/借用记录」四块删除**（依赖不存在的用量上报与借用流水） |
| 状态条「quark 0.86 越过 danger 线」 | 改 | 封号风险无数据源。改为引用真实信号：「quark 2 账号 banned（account_health）· 自动降权未接线（G5），需手动熔断」 |

### audit.html

| 元素 | 处理 | 原因 / 建议替代 |
|---|---|---|
| 四类事件 | 留 | JWT=JSON-lines 查询面；手动干预/账号变更/白名单=admin_audit 新表（E9，中低） |
| 「结果」列（含 jwt.renew fail） | 留（需小改后端） | 当前失败签发不落审计，需补记 result（低） |
| 「操作者」列 | 留 | 依赖认证（F1 精简版） |
| 导出 JSON-lines | 留 | 查询面的格式输出（低） |

### edge-node.html（节点本地）

| 元素 | 处理 | 原因 / 建议替代 |
|---|---|---|
| JWT 大数字「29 天」 | 改 | 见必改 #1 |
| 「节点评分 31」 | 改 | 见必改 #2，改为「灰名单对端数」 |
| 三级缓存分区 + blob 数 | 留 | 本地直读 |
| 命中率趋势（三序列 sparkline） | 改（数据源） | 节点不存时序 → 前端直查 Prometheus；或 v1 只显示瞬时值，趋势图删 |
| TTFB 分布 | 改（数据源） | 同上（histogram_quantile） |
| 「逐出活动」明细表 | 删（降） | 缓存逐出无事件记录。v1 删除该卡或改为「近 1h 逐出计数」（加一个计数器，低）；明细表需缓存层事件日志，二期 |
| 「/metrics 暴露 · Prometheus 抓取 正常·8s 前 · 214 序列」 | 改 | 节点不知道自己何时被 scrape。改为：挂载状态（本地可知）+ 本地序列数；「抓取正常」属 Prometheus `up{}`，如需展示放 CP 侧 |
| 「本地告警事件」表 | 降 | 告警规则在 Prometheus/Alertmanager 评估，非节点本地产生。v1 两个选项：(a) 删除该卡；(b) 数据接 alert_events（E12 建成后）按本节点过滤。落地前显示空态，不要 mock 含义 |
| 「重载配置」按钮 | 留（需后端） | 需实现配置重载端点（中低）；未实现前禁用 |
| 「清空 warm/cold」按钮 | 留（需后端） | 需 flush 端点（中低）；二次确认文案已到位 |

### edge-pins.html（节点本地）

| 元素 | 处理 | 原因 / 建议替代 |
|---|---|---|
| content_id 列 + 按内容搜索 | 留（需小改后端） | pinstore PinEntry 加 content_id（E6，低，PinPlan 载荷里有） |
| 「拉取中 61%」进度 | 降 | fetch 进度无追踪。降级为三态：就绪 / 拉取中 / 失败（pinstore state 字段，E6）；百分比二期再议 |
| 失败 err 文本（"回源 404…"） | 留 | pinstore 加 last_error（E6，低） |
| PinPlan 下发历史（接收时间/应用状态） | 留 | 节点本地 ring buffer（E6，低） |
| 重试按钮 | 留 | ApplyPin 幂等已有，加端点即可 |

### edge-network.html（节点本地）

| 元素 | 处理 | 原因 / 建议替代 |
|---|---|---|
| 连接/DHT/GossipSub/监听地址 | 留 | host 自省，低 |
| NAT/relay 卡 | 留 | AutoNAT 状态可取；「DCUtR 成功率 41/53」需加一个计数器（低），未加前显示"—" |
| 「一致性哈希环位置」卡 | 留（加注） | hashring 模块存在可算位置；但**环未接入请求路由**（edge_router 未入 mux）。卡片加一行说明："当前仅用于 peer 拓扑展示，未参与请求路由" |
| Peer 列表（评分/stale） | 留 | peerstore 直读——这是评分语义正确的唯一页面 |
| L4 回源四卡 + 本地账号池 | 留 | metrics + accountpool 本地状态 |
| 非 L4 隐藏 + L4Fetcher 说明 | 留 | 已正确对齐 |

### index.html / mediaworker-admin-portal.html

| 元素 | 处理 | 原因 / 建议替代 |
|---|---|---|
| 侧边栏写死 admin 身份 | 改 | 接 `/v1/auth/me`；**新增登录页**（当前 10 页中缺失，认证落地后必需） |
| 范围说明（界面一后续迭代 / G1 试播缺口） | 留 | 表述准确 |
| index.html 与 mediaworker-admin-portal.html 内容重复 | 改 | 两文件 hero+模块卡几乎一致，保留 index.html 一个入口，另一个删除或改为跳转，避免双份维护 |
| edge-node-monitor.html 与 edge-node.html | 改 | critique.json 标注前者为模板收敛产物；确认哪个是正式页，另一个移出交付目录 |

---

## 3. 建议新增（UI 侧补齐）

1. **登录页**：用户名/密码 → POST /v1/auth/login，失败态与 token 过期跳回（对应接口文档 §2）。
2. **Prometheus/Grafana 入口**：dashboard 与 edge-node 的深度排查跳转链接（技术选型已有 Grafana，UI 目前只有文字提及，无链接位）。
3. **全局限流文案组件**：所有"已下发，待生效"toast 已有（app.js `dispatched`），建议列表页加统一的"数据快照时间"角标（部分页已有 freshness tick，保持即可）。

---

## 4. 调整优先级

| 优先级 | 项 |
|---|---|
| 立即改（与后端矛盾，会误导运营） | 必改 #1 JWT 29 天、必改 #3 policy"已生效"标注、accounts 熔断器列语义、index 重复页 |
| 本期删/降（无数据源，成本不值） | 必改 #2 评分三处、必改 #4 存储用量、policy Tab1/Tab2 + 配额四块、逐出活动明细、拉取进度%、PinPlan"已确认"、本地告警（先空态） |
| 保留（后端低成本可实现） | 见 `ui-api-requirements.md` §7 第一、二批 |
