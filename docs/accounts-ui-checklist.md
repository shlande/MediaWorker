# accounts.html 调整清单（动态表单改版）

> 配套：`docs/vendor-account-params.md`（字段基线）、`docs/account-backend-adjustments.md`（接口）。
> 标记：【删】移除 /【改】修改 /【新】新增 /【留】保持。页面其他既有结论（熔断器列删、存储用量列删、VendorProfile 只读化）见 `ui-adjustments.md` §2-accounts，仍有效。

---

## 1. 抽屉表单（核心改版）

### 1.1 凭据 JSON textarea ——【删】

当前 `<textarea id="ad-cred" placeholder='留空 = 不修改。当前值已掩码:{"cookies":"••••••",...}'>` 整块删除，由 §1.3 的动态字段区替代。UI 任何场景不再拼裸 JSON。

### 1.2 添加账号流程 ——【改】（当前是 bug）

当前 `add-acct.onclick = () => openAcct(0)` 直接打开第一个账号的数据。改为：
1. 点「+ 添加账号」→ 先选厂商（五厂商卡片或 select 置顶的空白抽屉）；
2. 选定后渲染该厂商的**空白**动态字段区（account_id 可写）；
3. 编辑态进入时 account_id 与 vendor 锁定（主键不可改，改主键=删旧建新）。

### 1.3 动态字段区 ——【新】

厂商切换时整组替换。字段规格（与后端校验 B4 同源）：

| vendor | 字段（顺序即渲染序） |
|---|---|
| baidu | account_id*、client_id(AppKey)*、client_secret*(密码框)、refresh_token*(密码框)、redirect_uri(选填) |
| onedrive | account_id*、client_id*、client_secret*(密码框)、refresh_token*(密码框)、redirect_uri*、region*(下拉：全球/世纪互联/US Gov/德国 ↔ global/cn/us/de) |
| aliyundrive | account_id*、refresh_token*(密码框)、client_id(选填，自建应用)、client_secret(选填，与 client_id 成对校验) |
| 115 | account_id*、cookies*(kv-rows，预置 UID/CID/SEID 三行空值) |
| quark | account_id*、cookies*(kv-rows) |

- **mock 三厂商提示条**：115/quark/aliyundrive 字段区顶部加 info 条「该厂商驱动未实现，保存后暂不参与上传/回源，连接测试不可用（501）」。
- **限流联动**：字段区下方限流三元组（qps/burst/并发）的 placeholder 随厂商联动显示驱动默认值（baidu 2/4/8、onedrive 10/20/16、115 1/2/5、quark 0.5/1/5、aliyundrive 5/10/10），留空=用默认。
- **region 说明**：onedrive 的 region 决定 token 与 API host，选错=授权失败；在 label 旁加「?」提示。

### 1.4 新控件 ——【新】

| 控件 | 用途 | 规格 |
|---|---|---|
| password 输入 | client_secret / refresh_token / cookies 值 | 默认掩码，右侧眼睛图标切换显隐；编辑态显示"已设置 ·•••••"占位 |
| select | region、厂商选择 | — |
| kv-rows | cookies | 每行「键 input + 值 password input + 删除钮」，底部「+ 添加一行」；键名校验 `^[A-Za-z0-9_]+$`，重复键高亮 |
| field error 行内提示 | 全部字段 | 红字置于字段下方，来自 400 `field_errors` 或前端预校验 |
| 测试连接按钮 | 抽屉操作区 | 见 §1.6 |

### 1.5 编辑态的"留空=不修改"语义 ——【改】

当前只有 placeholder 一句提示。改为逐字段状态：
- 敏感字段（client_secret/refresh_token/cookies 值）编辑态显示占位「已设置，留空保持不变」；聚焦清空占位；提交时**未触碰的字段不进 body**。
- cookies 编辑态：先显示 `N 个键已设置（UID、CID…）`，点「修改」才展开 kv-rows；**展开即视为整体替换**（kv 不做键级合并），并在展开处提示此语义。
- 非敏感字段（region、redirect_uri、限流、enabled）正常回显可改。

### 1.6 测试连接 ——【新】

抽屉操作区加「测试连接」次按钮（保存按钮左侧）：
- 点击 → 收集当前表单 auth 字段 POST `/v1/admin/accounts/test`（草稿模式；已存账号未改动凭据时用已存模式）；
- 按钮进入 testing 态（spinner + 禁用，超时 15s）；
- 结果块渲染在按钮下方：成功=绿色「healthy · 412ms」；失败=红色「degraded · invalid_grant: refresh_token expired」（直接展示后端 error_msg）；501=灰色「该厂商驱动未实现」。
- **保存不强制要求测试通过**，但测试失败的账号保存时弹一次确认（"凭据测试未通过，仍要保存吗？"）。

### 1.7 校验与错误呈现 ——【新】

- 前端预校验（与 B4 同源）：必填、URL 格式、region 枚举、kv 键名/重复键、限流数值范围、account_id 正则 `^[a-zA-Z0-9_-]{2,64}$`。
- 服务端 400 的 `field_errors` 逐字段回填；409 账号已存在单独 toast。
- 校验失败时「保存并下发」不发起请求，第一个错误字段聚焦。

---

## 2. 列表页

| 项 | 处理 | 说明 |
|---|---|---|
| 「熔断器」列 | 【删】（沿用 ui-adjustments 结论） | 节点本地语义，CP 聚合语义错误；本地页已覆盖 |
| 「存储用量」列 | 【删】（沿用） | 无用量采集管线 |
| 凭据列（新增） | 【新】 | 窄列显示 `credential_meta`：OAuth2 显示「OAuth2 · 已配置/缺 refresh_token」，Cookie 系显示「Cookies · 3 键」；残缺配置标黄（如只有 client_id 没有 secret） |
| region 列（onedrive） | 【新·可选】 | onedrive 行内 chip 显示 region；或并入「厂商」列 `onedrive · cn` |
| 启用开关 | 【留】 | 语义不变（快照过滤） |
| 操作列 | 【改】 | 「编辑」之外加「测试」（已存模式连接测试，结果 toast）；危险操作（封禁/熔断）保持在抽屉内，不进列表行 |

---

## 3. VendorProfile 区

【降】（沿用 ui-adjustments 结论）：改为**只读表格 + 说明条**「节点以本地 YAML 配置为准，此处为控制面记录值；修改需同步节点配置」。「保存修改」按钮删除，避免产生"改了会生效"的误解。（若后续后端加 VENDOR_PROFILE_UPDATE 事件，再恢复编辑态。）

---

## 4. 交互流程定稿

### 4.1 添加账号
选厂商 → 空白动态表单（限流 placeholder 联动）→（可选）测试连接 → 保存 → 201 → toast「已创建，快照周期（≤60s）后全网可见」→ 列表插入行（健康态显示"待首次探测"，而非假装 healthy）。

### 4.2 编辑账号
点行/编辑 → 抽屉：基本信息回显（vendor/account_id 锁定）→ 凭据区按 §1.5 掩码语义 → 运行状态只读区保留（健康/错误/延迟）→ 保存 → 202「已下发，待生效（6–10s 传播）」。

### 4.3 凭据轮换
抽屉内「轮换凭据」→ 弹层只渲染该厂商的 auth 字段（其余不出现）→ 测试连接（新凭据）→ 确认 → PUT 仅含 auth → toast「CREDENTIAL_UPDATE 已广播」。
**与现在不同**：轮换后旧凭据失效时机 = 快照周期，文案从"旧凭据在快照周期后失效"保留。

### 4.4 封禁/熔断
保留现有二次确认（confirmDanger 组件）不变。

---

## 5. 文案与空态

- 「待首次探测」：新建账号的健康态在首个 30s 探测周期前的真实空态，不要默认 healthy。
- 「凭据不完整」：credential_meta 缺关键件（has_refresh_token=false）时，健康列显示灰条「凭据不完整，不参与调度」。
- quark 行固定提示：「下载链 IP 绑定，链接不可跨节点复用」（info 级，不告警）。
- 全部"已下发，待生效"toast 沿用 `MW.dispatched` 组件。

---

## 6. 改动清单速查（给前端执行）

| # | 位置 | 动作 |
|---|---|---|
| 1 | `#ad-cred` textarea | 删除，替换为动态字段区容器 `#ad-auth` |
| 2 | `add-acct` 点击 | 改为厂商选择流程，不再 `openAcct(0)` |
| 3 | 抽屉 | 新增：厂商选择态、动态字段渲染器（按 §1.3 表）、password/select/kv-rows 三种控件、字段级 error 行 |
| 4 | 抽屉操作区 | 新增「测试连接」按钮 + 结果块 |
| 5 | 敏感字段 | 实现"未触碰不进 body"提交逻辑；cookies 整体替换语义 + 提示 |
| 6 | 列表 | 删熔断器列、删存储用量列、新增凭据状态列、操作列加「测试」 |
| 7 | VendorProfile 区 | 表格只读化 + 说明条，删「保存修改」按钮 |
| 8 | 健康列 | 新增「待首次探测」「凭据不完整」两种空态 |
| 9 | 编辑态 | vendor/account_id 锁定；运行状态区保留只读 |
| 10 | mock 厂商 | 字段区顶部"驱动未实现"info 条；测试按钮置 501 态 |
