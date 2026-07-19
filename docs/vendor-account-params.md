# 网盘账号参数清单（按厂商）

> 依据：`internal/storage/driver/`（baidu/onedrive 真实实现，115/quark/aliyundrive 为 mock）、`internal/storage/auth/oauth2.go`、
> `internal/storage/accountpool/`、`internal/controlplane/accountregistry/`、`internal/config/`、`configs/ingest-worker.yaml`、`docs/storage/README.md`。
> 目的：为 control-plane 账号管理的**动态表单**（按厂商切换字段）提供字段级基线，替代当前的"凭据 JSON 手填"。

---

## 1. 结论速览

1. **两种认证模型**：OAuth2（baidu、onedrive，代码已实现）与 Cookie/待定型（115、quark、aliyundrive，驱动为 mock，参数属设计口径）。
2. **当前真正消费账号参数的是 ingest-worker / janitor 的本地 YAML**（`CloudAccountConfig`，8 个字段）；edge-node 的 L4 数据面**尚未接线**（`cmd/edge-node/main.go:476` dataPlane=nil），控制面快照到节点的消费链不存在——账号管理的"系统记录"是 `cloud_account` 表 + `AccountInfo` 快照，但**下游还没人接**。
3. **凭据结构存在三处形状不一致**（详见 §4），动态表单提交前必须由后端统一归一化，UI 不应直接拼 `types.Credential` 的 JSON。

---

## 2. 各厂商参数矩阵

### 2.1 baidu（百度网盘）— OAuth2 · 已实现

代码：`internal/storage/driver/baidu/`（PCS/xpan API）、`auth/oauth2.go`（token endpoint `https://openapi.baidu.com/oauth/2.0/token`）。

| 字段 | 类型 | 必填 | 说明 | 代码证据 |
|---|---|---|---|---|
| `account_id` | string | ✔ | 池内唯一标识（如 `mw_bak_01`），人工命名 | `CloudAccountConfig.AccountID` |
| `client_id` | string | ✔ | 百度开放平台应用 AppKey | `oauth2.go` refresh 表单 |
| `client_secret` | string（敏感） | ✔ | 应用 SecretKey | 同上 |
| `refresh_token` | string（敏感） | ✔ | 用户授权后换得；access_token 由 TokenManager 自动刷新（内存，不落库） | `auth/oauth2.go:167-175` |
| `redirect_uri` | string | 选填 | 百度的 refresh grant 不带该字段（代码仅非空才发） | `oauth2.go:173` |
| `region` | — | ✖ | 百度不使用 | `build.go:82`（仅 OneDrive 用） |

- 限流默认（可被 `rate_limits` 覆盖）：**QPS 2.0 / Burst 4 / 并发 8**（`baidu.go:303`）。
- 健康探测：List root + GetLink 试取（`docs/storage §7.1`）。
- 特殊行为：下载链 **IP 绑定**（`docs/storage §2`）；上传走分块 precreate+superfile2（`baidu/upload.go`）。
- 获取引导：百度开放平台建应用拿 AppKey/SecretKey → 引导用户走授权码流程换 refresh_token。

### 2.2 onedrive — OAuth2 · 已实现（多 region）

代码：`internal/storage/driver/onedrive/`（Graph API v1.0）、`auth/oauth2.go:64-79`。

| 字段 | 类型 | 必填 | 说明 | 代码证据 |
|---|---|---|---|---|
| `account_id` | string | ✔ | 同上 | — |
| `client_id` | string | ✔ | Azure 应用注册 Client ID | `oauth2.go` |
| `client_secret` | string（敏感） | ✔ | Azure Client Secret | 同上 |
| `refresh_token` | string（敏感） | ✔ | OAuth2 授权码换得 | 同上 |
| `redirect_uri` | string | **✔** | OneDrive 的 refresh grant 必须带（代码注释明确 "Required for OneDrive"） | `oauth2.go:36,173` |
| `region` | **枚举** | ✔ | `global / cn / us / de`，决定 token host 与 Graph host（未知值回落 global） | `oauth2.go:64-69`、`onedrive.go:23-28` |

- 限流默认：**QPS 10 / Burst 20 / 并发 16**（`onedrive.go:463`）。
- 健康探测：`GET /me/drive/root/children?top=1`（`docs/storage §7.1`）。
- 获取引导：Azure Portal 注册应用（按 region 选云：全球/世纪互联/US Gov/德国）→ 配置 redirect_uri → 授权码换 refresh_token。

### 2.3 115 — mock · 设计口径（驱动待实现）

代码现状：`driver/mock/mock_115.go`，无真实参数消费。以下为 `docs/storage` 设计口径 + 字段位：

| 字段 | 类型 | 必填 | 说明 |
|---|---|---|---|
| `account_id` | string | ✔ | — |
| `cookies` | **键值对表**（敏感） | ✔（二选一） | `types.Credential.Cookies map[string]string` 就是为此预留（`types.go:106`）；115 网页 API 惯例为 `UID/CID/SEID` 三键 |
| `refresh_token` 等 OAuth2 组合 | string | ✔（二选一） | `docs/storage §4.1` 把 115 归入 OAuth2 组（官方开放平台）；**最终二选一取决于真实驱动实现** |

- 限流默认（mock）：**QPS 1.0 / Burst 2 / 并发 5**（OpenAPI 硬限制口径，`mock_115.go:42`）。
- 健康探测（设计）：List root + GetLink 1 个测试文件，延迟 <2s。
- 表单建议：字段按"键值对动态行"渲染 cookies（添加/删除行），不要把整段 Cookie 字符串塞一个输入框。

### 2.4 quark（夸克）— mock · 设计口径

| 字段 | 类型 | 必填 | 说明 |
|---|---|---|---|
| `account_id` | string | ✔ | — |
| `cookies` | 键值对表（敏感） | ✔ | `docs/storage §4.1` 明确"Cookies // 百度、夸克"（注：该注释的"百度"与代码 OAuth2 实现不符，以代码为准——百度用 OAuth2；夸克 cookie 口径与 AList 实践一致） |

- 限流默认（mock）：**QPS 0.5 / Burst 1 / 并发 5**（风控最严，`mock_115.go:44`）。
- 特殊行为：下载链 **IP 绑定**（`mock_115.go:99` `ipBound = vendor==quark`）——意味着链接不可跨节点复用，表单/详情页应有提示。
- 健康探测（设计）：List root 成功且返回内容。

### 2.5 aliyundrive（阿里云盘）— mock · 设计口径

| 字段 | 类型 | 必填 | 说明 |
|---|---|---|---|
| `account_id` | string | ✔ | — |
| `refresh_token` | string（敏感） | ✔ | `docs/storage §4.1` 归入 OAuth2 组；阿里开放平台 OAuth2 refresh_token 模式 |
| `client_id` / `client_secret` | string | 选填 | 默认公共客户端可省，自建应用时填写（待真实驱动定稿） |

- 限流默认（mock）：**QPS 5.0 / Burst 10 / 并发 10**。
- 健康探测（设计）：token refresh + List root 均成功。

---

## 3. 跨厂商公共字段（动态表单的固定区）

| 字段 | 类型 | 说明 |
|---|---|---|
| `vendor` | 枚举 | 115 / baidu / quark / onedrive / aliyundrive（`types.go:97-101`） |
| `enabled` | bool | `cloud_account.enabled`，吊销=置 false 不删行（`registry.go:118`） |
| `rate_limit` | 三元组 | `qps / burst / concurrent_limit`（`types.RateLimitConfig`，`cloud_account.rate_limit_config` JSONB）；表单应把 §2 各厂商默认值作为 placeholder |
| `vendor_profile` | 三元组 | `weight / base_latency_ms / bandwidth_mbps`（`types.VendorProfile`）；默认：115=3.0/100/50、baidu=2.0/200/80、onedrive=2.0/80/40、aliyundrive=2.5/90/40、quark=1.0/300/30（`docs/storage §4.3`） |

---

## 4. 凭据结构的三处形状不一致（动态表单必须先对齐）

| 载体 | 形状 | 位置 |
|---|---|---|
| 节点/Worker 本地 YAML | `CloudAccountConfig{vendor, account_id, client_id, client_secret, refresh_token, redirect_uri, region, enabled}` | `internal/config/config.go:275` |
| 下发链路 `types.Credential` | `{cookies, access_token, refresh_token, token_expire}` — **没有 client_id/client_secret/redirect_uri/region** | `types.go:105-110` |
| CP 注册表 `AccountInfo` | `{vendor, account_id, credential(types.Credential), rate_limit_config, vendor_profile, enabled}` | `registry.go:31-38` |

**问题**：节点侧构建驱动需要 client_id/client_secret/redirect_uri/region（`accountpool/build.go:87-100`），但 CP 快照的 `types.Credential` 携带不了这四个字段——按当前形状，快照下发的账号**无法构建真实驱动**。这是账号分发链路落地前必须补的结构缺口。

**建议**（供后端对齐，UI 依此设计提交格式）：
- `cloud_account.credential` JSONB 扩展为**按厂商的 union 结构**（或并列加 `client_config` JSONB）：OAuth2 系携带 `{client_id, client_secret, refresh_token, redirect_uri?, region?}`，Cookie 系携带 `{cookies: {...}}`；
- CP 管理 API 接收**结构化字段**（动态表单的直接产物），由后端归一化为厂商 union 存储，**禁止 UI 拼裸 JSON**；
- `access_token`/`token_expire` 不进表单——由 TokenManager 运行时刷新（当前即如此，`oauth2.go` 注释 "Tokens are kept in memory only"）。

---

## 5. 动态表单 Schema 草案（给 UI 的直接输入）

```jsonc
// 建议后端提供 GET /v1/admin/vendors/form-schema（或前端内置，v1 可前端内置）
{
  "baidu": {
    "auth": "oauth2",
    "fields": [
      {"key": "account_id",    "label": "账号标识",   "type": "text",     "required": true,  "placeholder": "mw_bak_01"},
      {"key": "client_id",     "label": "AppKey",     "type": "text",     "required": true},
      {"key": "client_secret", "label": "SecretKey",  "type": "password", "required": true,  "sensitive": true},
      {"key": "refresh_token", "label": "Refresh Token", "type": "password", "required": true, "sensitive": true,
       "help": "百度开放平台授权码流程换得"},
      {"key": "redirect_uri",  "label": "Redirect URI", "type": "text",   "required": false}
    ],
    "defaults": {"rate_limit": {"qps": 2, "burst": 4, "concurrent": 8}},
    "notes": ["下载链 IP 绑定", "access_token 由系统自动刷新"]
  },
  "onedrive": {
    "auth": "oauth2",
    "fields": [
      {"key": "account_id",    "label": "账号标识",   "type": "text",     "required": true},
      {"key": "client_id",     "label": "Client ID",  "type": "text",     "required": true},
      {"key": "client_secret", "label": "Client Secret", "type": "password", "required": true, "sensitive": true},
      {"key": "refresh_token", "label": "Refresh Token", "type": "password", "required": true, "sensitive": true},
      {"key": "redirect_uri",  "label": "Redirect URI", "type": "text",   "required": true},
      {"key": "region",        "label": "区域",        "type": "select",   "required": true,
       "options": [{"value": "global", "label": "全球"}, {"value": "cn", "label": "世纪互联"},
                   {"value": "us", "label": "US Gov"}, {"value": "de", "label": "德国"}]}
    ],
    "defaults": {"rate_limit": {"qps": 10, "burst": 20, "concurrent": 16}}
  },
  "115": {
    "auth": "cookie-or-oauth2-tbd",       // 驱动未实现，schema 先按 cookie 渲染，实现后定稿
    "fields": [
      {"key": "account_id", "label": "账号标识", "type": "text", "required": true},
      {"key": "cookies",    "label": "Cookies",  "type": "kv-rows", "required": true, "sensitive": true,
       "kvHint": [{"key": "UID"}, {"key": "CID"}, {"key": "SEID"}]}
    ],
    "defaults": {"rate_limit": {"qps": 1, "burst": 2, "concurrent": 5}}
  },
  "quark": {
    "auth": "cookie",
    "fields": [
      {"key": "account_id", "label": "账号标识", "type": "text", "required": true},
      {"key": "cookies",    "label": "Cookies",  "type": "kv-rows", "required": true, "sensitive": true}
    ],
    "defaults": {"rate_limit": {"qps": 0.5, "burst": 1, "concurrent": 5}},
    "notes": ["下载链 IP 绑定（链接不可跨节点复用）", "风控最严，限流最低"]
  },
  "aliyundrive": {
    "auth": "oauth2",
    "fields": [
      {"key": "account_id",    "label": "账号标识",   "type": "text", "required": true},
      {"key": "refresh_token", "label": "Refresh Token", "type": "password", "required": true, "sensitive": true},
      {"key": "client_id",     "label": "Client ID（自建应用时）", "type": "text", "required": false},
      {"key": "client_secret", "label": "Client Secret（自建应用时）", "type": "password", "required": false, "sensitive": true}
    ],
    "defaults": {"rate_limit": {"qps": 5, "burst": 10, "concurrent": 10}}
  }
}
```

新控件需求（相对当前 accounts.html 的增量）：`password` 输入（带显隐切换）、`select`、`kv-rows`（键值对动态行增删）、敏感字段"留空=不修改"语义（与现有掩码策略一致）、厂商切换时表单区整组替换 + 限流默认值随厂商联动 placeholder。
