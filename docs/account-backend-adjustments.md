# 账号管理 — 后端接口调整

> 配套：`docs/vendor-account-params.md`（参数清单）、`docs/accounts-ui-checklist.md`（UI 调整清单）。
> 目标：消灭"凭据裸 JSON 手填"，支持按厂商的动态表单提交；同时修复凭据结构三处形状不一致（`types.Credential` 装不下 OAuth2 四要素），为账号快照下发链路补契约。

---

## B1. 凭据结构归一化（核心变更）

### 现状问题
| 载体 | 形状 | 缺口 |
|---|---|---|
| 节点/Worker YAML `CloudAccountConfig` | client_id/client_secret/refresh_token/redirect_uri/region | — |
| 下发链路 `types.Credential` | cookies/access_token/refresh_token/token_expire | **装不下 client_id/secret/redirect_uri/region，快照账号无法构建真实驱动** |
| CP `AccountInfo` | vendor/account_id/credential/rate_limit/vendor_profile/enabled | 随 Credential 受限 |

### 调整：静态授权材料与动态令牌分离

```sql
-- 迁移（新列可空，存量行不受影响；ingest-worker 当前走 YAML，PG 中无生产凭据，迁移成本≈0）
ALTER TABLE cloud_account ADD COLUMN client_config JSONB;
```

```go
// internal/types/types.go 新增：静态授权材料（管理员维护，经快照下发）
type ClientConfig struct {
    ClientID     string `json:"client_id,omitempty"`
    ClientSecret string `json:"client_secret,omitempty"`
    RedirectURI  string `json:"redirect_uri,omitempty"`
    Region       string `json:"region,omitempty"`     // onedrive: global|cn|us|de
}

// Credential 收窄为"秘密材料"：refresh_token 与 cookies
// access_token / token_expire 字段保留但标记 deprecated——TokenManager 内存管理，本就不落库
type Credential struct {
    Cookies      map[string]string `json:"cookies,omitempty"`
    RefreshToken string            `json:"refresh_token,omitempty"`
    AccessToken  string            `json:"access_token,omitempty"`  // deprecated
    TokenExpire  time.Time         `json:"token_expire,omitempty"`  // deprecated
}

// accountregistry.AccountInfo 增加 ClientConfig，快照 []AccountInfo 自动携带
type AccountInfo struct {
    Vendor        types.Vendor          `json:"vendor"`
    AccountID     string                `json:"account_id"`
    Credential    types.Credential      `json:"credential"`      // cookies / refresh_token
    ClientConfig  types.ClientConfig    `json:"client_config"`   // 新增
    RateLimitCfg  types.RateLimitConfig `json:"rate_limit_config"`
    VendorProfile types.VendorProfile   `json:"vendor_profile"`
    Enabled       bool                  `json:"enabled"`
}
```

- 节点侧未来从快照构建驱动时，`ClientConfig + Credential` = `CloudAccountConfig` 的全集，`accountpool.BuildFromConfig` 路径可直接复用（本次只定契约，节点消费链属后续工作）。
- Cookie 系厂商（115/quark）：`client_config` 为空，`credential.cookies` 承载。

---

## B2. 管理 API（结构化 CRUD）

统一挂 F1 鉴权中间件；所有写操作写 `admin_audit`；凭据变更复用 `registry.OnCredentialChange` → CREDENTIAL_UPDATE 广播（已有）。

### 读取（掩码）

```
GET /v1/admin/accounts?vendor=&state=
→ [{
    "vendor": "baidu", "account_id": "mw_bak_01", "enabled": true,
    "health": {"state": "healthy", "latency_ms": 342, "error_msg": "", "ban_until": null, "last_check": "..."},
    "rate_limit": {"qps": 2, "burst": 4, "concurrent_limit": 8},
    "vendor_profile": {"weight": 2.0, "base_latency_ms": 200, "bandwidth_mbps": 80},
    "credential_meta": {                  -- 只有"存在性"元数据，永不返回秘密值
      "auth_type": "oauth2",              -- oauth2 | cookie
      "has_client_secret": true,
      "has_refresh_token": true,
      "region": "",                        -- onedrive 才有
      "cookie_keys": []                    -- cookie 系厂商返回键名列表
    }
  }]
```

### 创建

```
POST /v1/admin/accounts
{
  "vendor": "onedrive", "account_id": "mw_od_01", "enabled": true,
  "rate_limit":     {"qps": 10, "burst": 20, "concurrent_limit": 16},   // 可选，缺省=厂商驱动默认
  "vendor_profile": {"weight": 2.0, "base_latency_ms": 80, "bandwidth_mbps": 40}, // 可选
  "auth": {                       // 按厂商 union，见 B4 校验表
    "client_id": "...", "client_secret": "...", "refresh_token": "...",
    "redirect_uri": "https://...", "region": "cn"
  }
}
→ 201 {vendor, account_id}
→ 400 {"error":"...", "field_errors":{"refresh_token":"required","region":"must be one of global|cn|us|de"}}
→ 409 {"error":"account exists"}
```

服务端动作：按 B4 校验 → 拆分为 `credential`（refresh_token/cookies）+ `client_config`（其余）→ `CreateAccount`（registry 扩展为同时写两列）。

### 更新（含凭据轮换）

```
PUT /v1/admin/accounts/{vendor}/{account_id}
{ "enabled"?, "rate_limit"?, "vendor_profile"?, "auth"? }
```

- `auth` 内敏感字段**缺省或空 = 不修改**；cookies 若出现则**整体替换**（kv 语义，不做键级合并）；显式 `region` 修改会同时重写 client_config。
- 凭据任何字段变化 → CREDENTIAL_UPDATE 广播；`enabled=false` → 下个 ACCOUNT_SNAPSHOT 周期（≤60s）从快照过滤（E8，registry emitSnapshot 已经只查 enabled=true，天然满足）。
- `POST /v1/admin/accounts/{vendor}/{id}/rotate` 可保留为便捷入口，内部等同 PUT 仅含 auth。

### 表单 Schema（可选端点）

```
GET /v1/admin/vendors/form-schema → vendor-account-params.md §5 的 JSON
```
v1 允许前端内置；提供端点的好处是 115/quark/aliyundrive 驱动定稿后 UI 零改动跟随。

---

## B3. 连接测试端点（录入即时反馈）

```
POST /v1/admin/accounts/test
  a) 草稿模式: {"vendor":"baidu", "auth":{...}}          — 未保存的表单内容
  b) 已存模式: {"vendor":"baidu", "account_id":"mw_01"}  — 用库内凭据
→ 200 {"state":"healthy","latency_ms":412}
→ 422 {"state":"degraded","error_msg":"auth: token error ... (invalid_grant)"}
→ 501 {"error":"driver not implemented","vendor":"quark"}   -- mock 厂商
```

- 实现：CP 临时构建 driver + TokenManager（`accountpool.BuildFromConfig` 同路径，单账号），调用 `driver.HealthCheck`（baidu/onedrive 已实现；115/quark/aliyundrive 返回 501）。
- 注意职责边界：docs 职责矩阵中 CP "不部署 driver"——此端点仅用于管理面测试调用，不进回源热路径，可接受；实现时建议包一层 `accounttester`，不把 driver 引入 CP 数据面。
- 失败信息直接回传 driver 的 error_msg（对排查"client_secret 错了还是 refresh_token 失效"至关重要，这正是手填 JSON 时代最痛的点）。

---

## B4. 服务端校验规则（与 UI 同源）

| vendor | auth 必填 | 格式 |
|---|---|---|
| baidu | client_id, client_secret, refresh_token | redirect_uri 若填必须合法 URL |
| onedrive | client_id, client_secret, refresh_token, redirect_uri, region | region ∈ {global,cn,us,de} |
| aliyundrive | refresh_token | client_id/client_secret 成对出现 |
| 115 | cookies（≥1 键） | 键名 `^[A-Za-z0-9_]+$`；缺 UID/CID/SEID 给 warning 不阻断（驱动定稿后收紧） |
| quark | cookies（≥1 键） | 键名同上 |

公共：
- `account_id`：`^[a-zA-Z0-9_-]{2,64}$`，(vendor, account_id) 冲突 → 409。
- `rate_limit`：qps 0.1–100，burst 1–100，concurrent 1–64；缺省用驱动默认（baidu 2/4/8、onedrive 10/20/16、115 1/2/5、quark 0.5/1/5、aliyundrive 5/10/10）。
- 错误响应统一 `{"error", "field_errors":{...}}`，UI 逐字段定位。

---

## B5. Registry 变更清单

| 变更 | 位置 | 说明 |
|---|---|---|
| `CreateAccount` 写 `client_config` 列 | `accountregistry/registry.go` | 与 credential 同事务插入 |
| 新增 `UpdateClientConfig` | 同上 | PUT 的 auth 变更拆两路更新 + 一次 CREDENTIAL_UPDATE 广播 |
| `ListByVendor` 读出 client_config | 同上 | 快照自动携带 |
| `AccountInfo` 加字段 | 同上 + `types.go` | 见 B1 |
| 新增 `SetEnabled`（可选） | 同上 | Revoke 已有（置 false）；补一个 enable=true 的恢复入口 |

## B6. 兼容与影响面

- `accountpool.BuildFromConfig`（ingest-worker/janitor 走 YAML）：**不动**。
- `types.Credential` 收窄：检查引用点（dataplane、healthcheck、node syncbroadcaster 客户端）——当前这些链路未接线，改动面小；保留 access_token 字段做兼容。
- 节点从快照构建驱动的 `BuildFromSnapshot([]AccountInfo)`：本次只定契约（B1 字段齐备），实现属"账号分发链路"后续工作，与 edge-node dataPlane 接线同批。
- 安全：GET 永不输出秘密；秘密只在 POST/PUT 入站；传输依赖部署层 TLS（内网）；admin_audit 记录"凭据已变更"但**不记录秘密内容**。

## B7. 实施批次建议

| 批 | 内容 | 依赖 |
|---|---|---|
| 1 | B1 结构迁移 + B2 CRUD（含校验 B4、掩码）+ Registry 变更 B5 | F1 鉴权（可先用静态 admin token 过渡） |
| 2 | B3 连接测试（baidu/onedrive） | 批 1 |
| 3 | form-schema 端点、115/quark/aliyundrive 真实驱动落地后收紧校验 | 驱动实现 |
