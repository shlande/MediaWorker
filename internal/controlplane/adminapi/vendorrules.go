package adminapi

// vendorrules.go is the SINGLE SOURCE OF TRUTH for per-vendor account rules.
// It backs B4 server-side validation (docs/account-backend-adjustments.md
// 139-153) now and todo 58's form-schema generation later — field metadata
// (auth type, required/optional keys, region enum, rate-limit defaults,
// operator notes) lives here and nowhere else.
//
// Auth material splits per B1/B2: refresh_token + cookies are dynamic secret
// material (types.Credential); client_id/client_secret/redirect_uri/region are
// static client material (types.ClientConfig). ValidateAuth performs that
// split-normalization; OverlayAuthPatch/ApplyAuthPatch implement B2
// partial-update semantics.

import (
	"context"
	"fmt"
	"maps"
	"net/url"
	"regexp"
	"strings"

	"github.com/shlande/mediaworker/internal/controlplane/accountregistry"
	"github.com/shlande/mediaworker/internal/types"
)

// allow: SIZE_OK — the VendorRules literal is a pure data table (the todo 58
// form-schema source); the validators below are short and single-purpose.

// VendorRule describes one vendor's auth-field contract.
type VendorRule struct {
	AuthType         string                // "oauth2" | "cookie"
	RequiredAuth     []string              // auth keys that must be present (non-empty)
	OptionalAuth     []string              // auth keys that may be present
	RegionValues     []string              // allowed region values (empty = region unused)
	DefaultRateLimit types.RateLimitConfig // applied when POST omits rate_limit
	Notes            []string              // operator guidance, surfaced in todo 58 form-schema
}

// VendorRules is the per-vendor rule table (docs/vendor-account-params.md §2,
// docs/account-backend-adjustments.md B4).
var VendorRules = map[types.Vendor]VendorRule{
	types.VendorBaidu: {
		AuthType:         "oauth2",
		RequiredAuth:     []string{"client_id", "client_secret", "refresh_token"},
		OptionalAuth:     []string{"redirect_uri"},
		DefaultRateLimit: types.RateLimitConfig{QPS: 2, Burst: 4, ConcurrentLimit: 8},
		Notes: []string{
			"redirect_uri 选填，若填必须是合法 URL",
			"百度开放平台建应用拿 AppKey/SecretKey，授权码流程换 refresh_token",
			"下载链 IP 绑定",
		},
	},
	types.VendorOneDrive: {
		AuthType:         "oauth2",
		RequiredAuth:     []string{"client_id", "client_secret", "refresh_token", "redirect_uri", "region"},
		RegionValues:     []string{"global", "cn", "us", "de"},
		DefaultRateLimit: types.RateLimitConfig{QPS: 10, Burst: 20, ConcurrentLimit: 16},
		Notes: []string{
			"redirect_uri 必填（OneDrive 的 refresh grant 必须携带）",
			"region 决定 token host 与 Graph host",
			"Azure Portal 按 region 注册应用（全球/世纪互联/US Gov/德国）",
		},
	},
	types.VendorAliyundrive: {
		AuthType:         "oauth2",
		RequiredAuth:     []string{"refresh_token"},
		OptionalAuth:     []string{"client_id", "client_secret"},
		DefaultRateLimit: types.RateLimitConfig{QPS: 5, Burst: 10, ConcurrentLimit: 10},
		Notes: []string{
			"client_id/client_secret 必须成对出现（默认公共客户端可省，自建应用时填写）",
			"阿里开放平台 OAuth2 refresh_token 模式",
		},
	},
	types.Vendor115: {
		AuthType:         "cookie",
		RequiredAuth:     []string{"cookies"},
		DefaultRateLimit: types.RateLimitConfig{QPS: 1, Burst: 2, ConcurrentLimit: 5},
		Notes: []string{
			"cookies 至少 1 个键；115 网页 API 惯例键 UID/CID/SEID（缺失仅告警不阻断）",
			"表单按键值对动态行渲染 cookies，不要塞整段 Cookie 字符串",
		},
	},
	types.VendorQuark: {
		AuthType:         "cookie",
		RequiredAuth:     []string{"cookies"},
		DefaultRateLimit: types.RateLimitConfig{QPS: 0.5, Burst: 1, ConcurrentLimit: 5},
		Notes: []string{
			"cookies 至少 1 个键",
			"下载链 IP 绑定，链接不可跨节点复用",
			"风控最严，默认 QPS 0.5",
		},
	},
}

const (
	vendorEnumHint     = "must be one of 115|baidu|quark|onedrive|aliyundrive"
	accountIDHint      = "must match ^[a-zA-Z0-9_-]{2,64}$"
	vendorProfileNote  = "节点以本地 YAML 为准"
	cookieKeyPatternRe = `^[A-Za-z0-9_]+$`
)

var (
	accountIDPattern = regexp.MustCompile(`^[a-zA-Z0-9_-]{2,64}$`)
	cookieKeyPattern = regexp.MustCompile(cookieKeyPatternRe)
)

// scalarAuthKeys are the non-cookie auth keys, in deterministic check order.
var scalarAuthKeys = []string{"client_id", "client_secret", "refresh_token", "redirect_uri", "region"}

// sensitiveAuthKeys are scalar keys where B2 partial mode treats an empty
// string as "unchanged" (never wipes stored material by accident).
var sensitiveAuthKeys = map[string]bool{"client_secret": true, "refresh_token": true}

// ValidateAccountID enforces the B4 account_id shape.
func ValidateAccountID(id string) error {
	if !accountIDPattern.MatchString(id) {
		return fmt.Errorf("account_id %q: %s", id, accountIDHint)
	}
	return nil
}

// ValidateRateLimit enforces the B4 rate_limit bounds; keys are prefixed
// "rate_limit." so the UI can locate the failing sub-field.
func ValidateRateLimit(rl types.RateLimitConfig) map[string]string {
	fieldErrors := map[string]string{}
	if rl.QPS < 0.1 || rl.QPS > 100 {
		fieldErrors["rate_limit.qps"] = "must be between 0.1 and 100"
	}
	if rl.Burst < 1 || rl.Burst > 100 {
		fieldErrors["rate_limit.burst"] = "must be between 1 and 100"
	}
	if rl.ConcurrentLimit < 1 || rl.ConcurrentLimit > 64 {
		fieldErrors["rate_limit.concurrent"] = "must be between 1 and 64"
	}
	return fieldErrors
}

// ValidateAuth validates the auth object against the vendor's rule and
// split-normalizes it into credential (refresh_token/cookies) + clientConfig
// (client_id/client_secret/redirect_uri/region). An empty-string required
// field counts as missing. Unknown keys are ignored (forward-compatible).
// fieldErrors is empty on success; warnings never block (115 UID/CID/SEID).
func ValidateAuth(vendor types.Vendor, auth map[string]any) (credential types.Credential, clientConfig types.ClientConfig, fieldErrors map[string]string, warnings []string) {
	rule, ok := VendorRules[vendor]
	if !ok {
		return types.Credential{}, types.ClientConfig{}, map[string]string{"vendor": vendorEnumHint}, nil
	}
	fieldErrors = map[string]string{}
	required := map[string]bool{}
	for _, k := range rule.RequiredAuth {
		required[k] = true
	}
	allowed := map[string]bool{}
	for _, k := range rule.OptionalAuth {
		allowed[k] = true
	}

	values := map[string]string{}
	markMissing := func(key string) {
		// B2: an absent/empty region reports the enum hint, not bare "required".
		if key == "region" && len(rule.RegionValues) > 0 {
			fieldErrors[key] = "must be one of " + strings.Join(rule.RegionValues, "|")
			return
		}
		fieldErrors[key] = "required"
	}
	for _, key := range scalarAuthKeys {
		if !required[key] && !allowed[key] {
			continue
		}
		raw, present := auth[key]
		if !present {
			if required[key] {
				markMissing(key)
			}
			continue
		}
		s, isStr := raw.(string)
		if !isStr {
			fieldErrors[key] = "must be a string"
			continue
		}
		if required[key] && s == "" {
			markMissing(key)
			continue
		}
		values[key] = s
	}

	if v := values["redirect_uri"]; v != "" && !validHTTPURL(v) {
		fieldErrors["redirect_uri"] = "must be a valid URL"
	}
	if len(rule.RegionValues) > 0 {
		if v := values["region"]; v != "" && !containsString(rule.RegionValues, v) {
			fieldErrors["region"] = "must be one of " + strings.Join(rule.RegionValues, "|")
		}
	}
	if vendor == types.VendorAliyundrive {
		cid, cs := values["client_id"], values["client_secret"]
		if cid != "" && cs == "" {
			fieldErrors["client_secret"] = "required when client_id is set"
		}
		if cs != "" && cid == "" {
			fieldErrors["client_id"] = "required when client_secret is set"
		}
	}

	var cookies map[string]string
	if required["cookies"] || allowed["cookies"] {
		cookies = validateCookies(auth["cookies"], required["cookies"], fieldErrors)
	}
	if vendor == types.Vendor115 && len(cookies) > 0 {
		var missing []string
		for _, k := range []string{"UID", "CID", "SEID"} {
			if _, ok := cookies[k]; !ok {
				missing = append(missing, k)
			}
		}
		if len(missing) > 0 {
			warnings = append(warnings, "missing recommended 115 cookie key(s): "+strings.Join(missing, ", "))
		}
	}

	credential = types.Credential{RefreshToken: values["refresh_token"]}
	if len(cookies) > 0 {
		credential.Cookies = cookies
	}
	clientConfig = types.ClientConfig{
		ClientID:     values["client_id"],
		ClientSecret: values["client_secret"],
		RedirectURI:  values["redirect_uri"],
		Region:       values["region"],
	}
	return credential, clientConfig, fieldErrors, warnings
}

// validateCookies parses the cookies value (map[string]any from JSON, or
// map[string]string from programmatic callers) and records key-shape errors.
func validateCookies(raw any, required bool, fieldErrors map[string]string) map[string]string {
	if raw == nil {
		if required {
			fieldErrors["cookies"] = "required"
		}
		return nil
	}
	cookies := map[string]string{}
	addPair := func(k, v string) {
		if !cookieKeyPattern.MatchString(k) {
			fieldErrors["cookies"] = fmt.Sprintf("invalid cookie key %q: must match %s", k, cookieKeyPatternRe)
			return
		}
		cookies[k] = v
	}
	switch m := raw.(type) {
	case map[string]any:
		for k, v := range m {
			s, ok := v.(string)
			if !ok {
				fieldErrors["cookies"] = fmt.Sprintf("value for cookie key %q must be a string", k)
				continue
			}
			addPair(k, s)
		}
	case map[string]string:
		for k, v := range m {
			addPair(k, v)
		}
	default:
		fieldErrors["cookies"] = "must be an object of key/value pairs"
		return nil
	}
	if required && len(cookies) == 0 {
		if _, alreadyFailed := fieldErrors["cookies"]; !alreadyFailed {
			fieldErrors["cookies"] = "at least one cookie key is required"
		}
	}
	return cookies
}

// OverlayAuthPatch merges a partial PUT auth patch over the currently stored
// material, implementing B2 semantics: scalar sensitive fields (refresh_token,
// client_secret) absent-or-empty = unchanged; other scalar fields absent =
// unchanged, present (even empty) = overwrite; cookies present = wholesale
// replacement (kv semantics, no per-key merge). The result feeds ValidateAuth.
func OverlayAuthPatch(current types.Credential, cc types.ClientConfig, patch map[string]any) map[string]any {
	merged := map[string]any{}
	if current.RefreshToken != "" {
		merged["refresh_token"] = current.RefreshToken
	}
	if len(current.Cookies) > 0 {
		merged["cookies"] = current.Cookies
	}
	if cc.ClientID != "" {
		merged["client_id"] = cc.ClientID
	}
	if cc.ClientSecret != "" {
		merged["client_secret"] = cc.ClientSecret
	}
	if cc.RedirectURI != "" {
		merged["redirect_uri"] = cc.RedirectURI
	}
	if cc.Region != "" {
		merged["region"] = cc.Region
	}
	for k, v := range patch {
		if k == "cookies" {
			merged["cookies"] = v // wholesale replacement
			continue
		}
		if s, isStr := v.(string); isStr && s == "" && sensitiveAuthKeys[k] {
			continue // B2: sensitive field empty = unchanged
		}
		merged[k] = v
	}
	return merged
}

// BuildAccountInfo validates a create request end to end (vendor enum,
// account_id shape, rate_limit bounds, auth rules) and assembles the
// AccountInfo for registry.CreateAccount. rate_limit defaults to the vendor's
// DefaultRateLimit; enabled defaults to true; vendor_profile.Vendor is pinned
// to the account vendor.
func BuildAccountInfo(req createAccountRequest) (accountregistry.AccountInfo, map[string]string, []string) {
	vendor := types.Vendor(req.Vendor)
	fieldErrors := map[string]string{}
	rule, ok := VendorRules[vendor]
	if !ok {
		fieldErrors["vendor"] = vendorEnumHint
	}
	if err := ValidateAccountID(req.AccountID); err != nil {
		fieldErrors["account_id"] = accountIDHint
	}
	rlCfg := rule.DefaultRateLimit
	if req.RateLimit != nil {
		for k, v := range ValidateRateLimit(*req.RateLimit) {
			fieldErrors[k] = v
		}
		rlCfg = *req.RateLimit
	}
	var cred types.Credential
	var cc types.ClientConfig
	var warnings []string
	if len(req.Auth) == 0 {
		fieldErrors["auth"] = "required"
	} else {
		c, cfg, fe, w := ValidateAuth(vendor, req.Auth)
		cred, cc, warnings = c, cfg, w
		maps.Copy(fieldErrors, fe)
	}
	en := true
	if req.Enabled != nil {
		en = *req.Enabled
	}
	var vpCfg types.VendorProfile
	if req.VendorProfile != nil {
		vpCfg = *req.VendorProfile
		vpCfg.Vendor = vendor
	}
	return accountregistry.AccountInfo{
		Vendor:        vendor,
		AccountID:     req.AccountID,
		Credential:    cred,
		ClientConfig:  cc,
		RateLimitCfg:  rlCfg,
		VendorProfile: vpCfg,
		Enabled:       en,
	}, fieldErrors, warnings
}

// AccountAuthWriter is the narrow registry surface ApplyAuthPatch needs.
// *accountregistry.AccountRegistry satisfies it.
type AccountAuthWriter interface {
	GetAccountSecret(ctx context.Context, vendor types.Vendor, accountID string) (types.Credential, types.ClientConfig, error)
	UpdateCredential(ctx context.Context, vendor types.Vendor, accountID string, cred types.Credential) error
	UpdateClientConfig(ctx context.Context, vendor types.Vendor, accountID string, cc types.ClientConfig) error
	OnCredentialChange(ctx context.Context, vendor types.Vendor, accountID string)
}

// ApplyAuthPatch applies a partial auth patch to a stored account: read
// current material, overlay, validate, write back only what changed, and fire
// exactly ONE CREDENTIAL_UPDATE broadcast when credential and/or
// client_config changed (todo 6 caller-fires-once semantics). Returns
// fieldErrors (400) or an error wrapping accountregistry.ErrAccountNotFound
// (404). warnings never block.
func ApplyAuthPatch(ctx context.Context, w AccountAuthWriter, vendor types.Vendor, accountID string, patch map[string]any) (map[string]string, []string, error) {
	currentCred, currentCC, err := w.GetAccountSecret(ctx, vendor, accountID)
	if err != nil {
		return nil, nil, err
	}
	merged := OverlayAuthPatch(currentCred, currentCC, patch)
	newCred, newCC, fieldErrors, warnings := ValidateAuth(vendor, merged)
	if len(fieldErrors) > 0 {
		return fieldErrors, nil, nil
	}
	credChanged := currentCred.RefreshToken != newCred.RefreshToken || !maps.Equal(currentCred.Cookies, newCred.Cookies)
	ccChanged := currentCC != newCC
	if credChanged {
		if err := w.UpdateCredential(ctx, vendor, accountID, newCred); err != nil {
			return nil, nil, err
		}
	}
	if ccChanged {
		if err := w.UpdateClientConfig(ctx, vendor, accountID, newCC); err != nil {
			return nil, nil, err
		}
	}
	if credChanged || ccChanged {
		w.OnCredentialChange(ctx, vendor, accountID)
	}
	return nil, warnings, nil
}

func validHTTPURL(v string) bool {
	u, err := url.Parse(v)
	return err == nil && (u.Scheme == "http" || u.Scheme == "https") && u.Host != ""
}

func containsString(list []string, v string) bool {
	for _, item := range list {
		if item == v {
			return true
		}
	}
	return false
}
