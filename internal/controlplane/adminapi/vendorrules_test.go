package adminapi

import (
	"strings"
	"testing"

	"github.com/shlande/mediaworker/internal/types"
)

// Given the vendor rule table, when inspected, then each vendor carries the
// B4 auth contract and the docs-matrix rate-limit defaults.
func TestVendorRules_Matrix(t *testing.T) {
	cases := []struct {
		vendor     types.Vendor
		authType   string
		required   []string
		qps        float64
		burst      int
		concurrent int
	}{
		{types.VendorBaidu, "oauth2", []string{"client_id", "client_secret", "refresh_token"}, 2, 4, 8},
		{types.VendorOneDrive, "oauth2", []string{"client_id", "client_secret", "refresh_token", "redirect_uri", "region"}, 10, 20, 16},
		{types.VendorAliyundrive, "oauth2", []string{"refresh_token"}, 5, 10, 10},
		{types.Vendor115, "cookie", []string{"cookies"}, 1, 2, 5},
		{types.VendorQuark, "cookie", []string{"cookies"}, 0.5, 1, 5},
	}
	for _, tc := range cases {
		rule, ok := VendorRules[tc.vendor]
		if !ok {
			t.Fatalf("VendorRules missing %s", tc.vendor)
		}
		if rule.AuthType != tc.authType {
			t.Errorf("%s AuthType = %q, want %q", tc.vendor, rule.AuthType, tc.authType)
		}
		if strings.Join(rule.RequiredAuth, ",") != strings.Join(tc.required, ",") {
			t.Errorf("%s RequiredAuth = %v, want %v", tc.vendor, rule.RequiredAuth, tc.required)
		}
		rl := rule.DefaultRateLimit
		if rl.QPS != tc.qps || rl.Burst != tc.burst || rl.ConcurrentLimit != tc.concurrent {
			t.Errorf("%s DefaultRateLimit = %+v, want {%v %d %d}", tc.vendor, rl, tc.qps, tc.burst, tc.concurrent)
		}
	}
	od := VendorRules[types.VendorOneDrive]
	if strings.Join(od.RegionValues, ",") != "global,cn,us,de" {
		t.Errorf("onedrive RegionValues = %v, want [global cn us de]", od.RegionValues)
	}
	if len(VendorRules) != 5 {
		t.Errorf("len(VendorRules) = %d, want 5 (five-value vendor enum)", len(VendorRules))
	}
}

// Given valid baidu auth material, when ValidateAuth runs, then the material
// splits into credential (refresh_token) + client_config (the rest).
func TestValidateAuth_BaiduSplitNormalize(t *testing.T) {
	cred, cc, fieldErrors, warnings := ValidateAuth(types.VendorBaidu, map[string]any{
		"client_id":     "appkey-1",
		"client_secret": "secret-1",
		"refresh_token": "rt-1",
		"redirect_uri":  "https://example.com/callback",
	})
	if len(fieldErrors) != 0 {
		t.Fatalf("fieldErrors = %v, want empty", fieldErrors)
	}
	if len(warnings) != 0 {
		t.Errorf("warnings = %v, want empty", warnings)
	}
	if cred.RefreshToken != "rt-1" {
		t.Errorf("credential.refresh_token = %q, want rt-1", cred.RefreshToken)
	}
	if cc.ClientID != "appkey-1" || cc.ClientSecret != "secret-1" || cc.RedirectURI != "https://example.com/callback" {
		t.Errorf("clientConfig = %+v, want appkey-1/secret-1/https://example.com/callback", cc)
	}
	if cc.Region != "" {
		t.Errorf("clientConfig.region = %q, want empty (baidu has no region)", cc.Region)
	}
}

// Given B4 rule violations, when ValidateAuth runs per vendor, then each
// yields the documented field error.
func TestValidateAuth_RuleViolations(t *testing.T) {
	cases := []struct {
		name      string
		vendor    types.Vendor
		auth      map[string]any
		wantField string
		wantMsg   string // substring; "" = only assert the field key is present
	}{
		{
			name:   "baidu missing refresh_token",
			vendor: types.VendorBaidu,
			auth: map[string]any{
				"client_id": "x", "client_secret": "y",
			},
			wantField: "refresh_token",
			wantMsg:   "required",
		},
		{
			name:   "baidu bad redirect_uri",
			vendor: types.VendorBaidu,
			auth: map[string]any{
				"client_id": "x", "client_secret": "y", "refresh_token": "z",
				"redirect_uri": "not-a-url",
			},
			wantField: "redirect_uri",
			wantMsg:   "must be a valid URL",
		},
		{
			name:   "onedrive missing region gets enum hint",
			vendor: types.VendorOneDrive,
			auth: map[string]any{
				"client_id": "x", "client_secret": "y", "refresh_token": "z",
				"redirect_uri": "https://example.com/cb",
			},
			wantField: "region",
			wantMsg:   "must be one of global|cn|us|de",
		},
		{
			name:   "onedrive bad region value",
			vendor: types.VendorOneDrive,
			auth: map[string]any{
				"client_id": "x", "client_secret": "y", "refresh_token": "z",
				"redirect_uri": "https://example.com/cb", "region": "moon",
			},
			wantField: "region",
			wantMsg:   "must be one of global|cn|us|de",
		},
		{
			name:      "aliyundrive missing refresh_token",
			vendor:    types.VendorAliyundrive,
			auth:      map[string]any{},
			wantField: "refresh_token",
			wantMsg:   "required",
		},
		{
			name:   "aliyundrive client_id without client_secret",
			vendor: types.VendorAliyundrive,
			auth: map[string]any{
				"refresh_token": "z", "client_id": "x",
			},
			wantField: "client_secret",
			wantMsg:   "required when client_id is set",
		},
		{
			name:   "aliyundrive client_secret without client_id",
			vendor: types.VendorAliyundrive,
			auth: map[string]any{
				"refresh_token": "z", "client_secret": "y",
			},
			wantField: "client_id",
			wantMsg:   "required when client_secret is set",
		},
		{
			name:      "115 missing cookies",
			vendor:    types.Vendor115,
			auth:      map[string]any{},
			wantField: "cookies",
			wantMsg:   "required",
		},
		{
			name:   "quark empty cookies",
			vendor: types.VendorQuark,
			auth: map[string]any{
				"cookies": map[string]any{},
			},
			wantField: "cookies",
			wantMsg:   "at least one cookie key is required",
		},
		{
			name:   "quark bad cookie key name",
			vendor: types.VendorQuark,
			auth: map[string]any{
				"cookies": map[string]any{"token-x": "v"},
			},
			wantField: "cookies",
			wantMsg:   "invalid cookie key",
		},
		{
			name:   "non-string scalar",
			vendor: types.VendorBaidu,
			auth: map[string]any{
				"client_id": 42, "client_secret": "y", "refresh_token": "z",
			},
			wantField: "client_id",
			wantMsg:   "must be a string",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, _, fieldErrors, _ := ValidateAuth(tc.vendor, tc.auth)
			msg, present := fieldErrors[tc.wantField]
			if !present {
				t.Fatalf("fieldErrors = %v, want key %q", fieldErrors, tc.wantField)
			}
			if tc.wantMsg != "" && !strings.Contains(msg, tc.wantMsg) {
				t.Errorf("fieldErrors[%q] = %q, want substring %q", tc.wantField, msg, tc.wantMsg)
			}
		})
	}
}

// Given 115 cookies missing the UID/CID/SEID convention keys, when
// ValidateAuth runs, then a warning is emitted WITHOUT blocking.
func TestValidateAuth_115ConventionWarningNotBlocking(t *testing.T) {
	cred, _, fieldErrors, warnings := ValidateAuth(types.Vendor115, map[string]any{
		"cookies": map[string]any{"UID": "u1"},
	})
	if len(fieldErrors) != 0 {
		t.Fatalf("fieldErrors = %v, want empty (warning must not block)", fieldErrors)
	}
	if len(warnings) == 0 {
		t.Fatal("warnings empty, want missing CID/SEID warning")
	}
	if !strings.Contains(warnings[0], "CID") || !strings.Contains(warnings[0], "SEID") {
		t.Errorf("warning = %q, want mention of CID and SEID", warnings[0])
	}
	if cred.Cookies["UID"] != "u1" {
		t.Errorf("credential.cookies = %v, want UID=u1", cred.Cookies)
	}

	// Full convention keys → no warning.
	_, _, _, warnings = ValidateAuth(types.Vendor115, map[string]any{
		"cookies": map[string]any{"UID": "u", "CID": "c", "SEID": "s"},
	})
	if len(warnings) != 0 {
		t.Errorf("warnings = %v, want empty when UID/CID/SEID all present", warnings)
	}
}

// Given aliyundrive's optional pair, when both are supplied, then validation
// passes and the pair lands in client_config.
func TestValidateAuth_AliyunPairHappy(t *testing.T) {
	_, cc, fieldErrors, _ := ValidateAuth(types.VendorAliyundrive, map[string]any{
		"refresh_token": "rt", "client_id": "cid", "client_secret": "cs",
	})
	if len(fieldErrors) != 0 {
		t.Fatalf("fieldErrors = %v, want empty", fieldErrors)
	}
	if cc.ClientID != "cid" || cc.ClientSecret != "cs" {
		t.Errorf("clientConfig = %+v, want cid/cs", cc)
	}
}

// Given account_id candidates, when ValidateAccountID runs, then only the
// B4 shape is accepted.
func TestValidateAccountID(t *testing.T) {
	valid := []string{"mw_bak_01", "ab", "A1-_", strings.Repeat("x", 64)}
	for _, id := range valid {
		if err := ValidateAccountID(id); err != nil {
			t.Errorf("ValidateAccountID(%q) = %v, want nil", id, err)
		}
	}
	invalid := []string{"", "a", strings.Repeat("x", 65), "bad id", "中文", "a.b"}
	for _, id := range invalid {
		if err := ValidateAccountID(id); err == nil {
			t.Errorf("ValidateAccountID(%q) = nil, want error", id)
		}
	}
}

// Given rate_limit candidates, when ValidateRateLimit runs, then B4 bounds
// are enforced per sub-field.
func TestValidateRateLimit(t *testing.T) {
	cases := []struct {
		name      string
		rl        types.RateLimitConfig
		wantField string // "" = expect no errors
	}{
		{"valid", types.RateLimitConfig{QPS: 2, Burst: 4, ConcurrentLimit: 8}, ""},
		{"qps too low", types.RateLimitConfig{QPS: 0.05, Burst: 4, ConcurrentLimit: 8}, "rate_limit.qps"},
		{"qps too high", types.RateLimitConfig{QPS: 200, Burst: 4, ConcurrentLimit: 8}, "rate_limit.qps"},
		{"burst zero", types.RateLimitConfig{QPS: 2, Burst: 0, ConcurrentLimit: 8}, "rate_limit.burst"},
		{"burst too high", types.RateLimitConfig{QPS: 2, Burst: 101, ConcurrentLimit: 8}, "rate_limit.burst"},
		{"concurrent zero", types.RateLimitConfig{QPS: 2, Burst: 4, ConcurrentLimit: 0}, "rate_limit.concurrent"},
		{"concurrent too high", types.RateLimitConfig{QPS: 2, Burst: 4, ConcurrentLimit: 65}, "rate_limit.concurrent"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			fe := ValidateRateLimit(tc.rl)
			if tc.wantField == "" {
				if len(fe) != 0 {
					t.Fatalf("fieldErrors = %v, want empty", fe)
				}
				return
			}
			if _, ok := fe[tc.wantField]; !ok {
				t.Fatalf("fieldErrors = %v, want key %q", fe, tc.wantField)
			}
		})
	}
}

// Given stored material and a partial patch, when OverlayAuthPatch runs,
// then B2 semantics hold: sensitive empty = unchanged, cookies = wholesale,
// region present = client_config rewritten via the merged map.
func TestOverlayAuthPatch(t *testing.T) {
	current := types.Credential{RefreshToken: "old-rt", Cookies: map[string]string{"UID": "u", "CID": "c"}}
	cc := types.ClientConfig{ClientID: "cid", ClientSecret: "old-cs", RedirectURI: "https://cb", Region: "global"}

	t.Run("empty refresh_token keeps stored value", func(t *testing.T) {
		merged := OverlayAuthPatch(current, cc, map[string]any{"refresh_token": ""})
		if merged["refresh_token"] != "old-rt" {
			t.Errorf("refresh_token = %v, want old-rt (sensitive empty = unchanged)", merged["refresh_token"])
		}
	})
	t.Run("empty client_secret keeps stored value", func(t *testing.T) {
		merged := OverlayAuthPatch(current, cc, map[string]any{"client_secret": ""})
		if merged["client_secret"] != "old-cs" {
			t.Errorf("client_secret = %v, want old-cs", merged["client_secret"])
		}
	})
	t.Run("region present overwrites for client_config rewrite", func(t *testing.T) {
		merged := OverlayAuthPatch(current, cc, map[string]any{"region": "cn"})
		if merged["region"] != "cn" {
			t.Errorf("region = %v, want cn", merged["region"])
		}
		if merged["client_id"] != "cid" || merged["client_secret"] != "old-cs" {
			t.Errorf("merged = %v, want stored client material carried through", merged)
		}
	})
	t.Run("cookies wholesale replacement", func(t *testing.T) {
		merged := OverlayAuthPatch(current, cc, map[string]any{
			"cookies": map[string]any{"UID": "new"},
		})
		cookies, ok := merged["cookies"].(map[string]any)
		if !ok || len(cookies) != 1 || cookies["UID"] != "new" {
			t.Errorf("cookies = %v, want wholesale {UID:new}", merged["cookies"])
		}
	})
	t.Run("new refresh_token overwrites", func(t *testing.T) {
		merged := OverlayAuthPatch(current, cc, map[string]any{"refresh_token": "new-rt"})
		if merged["refresh_token"] != "new-rt" {
			t.Errorf("refresh_token = %v, want new-rt", merged["refresh_token"])
		}
	})
}
