package adminapi

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sort"
	"testing"

	"github.com/shlande/mediaworker/internal/types"
)

var fiveVendors = []types.Vendor{
	types.VendorBaidu,
	types.VendorOneDrive,
	types.Vendor115,
	types.VendorQuark,
	types.VendorAliyundrive,
}

// TestFormSchema_FiveVendorsPresent verifies all five vendors appear in the
// schema response.
func TestFormSchema_FiveVendorsPresent(t *testing.T) {
	s, rec := getFormSchema(t, true)
	resp := decodeFormSchema(t, rec)
	if len(resp.Vendors) != 5 {
		t.Fatalf("expected 5 vendors, got %d", len(resp.Vendors))
	}
	for _, v := range fiveVendors {
		if _, ok := resp.Vendors[v]; !ok {
			t.Errorf("vendor %s missing from schema", v)
		}
	}
	_ = s
}

// TestFormSchema_SchemaVersion validates the schema_version field.
func TestFormSchema_SchemaVersion(t *testing.T) {
	_, rec := getFormSchema(t, true)
	resp := decodeFormSchema(t, rec)
	if resp.SchemaVersion != "1" {
		t.Errorf("schema_version = %q, want %q", resp.SchemaVersion, "1")
	}
}

// TestFormSchema_BaiduRequiredFields verifies baidu has exactly three required
// fields matching the B4 table (account_id, client_id, client_secret) and
// refresh_token is also required per the full spec.
func TestFormSchema_BaiduRequiredFields(t *testing.T) {
	_, rec := getFormSchema(t, true)
	resp := decodeFormSchema(t, rec)
	baidu := resp.Vendors[types.VendorBaidu]

	requiredKeys := requiredFieldKeys(baidu.Fields)
	want := []string{"account_id", "client_id", "client_secret", "refresh_token"}
	if !strSetEqual(requiredKeys, want) {
		t.Errorf("baidu required keys = %v, want %v", requiredKeys, want)
	}
}

// TestFormSchema_OneDriveRegionOptions verifies onedrive has the region select
// with options global/cn/us/de.
func TestFormSchema_OneDriveRegionOptions(t *testing.T) {
	_, rec := getFormSchema(t, true)
	resp := decodeFormSchema(t, rec)
	od := resp.Vendors[types.VendorOneDrive]

	var region *fieldDef
	for i := range od.Fields {
		if od.Fields[i].Key == "region" {
			region = &od.Fields[i]
			break
		}
	}
	if region == nil {
		t.Fatal("onedrive missing region field")
	}
	if region.Type != "select" {
		t.Errorf("region type = %q, want select", region.Type)
	}
	if !region.Required {
		t.Error("region should be required")
	}
	wantOpts := map[string]string{
		"global": "全球",
		"cn":     "世纪互联",
		"us":     "US Gov",
		"de":     "德国",
	}
	if len(region.Options) != 4 {
		t.Fatalf("region has %d options, want 4", len(region.Options))
	}
	for _, o := range region.Options {
		if want, ok := wantOpts[o.Value]; !ok || want != o.Label {
			t.Errorf("region option %q label %q, want %q", o.Value, o.Label, want)
		}
	}
}

// TestFormSchema_115KvHint verifies 115 cookies has kvHint with UID/CID/SEID.
func TestFormSchema_115KvHint(t *testing.T) {
	_, rec := getFormSchema(t, true)
	resp := decodeFormSchema(t, rec)
	v115 := resp.Vendors[types.Vendor115]

	var cookies *fieldDef
	for i := range v115.Fields {
		if v115.Fields[i].Key == "cookies" {
			cookies = &v115.Fields[i]
			break
		}
	}
	if cookies == nil {
		t.Fatal("115 missing cookies field")
	}
	if cookies.Type != "kv-rows" {
		t.Errorf("cookies type = %q, want kv-rows", cookies.Type)
	}
	wantHints := []string{"UID", "CID", "SEID"}
	gotHints := make([]string, len(cookies.KvHint))
	for i, h := range cookies.KvHint {
		gotHints[i] = h.Key
	}
	if !strSetEqual(gotHints, wantHints) {
		t.Errorf("115 kvHint = %v, want %v", gotHints, wantHints)
	}
}

// TestFormSchema_RateLimitDefaults verifies rate_limit defaults match
// the vendor-account-params.md matrix.
func TestFormSchema_RateLimitDefaults(t *testing.T) {
	_, rec := getFormSchema(t, true)
	resp := decodeFormSchema(t, rec)

	want := map[types.Vendor]rateLimitDef{
		types.VendorBaidu:       {QPS: 2, Burst: 4, ConcurrentLimit: 8},
		types.VendorOneDrive:    {QPS: 10, Burst: 20, ConcurrentLimit: 16},
		types.VendorAliyundrive: {QPS: 5, Burst: 10, ConcurrentLimit: 10},
		types.Vendor115:         {QPS: 1, Burst: 2, ConcurrentLimit: 5},
		types.VendorQuark:       {QPS: 0.5, Burst: 1, ConcurrentLimit: 5},
	}
	for _, v := range fiveVendors {
		got := resp.Vendors[v].Defaults.RateLimit
		w := want[v]
		if got != w {
			t.Errorf("%s rate_limit = %+v, want %+v", v, got, w)
		}
	}
}

// TestFormSchema_ConsistencyGuard verifies that ValidateAuth's required keys
// match the schema's required=true keys for every vendor. If they drift the
// test fails.
func TestFormSchema_ConsistencyGuard(t *testing.T) {
	_, rec := getFormSchema(t, true)
	resp := decodeFormSchema(t, rec)

	for _, vendor := range fiveVendors {
		rule := VendorRules[vendor]
		entry := resp.Vendors[vendor]

		// Schema required keys (non-cookie, non-account_id).
		var schemaRequired []string
		for _, f := range entry.Fields {
			if f.Required {
				schemaRequired = append(schemaRequired, f.Key)
			}
		}

		// ValidateAuth required keys come from rule.RequiredAuth.
		validateRequired := rule.RequiredAuth

		// account_id is NOT in RequiredAuth (it's in ValidateAccountID),
		// but it IS marked required in the schema. Remove it for comparison.
		var schemaAuthRequired []string
		for _, k := range schemaRequired {
			if k == "account_id" {
				continue
			}
			schemaAuthRequired = append(schemaAuthRequired, k)
		}

		sort.Strings(schemaAuthRequired)
		sort.Strings(validateRequired)

		if !strSetEqual(schemaAuthRequired, validateRequired) {
			t.Errorf("%s schema required (non-account_id) = %v, ValidateAuth required = %v",
				vendor, schemaAuthRequired, validateRequired)
		}
	}
}

// TestFormSchema_NoToken401 verifies unauthenticated requests get 401.
func TestFormSchema_NoToken401(t *testing.T) {
	_, rec := getFormSchema(t, false)
	if rec.Code != http.StatusUnauthorized {
		t.Errorf("no-token status = %d, want %d, body: %s", rec.Code, http.StatusUnauthorized, rec.Body.String())
	}
}

// TestFormSchema_AuthTypes verifies auth field per vendor.
func TestFormSchema_AuthTypes(t *testing.T) {
	_, rec := getFormSchema(t, true)
	resp := decodeFormSchema(t, rec)

	want := map[types.Vendor]string{
		types.VendorBaidu:       "oauth2",
		types.VendorOneDrive:    "oauth2",
		types.VendorAliyundrive: "oauth2",
		types.Vendor115:         "cookie",
		types.VendorQuark:       "cookie",
	}
	for _, v := range fiveVendors {
		if got := resp.Vendors[v].Auth; got != want[v] {
			t.Errorf("%s auth = %q, want %q", v, got, want[v])
		}
	}
}

// TestFormSchema_RegisterRoutes verifies the route is mounted correctly.
func TestFormSchema_RegisterRoutes(t *testing.T) {
	secret := []byte("test-secret")
	srv := NewServer(secret)
	RegisterFormSchemaRoutes(srv)
	ts := httptest.NewServer(srv.mux)
	t.Cleanup(ts.Close)

	tok := signedToken(t, secret, []string{"admin"})
	req, _ := http.NewRequest(http.MethodGet, ts.URL+"/v1/admin/vendors/form-schema", nil)
	req.Header.Set("Authorization", "Bearer "+tok)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("request: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200", resp.StatusCode)
	}
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

func getFormSchema(t *testing.T, authed bool) (*Server, *httptest.ResponseRecorder) {
	t.Helper()
	secret := []byte("test-secret")
	srv := NewServer(secret)
	RegisterFormSchemaRoutes(srv)

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/v1/admin/vendors/form-schema", nil)
	if authed {
		req.Header.Set("Authorization", "Bearer "+signedToken(t, secret, []string{"admin"}))
	}
	srv.mux.ServeHTTP(rec, req)
	return srv, rec
}

func decodeFormSchema(t *testing.T, rec *httptest.ResponseRecorder) formSchemaResponse {
	t.Helper()
	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", rec.Code, rec.Body.String())
	}
	var resp formSchemaResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("decode: %v", err)
	}
	return resp
}

func requiredFieldKeys(fields []fieldDef) []string {
	var keys []string
	for _, f := range fields {
		if f.Required {
			keys = append(keys, f.Key)
		}
	}
	sort.Strings(keys)
	return keys
}

func strSetEqual(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}
