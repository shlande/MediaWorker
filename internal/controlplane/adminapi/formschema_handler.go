package adminapi

import (
	"net/http"

	"github.com/shlande/mediaworker/internal/types"
)

type fieldDef struct {
	Key       string          `json:"key"`
	Label     string          `json:"label"`
	Type      string          `json:"type"`
	Required  bool            `json:"required"`
	Sensitive bool            `json:"sensitive,omitempty"`
	Options   []FieldOption   `json:"options,omitempty"`
	KvHint    []KvHintEntry   `json:"kvHint,omitempty"`
	Help      string          `json:"help,omitempty"`
}

type defaultDef struct {
	RateLimit rateLimitDef `json:"rate_limit"`
}

type rateLimitDef struct {
	QPS             float64 `json:"qps"`
	Burst           int     `json:"burst"`
	ConcurrentLimit int     `json:"concurrent"`
}

type vendorSchemaEntry struct {
	Auth     string      `json:"auth"`
	Fields   []fieldDef  `json:"fields"`
	Defaults defaultDef  `json:"defaults"`
	Notes    []string    `json:"notes,omitempty"`
}

type formSchemaResponse struct {
	SchemaVersion string                         `json:"schema_version"`
	Vendors       map[types.Vendor]vendorSchemaEntry `json:"vendors"`
}

func formSchemaHandler(w http.ResponseWriter, _ *http.Request) {
	resp := formSchemaResponse{
		SchemaVersion: "1",
		Vendors:       buildSchema(),
	}
	WriteJSON(w, http.StatusOK, resp)
}

func buildSchema() map[types.Vendor]vendorSchemaEntry {
	out := make(map[types.Vendor]vendorSchemaEntry, len(VendorRules))
	for vendor, rule := range VendorRules {
		entry := vendorSchemaEntry{
			Auth:   rule.AuthType,
			Fields: make([]fieldDef, 0, len(rule.Fields)),
			Defaults: defaultDef{RateLimit: rateLimitDef{
				QPS:             rule.DefaultRateLimit.QPS,
				Burst:           rule.DefaultRateLimit.Burst,
				ConcurrentLimit: rule.DefaultRateLimit.ConcurrentLimit,
			}},
			Notes: rule.Notes,
		}
		for _, f := range rule.Fields {
			entry.Fields = append(entry.Fields, fieldDef{
				Key:       f.Key,
				Label:     f.Label,
				Type:      f.Type,
				Required:  f.Required,
				Sensitive: f.Sensitive,
				Options:   f.Options,
				KvHint:    f.KvHint,
				Help:      f.Help,
			})
		}
		out[vendor] = entry
	}
	return out
}

// RegisterFormSchemaRoutes mounts the form-schema endpoint.
func RegisterFormSchemaRoutes(srv *Server) {
	srv.Handle("GET /v1/admin/vendors/form-schema", http.HandlerFunc(formSchemaHandler), true)
}
