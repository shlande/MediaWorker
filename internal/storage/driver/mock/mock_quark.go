package mock

import "github.com/shlande/mediaworker/internal/types"

func NewQuarkDriver(cfg MockDriverConfig) *MockDriver {
	return NewMockDriver(types.VendorQuark, cfg)
}