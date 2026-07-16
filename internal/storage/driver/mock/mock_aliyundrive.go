package mock

import "github.com/shlande/mediaworker/internal/types"

func NewAliyundriveDriver(cfg MockDriverConfig) *MockDriver {
	return NewMockDriver(types.VendorAliyundrive, cfg)
}