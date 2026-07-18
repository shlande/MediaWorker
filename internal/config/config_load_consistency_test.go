// T23 gate (e) config-consistency test: verifies every configs/*.yaml example
// loads cleanly via its corresponding Load* function. Kept as a permanent
// regression guard — caught by `go test ./internal/config/...`.
//
// Originally added as T23's "一次性 Go 小测" evidence; promoted to a real
// test because the plan accepts it ("or keep it as a real test if it makes
// sense").

package config

import "testing"

func TestConfigLoadConsistency_T23(t *testing.T) {
	cases := []struct {
		name string
		fn   func() error
	}{
		{"node-edge.yaml", func() error {
			_, err := LoadConfig("../../configs/node-edge.yaml")
			return err
		}},
		{"node-l4.yaml", func() error {
			_, err := LoadConfig("../../configs/node-l4.yaml")
			return err
		}},
		{"control-plane.yaml", func() error {
			_, err := LoadControlPlaneConfig("../../configs/control-plane.yaml")
			return err
		}},
		{"ingest-worker.yaml", func() error {
			_, err := LoadIngestWorkerConfig("../../configs/ingest-worker.yaml")
			return err
		}},
		{"janitor.yaml", func() error {
			_, err := LoadJanitorConfig("../../configs/janitor.yaml")
			return err
		}},
	}
	for _, c := range cases {
		c := c
		t.Run(c.name, func(t *testing.T) {
			if err := c.fn(); err != nil {
				t.Fatalf("load %s: %v", c.name, err)
			}
		})
	}
}
