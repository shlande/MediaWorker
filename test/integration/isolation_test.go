// Package integration_test provides binary-level isolation tests that verify
// the control-plane and edge-node binaries do not cross-link into each other's
// role-specific packages. This enforces the architectural boundary:
//
//   cmd/control-plane  →  internal/controlplane/*  (no internal/node/*)
//   cmd/edge-node      →  internal/node/*          (no internal/controlplane/*)
//
// These tests use `go list -deps` to enumerate the full transitive dependency
// closure of each binary and assert that forbidden packages are absent.
package integration_test

import (
	"os"
	"os/exec"
	"strings"
	"testing"
)

var (
	// nodeForbiddenInControlPlane lists every internal/node/ package that must
	// NOT appear in the control-plane binary's dependency graph.
	nodeForbiddenInControlPlane = []string{
		"github.com/shlande/mediaworker/internal/node/cache",
		"github.com/shlande/mediaworker/internal/node/backhaul",
		"github.com/shlande/mediaworker/internal/node/icp",
		"github.com/shlande/mediaworker/internal/node/hashring",
		"github.com/shlande/mediaworker/internal/node/gossippop",
		"github.com/shlande/mediaworker/internal/node/pinstore",
		"github.com/shlande/mediaworker/internal/node/routing",
		"github.com/shlande/mediaworker/internal/node/monitor",
		"github.com/shlande/mediaworker/internal/node/dht",
		"github.com/shlande/mediaworker/internal/node/libp2phost",
		"github.com/shlande/mediaworker/internal/node/peerstore",
	}

	// controlPlaneForbiddenInNode lists every internal/controlplane/ package
	// that must NOT appear in the edge-node binary's dependency graph.
	controlPlaneForbiddenInNode = []string{
		"github.com/shlande/mediaworker/internal/controlplane/jwt",
		"github.com/shlande/mediaworker/internal/controlplane/pinstrategy",
		"github.com/shlande/mediaworker/internal/controlplane/syncbroadcaster",
		"github.com/shlande/mediaworker/internal/controlplane/dhtbootstrap",
		"github.com/shlande/mediaworker/internal/controlplane/metadata",
	}
)

// TestIsolation_ControlPlaneBinaryNoNodeCode verifies that the control-plane
// binary does not link any internal/node/* packages.
func TestIsolation_ControlPlaneBinaryNoNodeCode(t *testing.T) {
	deps := goListDeps(t, "./cmd/control-plane/")
	violations := checkForbidden(t, deps, nodeForbiddenInControlPlane)

	if len(violations) > 0 {
		t.Errorf("control-plane binary links %d forbidden internal/node/ packages:\n\t%s",
			len(violations),
			strings.Join(violations, "\n\t"))
	} else {
		t.Logf("control-plane binary: 0 forbidden internal/node/ packages out of %d total deps", len(deps))
	}
}

// TestIsolation_NodeBinaryNoControlPlaneCode verifies that the edge-node binary
// does not link any internal/controlplane/* packages.
func TestIsolation_NodeBinaryNoControlPlaneCode(t *testing.T) {
	deps := goListDeps(t, "./cmd/edge-node/")
	violations := checkForbidden(t, deps, controlPlaneForbiddenInNode)

	if len(violations) > 0 {
		t.Errorf("edge-node binary links %d forbidden internal/controlplane/ packages:\n\t%s",
			len(violations),
			strings.Join(violations, "\n\t"))
	} else {
		t.Logf("edge-node binary: 0 forbidden internal/controlplane/ packages out of %d total deps", len(deps))
	}
}

// goListDeps runs "go list -deps <target>" from the module root directory
// and returns the output lines (one package path per line).
func goListDeps(t *testing.T, target string) []string {
	t.Helper()

	// Resolve the module root via "go env GOMOD" so the command works
	// regardless of the test's current working directory.
	modRoot := goModuleRoot(t)

	cmd := exec.Command("go", "list", "-deps", target)
	cmd.Dir = modRoot
	out, err := cmd.Output()
	if err != nil {
		exitErr, _ := err.(*exec.ExitError)
		t.Fatalf("go list -deps %s: %v\nstderr: %s", target, err, string(exitErr.Stderr))
	}
	lines := strings.Split(strings.TrimSpace(string(out)), "\n")
	return lines
}

// goModuleRoot returns the path to the Go module root directory.
func goModuleRoot(t *testing.T) string {
	t.Helper()
	cmd := exec.Command("go", "env", "GOMOD")
	out, err := cmd.Output()
	if err != nil {
		t.Fatalf("go env GOMOD: %v", err)
	}
	gomod := strings.TrimSpace(string(out))
	// GOMOD returns the path to go.mod; dirname gives the module root.
	if gomod == "" || gomod == os.DevNull {
		t.Fatal("GOMOD is empty — not inside a Go module?")
	}
	// Strip "/go.mod" suffix to get the module root directory.
	return strings.TrimSuffix(gomod, "/go.mod")
}

// checkForbidden returns the subset of deps that match any entry in the
// forbidden package prefix list.
func checkForbidden(t *testing.T, deps []string, forbidden []string) []string {
	t.Helper()

	var violations []string
	for _, dep := range deps {
		for _, f := range forbidden {
			if strings.Contains(dep, f) {
				violations = append(violations, dep)
			}
		}
	}
	return violations
}
