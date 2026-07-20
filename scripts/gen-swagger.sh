#!/usr/bin/env bash
# gen-swagger.sh — Generate per-service Swagger 2.0 specs (JSON+YAML)
#
# Usage: bash scripts/gen-swagger.sh
# Run from the repository root (or any directory — script auto-detects root).
#
# Strategy: -d . (whole repo) with --exclude to prevent cross-service route
# leakage.  Comma-separated -d fails due to swag nesting bug (learnings #1,
# #7 in .omo/notepads/swagger-api-docs/learnings.md).
set -euo pipefail

# ── Resolve repository root ────────────────────────────────────────────────
cd "$(dirname "$0")/.."
ROOT="$(pwd)"

# ── Locate swag binary ──────────────────────────────────────────────────────
SWAG=""
if command -v swag &>/dev/null; then
  SWAG="swag"
elif [ -x "$(go env GOPATH)/bin/swag" ]; then
  SWAG="$(go env GOPATH)/bin/swag"
else
  echo "ERROR: swag CLI not found in PATH or \$(go env GOPATH)/bin."
  echo "Install with:"
  echo "  go install github.com/swaggo/swag/cmd/swag@v1.16.6"
  exit 1
fi

echo "Using swag: ${SWAG}"
"${SWAG}" --version

# ── Helper: run swag init for a service ─────────────────────────────────────
# Args: service_name  main_go_path  exclude_dirs(comma-separated)
gen_spec() {
  local svc="$1"
  local main="$2"
  local exclude="$3"

  echo ""
  echo "==> Generating spec for ${svc}..."
  # Tests confirmed:
  #   form (a) -g + comma-separated -d FAILS: swag nests -g path into each -d dir
  #   form (b) -g relative + comma-separated -d FAILS: can't resolve transitive types (e.g. metadata.CredentialMeta)
  #   form (c) -d . --exclude PASSES: full type resolution, route isolation via exclusion
  "${SWAG}" init \
    -g "${main}" \
    -d . \
    --parseInternal \
    --exclude "${exclude}" \
    --outputTypes json,yaml \
    -o "api/${svc}"

  local rc=$?
  if [ $rc -eq 0 ]; then
    echo "    OK — api/${svc}/swagger.{json,yaml} created."
  else
    echo "    ERROR: swag init for ${svc} failed with exit code ${rc}."
    return $rc
  fi
}

# ── Generate specs ──────────────────────────────────────────────────────────
# Each service scans the whole repo (-d .) but excludes the other services'
# source trees to prevent cross-service route leakage.  Route counts verified:
#   CP: 33 route×method combinations (30 unique paths)
#   edge: 13 paths
#   ingest: 3 paths

gen_spec "control-plane" \
  "cmd/control-plane/main.go" \
  "cmd/edge-node,cmd/ingest-worker,internal/node,internal/ingest"

gen_spec "edge-node" \
  "cmd/edge-node/main.go" \
  "cmd/control-plane,cmd/ingest-worker,internal/controlplane,internal/ingest"

gen_spec "ingest-worker" \
  "cmd/ingest-worker/main.go" \
  "cmd/control-plane,cmd/edge-node,internal/controlplane,internal/node"

echo ""
echo "=== Done ==="
