#!/usr/bin/env bash
# gen-swagger.sh — Generate per-service Swagger 2.0 specs (JSON+YAML)
#
# Usage: bash scripts/gen-swagger.sh
# Run from the repository root (or any directory — script auto-detects root).
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
gen_spec() {
  local svc="$1"      # service name (used for output dir name)
  local main="$2"     # path to main.go from repo root
  local dirs="$3"     # comma-separated -d directories
  local extra="$4"    # extra flags (empty string if none)

  echo ""
  echo "==> Generating spec for ${svc}..."
  # shellcheck disable=SC2086
  "${SWAG}" init \
    -g "${main}" \
    -d "${dirs}" \
    --parseInternal \
    --outputTypes json,yaml \
    -o "api/${svc}" \
    ${extra}

  local rc=$?
  if [ $rc -eq 0 ]; then
    echo "    OK — api/${svc}/swagger.{json,yaml} created."
  else
    echo "    WARNING: swag init exited ${rc} (may be expected if main.go lacks general-info annotations yet)."
  fi
  return $rc
}

# ── Generate specs ──────────────────────────────────────────────────────────
gen_spec "control-plane" \
  "cmd/control-plane/main.go" \
  "cmd/control-plane,internal/controlplane,internal/types" \
  ""

gen_spec "edge-node" \
  "cmd/edge-node/main.go" \
  "cmd/edge-node,internal/node,internal/types" \
  ""

gen_spec "ingest-worker" \
  "cmd/ingest-worker/main.go" \
  "cmd/ingest-worker,internal/ingest,internal/types" \
  ""

echo ""
echo "=== Done ==="