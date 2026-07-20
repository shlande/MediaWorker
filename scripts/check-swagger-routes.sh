#!/usr/bin/env bash
# check-swagger-routes.sh — Reconcile all registered HTTP routes against
# Swagger 2.0 specs (operations: METHOD + path).
#
# Usage: bash scripts/check-swagger-routes.sh
# Run from the repository root.
#
# Exit 0: all 49 route×method combinations match across source ↔ spec.
# Exit 1: difference detected (missing in spec or extra in source).
set -euo pipefail

cd "$(dirname "$0")/.."
ROOT="$(pwd)"

TMPDIR="$(mktemp -d)"
trap 'rm -rf "$TMPDIR"' EXIT

SRC="$TMPDIR/src_routes.txt"
SPEC="$TMPDIR/spec_routes.txt"

# ── Step 1: Extract all mux-registered routes from Go source ─────────────────
# Exclude *_test.go files (server_test.go has fake /v1/ping, /slow etc.)
# Normalise: POST/PUT/PATCH etc. → uppercase method prefix; path as-is.
# Edge case: /ingest/ prefix registration → POST /ingest/{content_type}

{
  # ── control-plane (33 routes / 3 public + 30 admin) ──
  echo '## CP'
  find internal/controlplane/jwt -name "httpserver.go" ! -name '*_test.go' -exec grep -hE 'mux\.Handle(Func)?\(' {} \; | \
    sed -n 's/.*mux\.Handle\(Func\)\{0,1\}("\([A-Z]* [^"]*\)".*/\2/p'
  find internal/controlplane/adminapi -name '*.go' ! -name '*_test.go' -exec grep -h 'srv\.Handle(' {} \; | \
    sed -n 's/.*srv\.Handle("\([A-Z]* [^"]*\)".*/\1/p'

  # ── edge-node (13 routes / 2 client + 1 healthz + 10 admin) ──
  echo '## EDGE'
  grep -hE 'mux\.Handle(Func)?\(' cmd/edge-node/main.go | \
    sed -n 's/.*mux\.Handle\(Func\)\{0,1\}("\([A-Z]* [^"]*\)".*/\2/p'
  grep -h 'HandleUnauthenticated(' cmd/edge-node/main.go | \
    sed -n 's/.*HandleUnauthenticated("\([A-Z]* [^"]*\)".*/\1/p'
  find internal/node/adminapi -name '*.go' ! -name '*_test.go' -exec grep -h 'srv\.Handle(' {} \; | \
    sed -n 's/.*srv\.Handle("\([A-Z]* [^"]*\)".*/\1/p'

  # ── ingest-worker (3 routes, net/http.ServeMux pure-path patterns) ──
  echo '## INGEST'
  grep -hE 'mux\.Handle(Func)?\(' cmd/ingest-worker/main.go | while read -r line; do
    pattern="$(echo "$line" | sed -n 's/.*mux\.Handle\(Func\)\{0,1\}("\([^"]*\)".*/\2/p')"
    case "$pattern" in
      /ingest/)  echo 'POST /ingest/{content_type}' ;;
      /*)        echo "GET $pattern" ;;
      *)         ;;
    esac
  done

} > "$SRC"

# ── Step 2: Extract operations per-service from Swagger 2.0 specs ─────────────
# Per-service deduplication (same path may appear in multiple specs — e.g.
# GET /metrics in all three, GET /healthz in edge+ingest).  Use ## markers.
SPEC_DIRS="control-plane:api/control-plane/swagger.json edge-node:api/edge-node/swagger.json ingest-worker:api/ingest-worker/swagger.json"
for entry in $SPEC_DIRS; do
  svc="${entry%%:*}"
  spec="${entry##*:}"
  echo "## $svc" | tr 'a-z' 'A-Z'
  jq -r '.paths | to_entries[] | .key as $p | .value | to_entries[] | select(.key != "parameters") | "\(.key | ascii_upcase) \($p)"' "$spec"
done > "$SPEC"

# ── Step 3: Compare ──────────────────────────────────────────────────────────
SRC_COUNT="$(grep -vcE '^##' "$SRC")"
SPEC_COUNT="$(grep -vcE '^##' "$SPEC")"

echo "Source routes extracted:  $SRC_COUNT"
echo "Swagger spec operations:  $SPEC_COUNT"
echo ""
echo "Per-service breakdown:"
echo "  CP:    $(sed -n '/^## CP$/,/^## EDGE$/p' "$SRC" | grep -vcE '^##') src / $(sed -n '/^## CONTROL-PLANE$/,/^## EDGE-NODE$/p' "$SPEC" | grep -vcE '^##') spec"
echo "  edge:  $(sed -n '/^## EDGE$/,/^## INGEST$/p' "$SRC" | grep -vcE '^##') src / $(sed -n '/^## EDGE-NODE$/,/^## INGEST-WORKER$/p' "$SPEC" | grep -vcE '^##') spec"
echo "  ingest: $(sed -n '/^## INGEST$/,$ p' "$SRC" | grep -vcE '^##') src / $(sed -n '/^## INGEST-WORKER$/,$ p' "$SPEC" | grep -vcE '^##') spec"

MISSING_IN_SPEC="$(comm -23 <(grep -vE '^##' "$SRC" | sort -u) <(grep -vE '^##' "$SPEC" | sort -u))"
EXTRA_IN_SPEC="$(comm -13 <(grep -vE '^##' "$SRC" | sort -u) <(grep -vE '^##' "$SPEC" | sort -u))"

if [ -n "$MISSING_IN_SPEC" ] || [ -n "$EXTRA_IN_SPEC" ]; then
  echo ""
  echo "=== ROUTE RECONCILIATION FAILED ==="
  if [ -n "$MISSING_IN_SPEC" ]; then
    echo ""
    echo "Routes in source but NOT in Swagger specs:"
    echo "$MISSING_IN_SPEC"
  fi
  if [ -n "$EXTRA_IN_SPEC" ]; then
    echo ""
    echo "Routes in Swagger specs but NOT in source:"
    echo "$EXTRA_IN_SPEC"
  fi
  exit 1
fi

echo ""
echo "OK: $SRC_COUNT routes matched"
exit 0
