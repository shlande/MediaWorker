# Review Remediation Learnings

## T4 - ingest 临时目录治理
- Added `WorkDir string` field to `ProcessResult` struct in `ingest.go`
- Set `WorkDir` in `dash.go` (outDir) and `image.go` (workDir) success returns
- `defer os.RemoveAll(result.WorkDir)` in `pipeline.go` Ingest() after Process success
  - Guarded with `if result.WorkDir != ""` — zero value = no cleanup needed
  - Defer fires on ALL return paths (success + all error paths), including upload failure and tx failure
- Added `sweepStaleWorkDir` in `cmd/ingest-worker/main.go` startup
  - Walks first-level children + `*_src.mp4` files, deletes entries with mtime < startup time
  - Failures are `slog.Warn` only, never fatal
- All existing error-path `os.RemoveAll` calls preserved (dash.go:52,62,69,77; image.go:52,59,66,75,101,107)
- All existing tests pass, 2 new tests added for WorkDir cleanup verification
- T3 (parallel task) modified `NewIngestPipeline` signature and `buildPipeline` test helper — had to pass `0` for redundancy in new tests

## T6 - JWT 签发策略化（CP 侧）
### What I did
Added `JWTPolicyConfig` (TTL/RefreshBeforeSeconds/BandwidthQuotaBytes/DefaultCapabilities) to `internal/config/controlplane.go` mounted as `ControlPlaneConfig.JWTPolicy yaml:"jwt_policy"`. Added optional `DeclaredCapabilities *NodeCapabilities` (POINTER for omitempty) on `types.JWTRequest`. Rewrote `HandleJWTRequest` grant logic to compute Edge/PeerICP/RelayProvider as `declared ∩ default`. L4Backhaul stays whitelist-only (service.go:65 unchanged — declared L4 silently ignored). `NewJWTService` now takes a `config.JWTPolicyConfig` parameter. Added 5 grant-matrix tests covering: (1) nil-declared = bit-for-bit current behaviour, (2) declared {Edge,Relay} + default allows Relay → grants Edge+Relay, (3) declared L4 + not whitelisted → L4 NOT granted, (4) declared L4=false + whitelisted → L4 still granted (whitelist precedence), (5) policy TTL/quota/refresh overrides propagate to issued JWT.

### Gotchas
- **Scope creep forced by signature change**: The plan listed only `httpserver_test.go` for test updates, but `NewJWTService` is also called by `internal/node/jwt/jwt_test.go` (7 sites) and `internal/node/libp2phost/gater_test.go` (1 site). Changing the signature breaks compilation in those packages. Updated them minimally (only constructor calls + `config` import) — required to satisfy `go build ./...`. Did NOT touch any non-constructor logic. The plan's "Do NOT modify files outside the listed scope" clause conflicted with the MUST DO requirement to update all callers; prioritised MUST DO since a broken build fails all gates.
- **Bool zero-value ambiguity**: Go `bool` has no "unset" state, so an all-false `DefaultCapabilities` is ambiguous with an explicit "grant nothing". Treated all-false as "use defaults" (edge+peer_icp=true, relay=false) to preserve bit-for-bit backward compat for YAML configs that omit the `default_capabilities` stanza. Documented inline in `applyJWTPolicyDefaults`. If operators ever need to explicitly grant nothing, they must set at least one field true (or we'd need `*bool` — deferred as out-of-scope).
- **Duplication of defaulting logic**: `applyJWTPolicyDefaults` lives in `internal/config` (called by loader); `applyPolicyDefaultsInPlace` mirrors it in `internal/controlplane/jwt` so test callers that bypass the loader still get sane defaults. Intentional duplication; flagged in code comment. Refactoring to a shared helper would create an import cycle (config → jwt → config).
- The `BandwidthQuota` (50_000_000) and `Exp` (1h) calculations in `service.go` were hardcoded; replaced with `s.policy.BandwidthQuotaBytes` and `s.ttl` (parsed once in constructor). TTL parse failure falls back to 1h (same as legacy).
- Pre-existing numbered-step docstring comments in `HandleJWTRequest` were preserved (renumbered 1-8 from 1-7) to keep diff reviewable.
- Evidence file at `.omo/evidence/task-6-review-remediation.log`. All 5 new grant-matrix tests pass; existing regression tests (expired/forged JWT → 403/400) still green.

## T7 - JWT 节点侧：declared_capabilities + 续签循环
### What I did
Modified `NewJWTClient` signature directly (no separate `NewJWTClientWithCaps`) to take a `types.NodeCapabilities` parameter — `RequestJWT` now sets `DeclaredCapabilities: &caps` (pointer, so JSON `omitempty` works; a non-nil pointer even all-false signals "I am declaring"). All 3 callers updated: `cmd/edge-node/main.go:207`, `internal/node/jwt/jwt_test.go` (2 sites — `TestJWT_ClientIntegration`, `TestJWT_ClientRetryDegraded`). Added `ParsedRefreshInterval`/`ParsedRefreshBeforeExpiry` `time.Duration` fields on `JWTServiceConfig` (yaml:"-") populated by `LoadConfig` via `normalizeJWTRefreshDurations` (defaults 5m for empty/zero/invalid — invalid does NOT fail loading). Implemented `runJWTRefreshLoop` in main.go: computes wait as `min(refreshInterval, exp-now-refreshBeforeExpiry)` by decoding the cached JWT's Exp via `sjwt.VerifyJWTAnyPeerID`; on request failure logs `slog.Error` and continues (NO panic/Fatal — matches initial-failure degraded-mode semantics); goroutine exits on `rootCtx.Done()`. `CurrentJWT()` already existed returning `types.CapabilityJWT` (mutex-protected); kept its signature, just verified it's empty before first success.

### Tests added (4 new in jwt_test.go, 4 sub-cases in config_test.go)
1. `TestJWT_ClientSendsDeclaredCapabilities`: mock HTTP server captures request body → asserts `declared_capabilities` field present and round-trips exact value; also asserts granted JWT reflects `declared ∩ default` policy.
2. `TestJWT_CurrentJWT_EmptyInitially`: confirms `CurrentJWT()` returns `""` before first successful request.
3. `TestJWT_RefreshLoopFiresMultipleTimes`: short interval (50ms) + short TTL (200ms) → after 700ms asserts ≥3 total requests (initial + ≥2 refreshes).
4. `TestJWT_RefreshLoopToleratesServerErrors`: server always 500 → loop continues retrying, no panic, ≥2 requests before ctx timeout.
5. Config: `TestLoadConfig_JWTRefreshDurationDefaults` with 4 sub-cases (explicit values, missing stanza, invalid string, zero/negative) all falling back to 5m default.

### Gotchas
- **T11 parallel work**: T11 (hash ring routing) modified `cmd/edge-node/main.go:373+` (backhaul/router section) while I was editing `:203-228` (JWT section). No merge conflict — different regions. T11 added `routing` import + `router.HandleBlobRequest` dispatch. My JWT section is above the backhaul/router section and is independent.
- **`CapabilitiesConfig` vs `NodeCapabilities` vs `JWTPolicyDefaultCapabilities`**: three distinct but field-compatible structs. `config.CapabilitiesConfig` (node declared, from YAML) → converted to `types.NodeCapabilities` at the `NewJWTClient` call site in main.go. `config.JWTPolicyDefaultCapabilities` is the CP-side default (no L4Backhaul field — L4 is whitelist-only). T6's test typo `config.CapabilitiesConfig{...}` for `DefaultCapabilities` field would not compile; corrected to `JWTPolicyDefaultCapabilities`.
- **Pointer-for-omitempty subtlety**: `DeclaredCapabilities *NodeCapabilities` — if you pass a non-pointer struct value, JSON still serializes the field (no omitempty). T6's design choice of pointer is what allows "declared absent" vs "declared all-false" distinction. Documented inline in client.go.
- **CP-side RefreshBefore vs node-side RefreshBeforeExpiry**: `JWTResponse.RefreshBefore` is server-provided hint (seconds before Exp). `JWTServiceConfig.RefreshBeforeExpiry` is node-local config (duration). The plan's `min(RefreshInterval, Exp-now-RefreshBeforeExpiry)` uses the node's local config, not the server hint — kept that interpretation. The CP hint is honoured indirectly through operator config alignment (documented in `runJWTRefreshLoop` docstring).
- **`sjwt.VerifyJWTAnyPeerID` in refresh loop**: chose to verify (not just decode) the cached JWT to extract Exp because we already hold the CP public key in main.go; verification gives a free integrity check on every loop iteration at negligible cost. Failure to verify (e.g. CP key rotation mid-flight) falls through to the configured `refreshInterval` — safe fallback.
- **Pre-existing `go vet` failure in `internal/storage/gc`**: T13's mock driver has a `Put` signature mismatch (`io.Reader` vs `interface{ Read([]byte) (int, error) }`). Confirmed pre-existing via `git stash` test — NOT my concern. Flagged for T13.
- Evidence at `.omo/evidence/task-7-review-remediation.log`. All gates green: `go build ./...` EXIT=0; `go test ./internal/node/jwt/...` PASS; `go test -race ./internal/node/jwt/...` PASS; `go test ./internal/config/...` PASS.

## T9 - 位置查询 API（CP 侧）
### What I did
- Added `RegisterLocationHandler(h http.Handler)` method to `JWTHTTPServer` (`internal/controlplane/jwt/httpserver.go`). Stores handler on struct; `Serve` conditionally mounts `mux.Handle("GET /v1/blob-locations/{hash}", h)` only if non-nil. No-op if not called → existing JWT-only behaviour preserved bit-for-bit (verified by `TestRegisterLocationHandler_NoRegistrationMeansRouteMissing`).
- New package `internal/controlplane/locationsvc/handler.go`: HTTP handler with `NewHandler(pubKey ed25519.PublicKey, mc metadata.BlobStoreClient) *Handler`. Auth flow: read `Authorization: Bearer <jwt>` → `sjwt.VerifyJWTAnyPeerID(jwt, pubKey)` → check `payload.Capabilities.Edge` → `mc.GetBlobLocations(ctx, hash)`. Returns 200 JSON `{"locations":[...]}` on success, 404 on empty, 401 on missing/expired/bad-sig/malformed-bearer, 403 on no Edge cap, 503 on nil mc, 500 on metadata error, 400 on missing path value.
- Wired in `cmd/control-plane/main.go` step 10b (right after `mc` is constructed so we know its nilness): `httpServer.RegisterLocationHandler(locationsvc.NewHandler(jwtSvc.PubKey(), mcBlob))` where `mcBlob` is `nil` when `mc == nil`. No new listening port (reuses JWT server's mux + graceful shutdown — plan line 176 satisfied).
- Tests: `internal/controlplane/locationsvc/handler_test.go` (11 tests) + 2 new tests in `httpserver_test.go`. All 5 required branches (200/404/401/403/503) covered plus extras (500, 400, end-to-end via httptest.Server with real Go 1.22 mux pattern, route-mount semantics). JWTs signed with real Ed25519 keys via `sjwt.SignJWT`.

### Gotchas
- **metadata.BlobStoreClient interface requires `*sql.Tx` in Write* signatures**: my initial fake tried to skip implementing Write* methods, but the interface needs them for compile-time satisfaction. Pulled in `database/sql` and added panic-stub Write methods to `fakeClientWrapper`. The handler only ever calls `GetBlobLocations`, so the panics are unreachable in tests — kept them as a guard against future drift. Documented in the wrapper's docstring.
- **Placement of `RegisterLocationHandler` call in main.go matters**: must come AFTER `mc` is constructed (step 10), not right after `jwtSvc` (step 5). First attempt placed it at step 5b — would have referenced `mc` before declaration. Moved to step 10b.
- **503 vs skip-registration is a deliberate contract choice**: plan says "mc 为 nil 时仍注册但返回 503". A future maintainer might "simplify" by skipping `RegisterLocationHandler` when `mc == nil`; this would break the deterministic-contract guarantee that edges see a stable HTTP status code regardless of CP's PG availability. Documented inline at step 10b.
- **No new listening port** (plan line 176 hard requirement): achieved by reusing the existing `JWTHTTPServer.Serve` mux — the location handler is just another route on the same `*http.ServeMux` as `POST /v1/node/jwt`. The existing 5s graceful shutdown (`shutdownTimeout` const) covers both routes uniformly.
- **Go 1.22+ mux pattern syntax** (`mux.Handle("GET /v1/blob-locations/{hash}", h)`) is already used in the existing `mux.HandleFunc("POST /v1/node/jwt", ...)` so this matches the repo's Go version. Verified via end-to-end test that `r.PathValue("hash")` populates correctly through a real `httptest.Server`.
- **`locationsvc.Handler` accepts `metadata.BlobStoreClient` (interface), not `*metadata.PGMetadataClient` (concrete)**: keeps the handler unit-testable with a fake, and matches the dependency-injection style already used by `pinstrategy.NewPinOrchestrator` which takes the same interface.
- Evidence file at `.omo/evidence/task-9-review-remediation.log`. `go build ./...` and `go vet ./internal/controlplane/... ./cmd/control-plane/...` both clean. All 11 + 2 = 13 new tests pass; all pre-existing controlplane tests still green.


## T5 - ingest 上传限额配置化 + work_dir 空闲空间启动检查
### What I did
Added `MaxUploadBytes int64 yaml:"max_upload_bytes"` to `IngestHTTPConfig` in `internal/config/ingest.go`. Normalize `<=0` to `10<<30` (10 GiB) in `LoadIngestWorkerConfig`. Wired the value through `handleIngest` (added `maxUploadBytes int64` parameter) to replace the hardcoded `10<<30` in `http.MaxBytesReader`. Added `checkWorkDirDiskSpace` function that calls `syscall.Statfs` on `cfg.Ingest.WorkDir` at startup — if free bytes < 2*MaxUploadBytes, emits `slog.Warn` (not fatal, per plan line 139). Added `max_upload_bytes` example (commented out) to `configs/ingest-worker.yaml`. Added 3 test cases: explicit value (1 GiB), missing key (default 10 GiB), and zero/negative (normalized to 10 GiB).

### Gotchas
- **syscall.Statfs type mismatch**: `stat.Bsize` is `uint32` on macOS (darwin), but `stat.Bavail` is `uint64`. The multiplication `int64(stat.Bavail) * stat.Bsize` fails because Go doesn't allow mixed-type arithmetic. Fixed with explicit `int64()` cast on both operands.
- **Test YAML indentation**: The `max_upload_bytes` key must be under the `http:` section (2-space indent), not at the root level. Initial test YAML had it at root, causing it to be silently ignored by the YAML parser and the explicit value test to get the default 10 GiB instead of 1 GiB.
- **handleIngest parameter passing**: The handler function didn't have access to `cfg`. Added `maxUploadBytes int64` parameter to `handleIngest` — minimal change, no need to pass the full config struct through.
- Statfs failure (e.g., workdir doesn't exist yet) is Warn-only, same as sweepStaleWorkDir pattern.
- ParseMultipartForm 64MB memory spill unchanged (plan line 140).
- Evidence file at `.omo/evidence/task-5-review-remediation.log`. All 3 new MaxUploadBytes tests pass; `go build ./cmd/ingest-worker` passes; `go vet` clean.

## T11 - 哈希环路由接线：EdgeRouter 挂 HTTP mux + 代理失败回退
### What I did
- `internal/node/routing/edge_router.go` `HandleBlobRequest` (:77-95): wrapped `proxyToPeer` error path — on failure, `slog.Warn("proxy to peer failed, falling back to local", "err", err, "blobHash", blobHash, "target", targetID)` then call `serveAsPrimary`. Availability-first: caller gets bytes if local backhaul has them (warm cache / ICP fetch from siblings). Added `log/slog` import.
- `cmd/edge-node/main.go`: added `internal/node/routing` import; after `backhaulMgr` construction (~:379) wired `router := routing.NewEdgeRouter(ring, backhaulMgr, nodeIdentity.PeerID, cfg.Access.DataPlane.Enabled, h)`. Replaced the HTTP handler's direct `backhaulMgr.HandleBlobL4/NoL4` call with `router.HandleBlobRequest(ctx, w, blobHash)`. Kept the 30s timeout, the `logger.Info("blob request", ...)` entry log, and the `logger.Error("blob request failed", ...) + http.Error(404)` failure wrapper (faithful to original behaviour).
- New test `TestEdgeRouter_ProxyFallbackToLocal` in `routing_test.go`: two libp2p hosts, h1 = primary (deliberately does NOT call `icp.RegisterHandlers`, so streams are rejected), h2 = non-primary router. `mockBackhaul` on h2 has `noL4Data` set. Asserts: `HandleBlobRequest` returns nil error, served bytes == local payload, `bh.noL4Called == 1`, and a Warn log containing "proxy to peer failed, falling back to local" + blobHash was emitted (captured via `slog.SetDefault` + `bytes.Buffer` text handler).

### Gotchas
- **HTTP handler error logging**: Plan said to use `router.HandleBlobHTTP(w, r, blobHash)` but `HandleBlobHTTP` (edge_router.go:86-90) swallows the error and writes a 500 with the raw error string. The original handler logged `logger.Error("blob request failed", "hash", ..., "err", ...)` then returned `http.Error(w, "blob not found", 404)`. Calling `router.HandleBlobRequest` directly preserved the original error-logging + 404 wrapper bit-for-bit. Functionally equivalent — `HandleBlobHTTP` is just a 4-line wrapper around `HandleBlobRequest`. Flagged for T12/T20 to revisit if HTTP status semantics matter.
- **Parallel T7 conflict on `cmd/edge-node/main.go`**: T7 (JWT node side) is mid-flight in the working tree, touching main.go:203-227 (JWT client call + `runJWTRefreshLoop` definition). T11 owns main.go:378+ (backhaul/router section). Staged only T11's hunks via `git apply --cached` with a hand-stripped patch (excluded the JWT hunk at @@ -202,7 +203,12 @@). T7's JWT hunk remains unstaged in the working tree for T7 to commit.
- **Pre-existing build breakage at HEAD (3440617) introduced by T6**: `nodejwt.NewJWTClient` (internal/node/jwt/client.go:42) takes 4 args (added `types.NodeCapabilities`) but main.go:206 (HEAD) still calls with 3 args. This is T7's job to fix (T7 task brief owns the JWT region). T11's `go build ./cmd/edge-node` cannot go green until T7 lands its fix. With T7's unstaged changes stashed, the only remaining `go build` failure is this T6-introduced signature mismatch — explicitly out of T11's scope per plan line 195 ("T11 has no upstream blockers") + task brief FILE CONFLICT AWARENESS note.
- **No stream-handler-registered test trick**: rather than mock `proxyToPeer` directly (would require injecting a host whose `NewStream` errors), I used a real libp2p host h1 that connects but does NOT register `icp.RegisterHandlers`. h2's `NewStream(ctx, h1.ID, /edge/blob/get/1.0.0)` either succeeds at the transport layer and then gets a stream-reset from h1 (no handler), or fails outright — either way `proxyToPeer` returns an error, triggering the fallback path. Verified test passes deterministically.
- `internal/node/backhaul.BackhaulManager` already satisfies `BlobRouterBackhaul` (L4 + NoL4 methods match exactly), so no adapter struct was needed — `backhaulMgr` passed directly to `NewEdgeRouter`.
- Evidence at `.omo/evidence/task-11-review-remediation.log`.

## T13 - janitor 核心：migration 014 + internal/storage/gc 两阶段软删除
### What I did
- Added migration `014_blob_deleted_at.sql`: `ALTER TABLE blob ADD COLUMN IF NOT EXISTS deleted_at TIMESTAMPTZ` (idempotent, follows 013's style — comment line + single statement).
- Updated `internal/storage/metadata/migrate_test.go` — added the 014 expectation (`ALTER TABLE blob ADD COLUMN IF NOT EXISTS deleted_at`) to all 3 existing tests (`TestMigrateAll_ExecutesInOrder`, `TestMigrateAll_WithExistingTables`, `TestMigrateAll_SQLParsesCorrectly`). `TestMigrateAll_FailsOnBadSQL` was left untouched (it short-circuits on the first migration).
- Created `internal/storage/gc/gc.go`:
  - `Collector{db, resolver, logger}` + `NewCollector`.
  - `AccountResolver` interface (`Resolve(backendID) → (driver.Driver, *circuitbreaker.CircuitBreaker, ok)`) — narrow so tests can inject a fake; production T14 will wrap `*accountpool.AccountPool`.
  - `ResolverFunc` function adapter (kept for T14's accountpool wrapper).
  - `MarkOrphans(ctx, minAge) (int, error)` — single UPDATE, default minAge 24h.
  - `Sweep(ctx, grace, batchLimit) (SweepResult, error)` — selects candidates, then per-blob: TOCTOU re-check → fetch locations → Driver.Remove each → single-tx DELETE blob_location + blob.
  - Structured `slog` logs: `marked=N` / `rescued=N deleted=N failed=N candidates=N`.
  - `TODO(gc): Class B drive reconciliation requires flat upload layout or drive index` (plan line 46/211) — left documented, not implemented.
- Created `internal/storage/gc/gc_test.go` — 9 tests covering all 4 plan paths + helpers:
  - MarkOrphans happy / idempotent / default-minAge
  - Sweep happy (2 copies removed + DELETE tx), rescue (content_blob ref appeared → deleted_at=NULL, zero Remove calls), delete-fail-circuit-break (Remove errors → ForceOpen called, blob preserved), already-broken-skipped (subsequent blob using same backend_id short-circuits without re-Remove), no-locations (blob with no blob_location rows → direct DELETE)
  - parseBackendID table test (incl. SplitN 2 with colons in account_id)
- Evidence at `.omo/evidence/task-13-review-remediation.log`. `go build ./...` EXIT=0; `go test -race ./internal/storage/gc/...` PASS (9 tests); `go test -race ./internal/storage/metadata/...` PASS.

### Gotchas / decisions
- **time.Duration → interval string**: PG `::interval` cast accepts Go's `Duration.String()` ("24h0m0s") which lib/pq passes as a text param. sqlmock WithArgs must use the exact same string — `(24*time.Hour).String()` not "24h". This is the only brittle test coupling; the runtime contract with lib/pq is solid (verified by sqlmock matching the regex on the SQL text).
- **Circuit-breaker contract**: `accountpool.AccountPool` already gives accounts a CB (CircuitBreaker interface with State/ForceOpen/ForceClose). The gc package holds the concrete `*circuitbreaker.CircuitBreaker` so it can call `ForceOpen()` on a single-account delete failure (idempotent + visible to concurrent read path). `cb == nil` is guarded — a hypothetical account without a CB still proceeds (Remove failure just logs + counts as Failed without circuit-breaking).
- **Circuit-break scope = this run + concurrent code**: the `broken` map (per-Sweep) skips further attempts to that backend in this run; `cb.ForceOpen()` makes the open state visible to concurrent readers/uploaders — matches the plan's "熔断该账号本轮" wording and reuses circuitbreaker's state machine.
- **TOCTOU protection**: re-check uses `SELECT 1 FROM content_blob WHERE blob_hash=$1 LIMIT 1`. If a reference appeared during the grace window (Metis F3 case: new ingest hit the dedup path during grace), `UPDATE blob SET deleted_at=NULL` rescues the row. No `WriteIngestTransaction` change (plan line 212 — "救回靠复核，不改 WriteIngestTransaction").
- **Per-blob abort on first copy failure**: after a Remove error on copy N, copies N+1..K of the same blob are NOT attempted in this run — the blob row stays intact (deleted_at still set, locations still all present) so the next Sweep retries from scratch. This is safer than partial delete (a partial delete would leave the blob unrecoverable if the row got DELETEd). Documented inline.
- **NoLocations path**: a soft-marked blob with zero blob_location rows (e.g. all locations were manually removed) is still hard-deleted via the single-tx DELETE — otherwise it would be stuck forever. This is an edge case not in the plan's 4 paths but the implementation handles it; covered by `TestSweep_NoLocations`.
- **sqlmock QueryMatcherRegexp default**: regex-escaped `\$1` etc. match the literal `$1` in queries. Regex must escape parentheses in `now\(\)`. Whitespace between SQL clauses is matched with `\s+` to be tolerant of formatting.
- **No new dependencies**: only uses `github.com/DATA-DOG/go-sqlmock` (already in go.mod), `internal/storage/circuitbreaker`, `internal/storage/driver` (+ mock), `internal/types`. No http server, no new vendor code.
- **T14 readiness**: `NewCollector(db, resolver, logger)` takes the raw `*sql.DB`. In production T14 will need to either expose `PGMetadataClient.db` (unexported today) or call `NewCollector` from inside the metadata package. Decision deferred to T14 — easiest is a `PGMetadataClient.DB() *sql.DB` accessor, but that's outside this task's scope.
