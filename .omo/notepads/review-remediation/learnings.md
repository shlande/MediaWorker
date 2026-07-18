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
