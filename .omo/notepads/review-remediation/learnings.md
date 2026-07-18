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
