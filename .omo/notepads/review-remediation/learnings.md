
## [2026-07-19] Task: T1
### What I did
Moved `internal/controlplane/metadata` → `internal/storage/metadata` via `git mv` (preserving history). Updated 7 import sites + 1 comment. Ran all verification gates.

### Gotchas
- `isolation_test.go` had a trailing comma in the import string (the grep matched `"controlplane/metadata",` with a comma). The edit had to match the exact string including the comma.
- The old `internal/controlplane/metadata/` directory is automatically removed by `git mv` — no manual cleanup needed.
- `go test ./...` runs cached results; the integration test was re-run with `-count=1` to force a fresh run.
- Evidence file at `.omo/evidence/task-1-review-remediation.log`.
