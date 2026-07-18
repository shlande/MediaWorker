
## [2026-07-19] Task: T1
### What I did
Moved `internal/controlplane/metadata` → `internal/storage/metadata` via `git mv` (preserving history). Updated 7 import sites + 1 comment. Ran all verification gates.

### Gotchas
- `isolation_test.go` had a trailing comma in the import string (the grep matched `"controlplane/metadata",` with a comma). The edit had to match the exact string including the comma.
- The old `internal/controlplane/metadata/` directory is automatically removed by `git mv` — no manual cleanup needed.
- `go test ./...` runs cached results; the integration test was re-run with `-count=1` to force a fresh run.
- Evidence file at `.omo/evidence/task-1-review-remediation.log`.

## [2026-07-19] Task: T2
### What I did
Exported `migrateAll` → `MigrateAll` (capitalize) in `migrate.go`. Updated all 4 call sites in `migrate_test.go`. Inserted `MigrateAll(db)` call in `NewPGMetadataClient` after `db.Ping()` success, before return — with proper error propagation (close db, return nil+err).

### Gotchas
- The `MigrateAll` call on migration failure must close the db connection before returning error, same pattern as the Ping failure path.
- `go test ./...` uses cache; `-count=1` forces a fresh run.
- Evidence file at `.omo/evidence/task-2-review-remediation.log`.
