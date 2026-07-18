package metadata

import (
	"database/sql"
	"embed"
	"fmt"
	"io/fs"
	"sort"
	"strings"
)

//go:embed migrations/*.sql
var migrationFS embed.FS

// migrateAll reads all embedded SQL migration files sorted by name and
// executes each against db. Returns the first error encountered; on error
// the migration is aborted but already-applied files are not rolled back.
func migrateAll(db *sql.DB) error {
	entries, err := fs.ReadDir(migrationFS, "migrations")
	if err != nil {
		return fmt.Errorf("migrations: read dir: %w", err)
	}

	var names []string
	for _, e := range entries {
		if !e.IsDir() && strings.HasSuffix(e.Name(), ".sql") {
			names = append(names, e.Name())
		}
	}
	sort.Strings(names)

	for _, name := range names {
		raw, err := migrationFS.ReadFile("migrations/" + name)
		if err != nil {
			return fmt.Errorf("migrations: read %s: %w", name, err)
		}
		stmt := strings.TrimSpace(string(raw))
		if stmt == "" {
			continue
		}
		if _, err := db.Exec(stmt); err != nil {
			return fmt.Errorf("migrations: execute %s: %w", name, err)
		}
	}
	return nil
}