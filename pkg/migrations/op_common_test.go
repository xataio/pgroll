package migrations_test

import (
	"database/sql"
	"testing"

	"pg-roll/pkg/roll"
)

func ViewMustExist(t *testing.T, db *sql.DB, schema, version, table string) {
	if !viewExists(t, db, schema, version, table) {
		t.Fatalf("Expected view to exist")
	}
}

func ViewMustNotExist(t *testing.T, db *sql.DB, schema, version, table string) {
	if viewExists(t, db, schema, version, table) {
		t.Fatalf("Expected view to not exist")
	}
}

func viewExists(t *testing.T, db *sql.DB, schema, version, table string) bool {
	versionSchema := roll.VersionedSchemaName(schema, version)
	var exists bool
	err := db.QueryRow(`
		SELECT EXISTS (
			SELECT 1
			FROM pg_catalog.pg_views
			WHERE schemaname = $1
			AND viewname = $2
		)`,
		versionSchema, table).Scan(&exists)
	if err != nil {
		t.Fatal(err)
	}
	return exists
}
