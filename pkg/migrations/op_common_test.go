package migrations_test

import (
	"context"
	"database/sql"
	"fmt"
	"testing"
	"time"

	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/postgres"
	"github.com/testcontainers/testcontainers-go/wait"
	"golang.org/x/exp/maps"

	"pg-roll/pkg/migrations"
	"pg-roll/pkg/roll"
	"pg-roll/pkg/state"
)

type TestCase struct {
	name          string
	migrations    []migrations.Migration
	afterStart    func(t *testing.T, db *sql.DB)
	afterComplete func(t *testing.T, db *sql.DB)
	afterRollback func(t *testing.T, db *sql.DB)
}

type TestCases []TestCase

func ExecuteTests(t *testing.T, tests TestCases) {
	for _, tt := range tests {
		withMigratorAndConnectionToContainer(t, func(mig *roll.Roll, db *sql.DB) {
			ctx := context.Background()

			// run all migrations except the last one
			for i := 0; i < len(tt.migrations)-1; i++ {
				if err := mig.Start(ctx, &tt.migrations[i]); err != nil {
					t.Fatalf("Failed to start migration: %v", err)
				}

				if err := mig.Complete(ctx); err != nil {
					t.Fatalf("Failed to complete migration: %v", err)
				}
			}

			// start the last migration
			if err := mig.Start(ctx, &tt.migrations[len(tt.migrations)-1]); err != nil {
				t.Fatalf("Failed to start migration: %v", err)
			}

			// run the afterStart hook
			if tt.afterStart != nil {
				tt.afterStart(t, db)
			}

			// roll back the migration if a rollback hook is provided
			if tt.afterRollback != nil {
				if err := mig.Rollback(ctx); err != nil {
					t.Fatalf("Failed to roll back migration: %v", err)
				}

				// run the afterRollback hook
				tt.afterRollback(t, db)

				// re-start the last migration
				if err := mig.Start(ctx, &tt.migrations[len(tt.migrations)-1]); err != nil {
					t.Fatalf("Failed to start migration: %v", err)
				}
			}

			// complete the last migration
			if err := mig.Complete(ctx); err != nil {
				t.Fatalf("Failed to complete migration: %v", err)
			}

			// run the afterComplete hook
			if tt.afterComplete != nil {
				tt.afterComplete(t, db)
			}
		})
	}
}

func withMigratorAndConnectionToContainer(t *testing.T, fn func(mig *roll.Roll, db *sql.DB)) {
	t.Helper()
	ctx := context.Background()

	waitForLogs := wait.
		ForLog("database system is ready to accept connections").
		WithOccurrence(2).
		WithStartupTimeout(5 * time.Second)

	ctr, err := postgres.RunContainer(ctx,
		testcontainers.WithImage(postgresImage),
		testcontainers.WithWaitStrategy(waitForLogs),
	)
	if err != nil {
		t.Fatal(err)
	}

	t.Cleanup(func() {
		if err := ctr.Terminate(ctx); err != nil {
			t.Fatalf("Failed to terminate container: %v", err)
		}
	})

	cStr, err := ctr.ConnectionString(ctx, "sslmode=disable")
	if err != nil {
		t.Fatal(err)
	}

	st, err := state.New(ctx, cStr, "pgroll")
	if err != nil {
		t.Fatal(err)
	}
	err = st.Init(ctx)
	if err != nil {
		t.Fatal(err)
	}
	mig, err := roll.New(ctx, cStr, "public", st)
	if err != nil {
		t.Fatal(err)
	}

	t.Cleanup(func() {
		if err := mig.Close(); err != nil {
			t.Fatalf("Failed to close migrator connection: %v", err)
		}
	})

	db, err := sql.Open("postgres", cStr)
	if err != nil {
		t.Fatal(err)
	}

	t.Cleanup(func() {
		if err := db.Close(); err != nil {
			t.Fatalf("Failed to close database connection: %v", err)
		}
	})

	fn(mig, db)
}

// Common assertions

func ViewMustExist(t *testing.T, db *sql.DB, schema, version, view string) {
	t.Helper()
	if !viewExists(t, db, schema, version, view) {
		t.Fatalf("Expected view to exist")
	}
}

func ViewMustNotExist(t *testing.T, db *sql.DB, schema, version, view string) {
	t.Helper()
	if viewExists(t, db, schema, version, view) {
		t.Fatalf("Expected view to not exist")
	}
}

func viewExists(t *testing.T, db *sql.DB, schema, version, view string) bool {
	t.Helper()
	versionSchema := roll.VersionedSchemaName(schema, version)
	var exists bool
	err := db.QueryRow(`
		SELECT EXISTS (
			SELECT 1
			FROM pg_catalog.pg_views
			WHERE schemaname = $1
			AND viewname = $2
		)`,
		versionSchema, view).Scan(&exists)
	if err != nil {
		t.Fatal(err)
	}
	return exists
}

func MustInsert(t *testing.T, db *sql.DB, schema, version, table string, record map[string]string) {
	t.Helper()
	versionSchema := roll.VersionedSchemaName(schema, version)

	recordStr := "("
	for i, k := range maps.Keys(record) {
		if i > 0 {
			recordStr += ", "
		}
		recordStr += k
	}
	recordStr += ") VALUES ("
	for i, v := range maps.Values(record) {
		if i > 0 {
			recordStr += ", "
		}
		recordStr += fmt.Sprintf("'%s'", v)
	}
	recordStr += ")"

	//nolint:gosec // this is a test so we don't care about SQL injection
	stmt := fmt.Sprintf("INSERT INTO %s.%s %s", versionSchema, table, recordStr)

	_, err := db.Exec(stmt)
	if err != nil {
		t.Fatal(err)
	}
}

func MustSelect(t *testing.T, db *sql.DB, schema, version, table string) []map[string]string {
	t.Helper()
	versionSchema := roll.VersionedSchemaName(schema, version)

	//nolint:gosec // this is a test so we don't care about SQL injection
	selectStmt := fmt.Sprintf("SELECT * FROM %s.%s", versionSchema, table)

	q, err := db.Query(selectStmt)
	if err != nil {
		t.Fatal(err)
	}

	res := make([]map[string]string, 0)

	for q.Next() {
		cols, err := q.Columns()
		if err != nil {
			t.Fatal(err)
		}
		values := make([]string, len(cols))
		valuesPtr := make([]any, len(cols))
		for i := range values {
			valuesPtr[i] = &values[i]
		}
		if err := q.Scan(valuesPtr...); err != nil {
			t.Fatal(err)
		}

		row := map[string]string{}
		for i, col := range cols {
			row[col] = values[i]
		}

		res = append(res, row)
	}

	return res
}
