package migrations_test

import (
	"context"
	"database/sql"
	"testing"
	"time"

	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/postgres"
	"github.com/testcontainers/testcontainers-go/wait"

	"pg-roll/pkg/migrations"
	"pg-roll/pkg/roll"
	"pg-roll/pkg/state"
)

type TestCase struct {
	name           string
	migrations     []migrations.Migration
	beforeComplete func(t *testing.T, db *sql.DB)
	afterComplete  func(t *testing.T, db *sql.DB)
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

			// run the beforeComplete hook
			if tt.beforeComplete != nil {
				tt.beforeComplete(t, db)
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

func TableMustExist(t *testing.T, db *sql.DB, schema, version, table string) {
	if !tableExists(t, db, schema, version, table) {
		t.Fatalf("Expected view to exist")
	}
}

func TableMustNotExist(t *testing.T, db *sql.DB, schema, version, table string) {
	if tableExists(t, db, schema, version, table) {
		t.Fatalf("Expected view to not exist")
	}
}

func tableExists(t *testing.T, db *sql.DB, schema, version, table string) bool {
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
