package roll_test

import (
	"context"
	"database/sql"
	"testing"
	"time"

	"pg-roll/pkg/migrations"
	"pg-roll/pkg/roll"
	"pg-roll/pkg/state"

	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/postgres"
	"github.com/testcontainers/testcontainers-go/wait"
)

const (
	postgresImage = "postgres:15.3"
)

func TestPreviousVersionIsDroppedAfterMigrationCompletion(t *testing.T) {
	t.Parallel()

	withMigratorAndConnectionToContainer(t, func(mig *roll.Roll, db *sql.DB) {
		ctx := context.Background()

		const (
			firstVersion  = "1_create_table"
			secondVersion = "2_create_table"
		)
		if err := mig.Start(ctx, &migrations.Migration{Name: firstVersion, Operations: migrations.Operations{createTableOp("table1")}}); err != nil {
			t.Fatalf("Failed to start first migration: %v", err)
		}
		if err := mig.Complete(ctx); err != nil {
			t.Fatalf("Failed to complete first migration: %v", err)
		}
		if err := mig.Start(ctx, &migrations.Migration{Name: secondVersion, Operations: migrations.Operations{createTableOp("table2")}}); err != nil {
			t.Fatalf("Failed to start second migration: %v", err)
		}
		if err := mig.Complete(ctx); err != nil {
			t.Fatalf("Failed to complete second migration: %v", err)
		}

		//
		// Check that the schema for the first version is dropped
		//
		var exists bool
		err := db.QueryRow(`
    SELECT EXISTS(
      SELECT 1
      FROM pg_catalog.pg_namespace
      WHERE nspname = $1
    )`, firstVersion).Scan(&exists)
		if err != nil {
			t.Fatal(err)
		}

		if exists {
			t.Errorf("Expected schema %q to not exist", firstVersion)
		}
	})
}

func createTableOp(tableName string) *migrations.OpCreateTable {
	return &migrations.OpCreateTable{
		Name: tableName,
		Columns: []migrations.Column{
			{
				Name:       "id",
				Type:       "integer",
				PrimaryKey: true,
			},
			{
				Name:   "name",
				Type:   "varchar(255)",
				Unique: true,
			},
		},
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
