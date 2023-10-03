// SPDX-License-Identifier: Apache-2.0

package roll_test

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/lib/pq"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/postgres"
	"github.com/testcontainers/testcontainers-go/wait"
	"github.com/xataio/pgroll/pkg/migrations"
	"github.com/xataio/pgroll/pkg/roll"
	"github.com/xataio/pgroll/pkg/state"
)

const (
	schema = "public"
	// The version of postgres against which the tests are run
	// if the POSTGRES_VERSION environment variable is not set.
	defaultPostgresVersion = "15.3"
)

func TestSchemaIsCreatedfterMigrationStart(t *testing.T) {
	t.Parallel()

	withMigratorAndConnectionToContainer(t, func(mig *roll.Roll, db *sql.DB) {
		ctx := context.Background()
		version := "1_create_table"

		if err := mig.Start(ctx, &migrations.Migration{Name: version, Operations: migrations.Operations{createTableOp("table1")}}); err != nil {
			t.Fatalf("Failed to start migration: %v", err)
		}

		//
		// Check that the schema exists
		//
		var exists bool
		err := db.QueryRow(`
    SELECT EXISTS(
      SELECT 1
      FROM pg_catalog.pg_namespace
      WHERE nspname = $1
    )`, roll.VersionedSchemaName(schema, version)).Scan(&exists)
		if err != nil {
			t.Fatal(err)
		}

		if !exists {
			t.Errorf("Expected schema %q to exist", version)
		}
	})
}

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
		// Check that the schema for the first version has been dropped
		//
		var exists bool
		err := db.QueryRow(`
    SELECT EXISTS(
      SELECT 1
      FROM pg_catalog.pg_namespace
      WHERE nspname = $1
    )`, roll.VersionedSchemaName(schema, firstVersion)).Scan(&exists)
		if err != nil {
			t.Fatal(err)
		}

		if exists {
			t.Errorf("Expected schema %q to not exist", firstVersion)
		}
	})
}

func TestSchemaIsDroppedAfterMigrationRollback(t *testing.T) {
	t.Parallel()

	withMigratorAndConnectionToContainer(t, func(mig *roll.Roll, db *sql.DB) {
		ctx := context.Background()
		version := "1_create_table"

		if err := mig.Start(ctx, &migrations.Migration{Name: version, Operations: migrations.Operations{createTableOp("table1")}}); err != nil {
			t.Fatalf("Failed to start migration: %v", err)
		}
		if err := mig.Rollback(ctx); err != nil {
			t.Fatalf("Failed to rollback migration: %v", err)
		}

		//
		// Check that the schema has been dropped
		//
		var exists bool
		err := db.QueryRow(`
    SELECT EXISTS(
      SELECT 1
      FROM pg_catalog.pg_namespace
      WHERE nspname = $1
    )`, roll.VersionedSchemaName(schema, version)).Scan(&exists)
		if err != nil {
			t.Fatal(err)
		}

		if exists {
			t.Errorf("Expected schema %q to not exist", version)
		}
	})
}

func TestSchemaOptionIsRespected(t *testing.T) {
	t.Parallel()

	withMigratorInSchemaAndConnectionToContainer(t, "schema1", func(mig *roll.Roll, db *sql.DB) {
		ctx := context.Background()
		version := "1_create_table"

		if err := mig.Start(ctx, &migrations.Migration{Name: version, Operations: migrations.Operations{createTableOp("table1")}}); err != nil {
			t.Fatalf("Failed to start migration: %v", err)
		}
		if err := mig.Complete(ctx); err != nil {
			t.Fatalf("Failed to complete migration: %v", err)
		}

		//
		// Check that the table exists in the correct schema
		//
		var exists bool
		err := db.QueryRow(`
    SELECT EXISTS(
      SELECT 1
      FROM pg_catalog.pg_tables
      WHERE tablename = $1
      AND schemaname = $2
    )`, "table1", "schema1").Scan(&exists)
		if err != nil {
			t.Fatal(err)
		}

		if !exists {
			t.Errorf("Expected table %q to exist in schema %q", "table1", "schema1")
		}
	})
}

func TestLockTimeoutIsEnforced(t *testing.T) {
	t.Parallel()

	withMigratorInSchemaWithLockTimeoutAndConnectionToContainer(t, "public", 100, func(mig *roll.Roll, db *sql.DB) {
		ctx := context.Background()

		// Start a create table migration
		err := mig.Start(ctx, &migrations.Migration{
			Name:       "01_create_table",
			Operations: migrations.Operations{createTableOp("table1")},
		})
		if err != nil {
			t.Fatalf("Failed to start migration: %v", err)
		}

		// Complete the create table migration
		if err := mig.Complete(ctx); err != nil {
			t.Fatalf("Failed to complete migration: %v", err)
		}

		// Start a transaction and take an ACCESS_EXCLUSIVE lock on the table
		// Don't commit the transaction so that the lock is held indefinitely
		tx, err := db.Begin()
		if err != nil {
			t.Fatalf("Failed to start transaction: %v", err)
		}
		t.Cleanup(func() {
			tx.Commit()
		})
		if _, err := tx.ExecContext(ctx, "LOCK TABLE table1 IN ACCESS EXCLUSIVE MODE"); err != nil {
			t.Fatalf("Failed to take ACCESS_EXCLUSIVE lock on table: %v", err)
		}

		// Attempt to run a second migration on the table while the lock is held
		// The migration should fail due to a lock timeout error
		err = mig.Start(ctx, &migrations.Migration{
			Name:       "02_create_table",
			Operations: migrations.Operations{addColumnOp("table1")},
		})
		if err == nil {
			t.Fatalf("Expected migration to fail due to lock timeout")
		}
		if err != nil {
			pqErr := &pq.Error{}
			if ok := errors.As(err, &pqErr); !ok {
				t.Fatalf("Migration failed with unexpected error: %v", err)
			}
			if pqErr.Code != "55P03" { // Lock not available error code
				t.Fatalf("Migration failed with unexpected error: %v", err)
			}
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

func addColumnOp(tableName string) *migrations.OpAddColumn {
	return &migrations.OpAddColumn{
		Table: tableName,
		Column: migrations.Column{
			Name:     "age",
			Type:     "integer",
			Nullable: true,
		},
	}
}

func withMigratorInSchemaWithLockTimeoutAndConnectionToContainer(t *testing.T, schema string, lockTimeoutMs int, fn func(mig *roll.Roll, db *sql.DB)) {
	t.Helper()
	ctx := context.Background()

	waitForLogs := wait.
		ForLog("database system is ready to accept connections").
		WithOccurrence(2).
		WithStartupTimeout(5 * time.Second)

	pgVersion := os.Getenv("POSTGRES_VERSION")
	if pgVersion == "" {
		pgVersion = defaultPostgresVersion
	}

	ctr, err := postgres.RunContainer(ctx,
		testcontainers.WithImage("postgres:"+pgVersion),
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

	mig, err := roll.New(ctx, cStr, schema, lockTimeoutMs, st)
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

	_, err = db.ExecContext(ctx, fmt.Sprintf("CREATE SCHEMA IF NOT EXISTS %s", schema))
	if err != nil {
		t.Fatal(err)
	}

	fn(mig, db)
}

func withMigratorInSchemaAndConnectionToContainer(t *testing.T, schema string, fn func(mig *roll.Roll, db *sql.DB)) {
	withMigratorInSchemaWithLockTimeoutAndConnectionToContainer(t, schema, 500, fn)
}

func withMigratorAndConnectionToContainer(t *testing.T, fn func(mig *roll.Roll, db *sql.DB)) {
	withMigratorInSchemaWithLockTimeoutAndConnectionToContainer(t, "public", 500, fn)
}
