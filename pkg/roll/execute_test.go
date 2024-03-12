// SPDX-License-Identifier: Apache-2.0

package roll_test

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"testing"

	"github.com/lib/pq"
	"github.com/stretchr/testify/assert"
	"github.com/xataio/pgroll/pkg/migrations"
	"github.com/xataio/pgroll/pkg/roll"
	"github.com/xataio/pgroll/pkg/state"
	"github.com/xataio/pgroll/pkg/testutils"
)

const (
	schema = "public"
)

func TestMain(m *testing.M) {
	testutils.SharedTestMain(m)
}

func TestSchemaIsCreatedAfterMigrationStart(t *testing.T) {
	t.Parallel()

	testutils.WithMigratorAndConnectionToContainer(t, func(mig *roll.Roll, db *sql.DB) {
		ctx := context.Background()
		version := "1_create_table"

		if err := mig.Start(ctx, &migrations.Migration{Name: version, Operations: migrations.Operations{createTableOp("table1")}}); err != nil {
			t.Fatalf("Failed to start migration: %v", err)
		}

		//
		// Check that the schema exists
		//
		if !schemaExists(t, db, roll.VersionedSchemaName(schema, version)) {
			t.Errorf("Expected schema %q to exist", version)
		}
	})
}

func TestDisabledSchemaManagement(t *testing.T) {
	t.Parallel()

	testutils.WithMigratorInSchemaAndConnectionToContainerWithOptions(t, "public", []roll.Option{roll.WithDisableViewsManagement()}, func(mig *roll.Roll, db *sql.DB) {
		ctx := context.Background()
		version := "1_create_table"

		if err := mig.Start(ctx, &migrations.Migration{Name: version, Operations: migrations.Operations{createTableOp("table1")}}); err != nil {
			t.Fatalf("Failed to start migration: %v", err)
		}

		//
		// Check that the schema doesn't get created
		//
		if schemaExists(t, db, roll.VersionedSchemaName(schema, version)) {
			t.Errorf("Expected schema %q to not exist", version)
		}

		if err := mig.Rollback(ctx); err != nil {
			t.Fatalf("Failed to rollback migration: %v", err)
		}

		if err := mig.Start(ctx, &migrations.Migration{Name: version, Operations: migrations.Operations{createTableOp("table1")}}); err != nil {
			t.Fatalf("Failed to start migration again: %v", err)
		}

		// complete the migration, check that the schema still doesn't exist
		if err := mig.Complete(ctx); err != nil {
			t.Fatalf("Failed to complete migration: %v", err)
		}

		if schemaExists(t, db, roll.VersionedSchemaName(schema, version)) {
			t.Errorf("Expected schema %q to not exist", version)
		}
	})
}

func TestPreviousVersionIsDroppedAfterMigrationCompletion(t *testing.T) {
	t.Parallel()

	t.Run("when the previous version is a pgroll migration", func(t *testing.T) {
		testutils.WithMigratorAndConnectionToContainer(t, func(mig *roll.Roll, db *sql.DB) {
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
			if schemaExists(t, db, roll.VersionedSchemaName(schema, firstVersion)) {
				t.Errorf("Expected schema %q to not exist", firstVersion)
			}
		})
	})

	t.Run("when the previous version is an inferred DDL migration", func(t *testing.T) {
		testutils.WithMigratorAndConnectionToContainer(t, func(mig *roll.Roll, db *sql.DB) {
			ctx := context.Background()
			const (
				firstVersion  = "1_create_table"
				secondVersion = "2_create_table"
			)

			// Run the first pgroll migration
			if err := mig.Start(ctx, &migrations.Migration{Name: firstVersion, Operations: migrations.Operations{createTableOp("table1")}}); err != nil {
				t.Fatalf("Failed to start first migration: %v", err)
			}
			if err := mig.Complete(ctx); err != nil {
				t.Fatalf("Failed to complete first migration: %v", err)
			}

			// Run a manual DDL migration
			_, err := db.ExecContext(ctx, "CREATE TABLE foo (id integer)")
			if err != nil {
				t.Fatalf("Failed to create table: %v", err)
			}

			// Run the second pgroll migration
			if err := mig.Start(ctx, &migrations.Migration{Name: secondVersion, Operations: migrations.Operations{createTableOp("table2")}}); err != nil {
				t.Fatalf("Failed to start second migration: %v", err)
			}
			if err := mig.Complete(ctx); err != nil {
				t.Fatalf("Failed to complete second migration: %v", err)
			}

			//
			// Check that the schema for the first version has been dropped
			//
			if schemaExists(t, db, roll.VersionedSchemaName(schema, firstVersion)) {
				t.Errorf("Expected schema %q to not exist", firstVersion)
			}
		})
	})
}

func TestSchemaIsDroppedAfterMigrationRollback(t *testing.T) {
	t.Parallel()

	testutils.WithMigratorAndConnectionToContainer(t, func(mig *roll.Roll, db *sql.DB) {
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
		if schemaExists(t, db, roll.VersionedSchemaName(schema, version)) {
			t.Errorf("Expected schema %q to not exist", version)
		}
	})
}

func TestRollbackOnMigrationStartFailure(t *testing.T) {
	t.Parallel()

	t.Run("when the DDL phase fails", func(t *testing.T) {
		t.Parallel()

		testutils.WithMigratorAndConnectionToContainer(t, func(mig *roll.Roll, db *sql.DB) {
			ctx := context.Background()

			// start a migration that will fail during the DDL phase
			err := mig.Start(ctx, &migrations.Migration{
				Name: "01_create_table",
				Operations: migrations.Operations{
					&migrations.OpCreateTable{
						Name: "table1",
						Columns: []migrations.Column{
							{
								Name: "id",
								Type: "invalid",
							},
						},
					},
				},
			})
			assert.Error(t, err)

			// ensure that there is no active migration
			status, err := mig.Status(ctx, "public")
			assert.NoError(t, err)
			assert.Equal(t, state.NoneMigrationStatus, status.Status)
		})
	})

	t.Run("when the backfill phase fails", func(t *testing.T) {
		t.Parallel()

		testutils.WithMigratorAndConnectionToContainer(t, func(mig *roll.Roll, db *sql.DB) {
			ctx := context.Background()

			// run an initial migration to create the table
			err := mig.Start(ctx, &migrations.Migration{
				Name:       "01_create_table",
				Operations: migrations.Operations{createTableOp("table1")},
			})
			assert.NoError(t, err)

			// complete the migration
			err = mig.Complete(ctx)
			assert.NoError(t, err)

			// insert some data into the table
			_, err = db.ExecContext(ctx, "INSERT INTO table1 (id, name) VALUES (1, 'alice'), (2, 'bob')")
			assert.NoError(t, err)

			// Start a migration that will fail during the backfill phase
			// Change the type of the `name` column but provide invalid up and down SQL
			err = mig.Start(ctx, &migrations.Migration{
				Name: "02_add_column",
				Operations: migrations.Operations{
					&migrations.OpAlterColumn{
						Table:  "table1",
						Column: "name",
						Type:   ptr("text"),
						Up:     ptr("invalid"),
						Down:   ptr("invalid"),
					},
				},
			})
			assert.Error(t, err)

			// Ensure that there is no active migration
			status, err := mig.Status(ctx, "public")
			assert.NoError(t, err)
			assert.Equal(t, "01_create_table", status.Version)
			assert.Equal(t, state.CompleteMigrationStatus, status.Status)
		})
	})
}

func TestSchemaOptionIsRespected(t *testing.T) {
	t.Parallel()

	testutils.WithMigratorInSchemaAndConnectionToContainer(t, "schema1", func(mig *roll.Roll, db *sql.DB) {
		ctx := context.Background()
		const version1 = "1_create_table"
		const version2 = "2_create_another_table"

		if err := mig.Start(ctx, &migrations.Migration{
			Name:       version1,
			Operations: migrations.Operations{createTableOp("table1")},
		}); err != nil {
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

		// Apply another migration to the same schema
		if err := mig.Start(ctx, &migrations.Migration{
			Name:       version2,
			Operations: migrations.Operations{createTableOp("table2")},
		}); err != nil {
			t.Fatalf("Failed to start migration: %v", err)
		}
		if err := mig.Complete(ctx); err != nil {
			t.Fatalf("Failed to complete migration: %v", err)
		}

		// Ensure that the versioned schema for the first migration has been dropped
		if schemaExists(t, db, roll.VersionedSchemaName("schema1", version1)) {
			t.Errorf("Expected schema %q to not exist", version1)
		}
	})
}

func TestLockTimeoutIsEnforced(t *testing.T) {
	t.Parallel()

	testutils.WithMigratorInSchemaAndConnectionToContainerWithOptions(t, "public", []roll.Option{roll.WithLockTimeoutMs(100)}, func(mig *roll.Roll, db *sql.DB) {
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

func TestViewsAreCreatedWithSecurityInvokerTrue(t *testing.T) {
	t.Parallel()

	testutils.WithMigratorAndConnectionToContainer(t, func(mig *roll.Roll, db *sql.DB) {
		ctx := context.Background()
		version := "1_create_table"

		if mig.PGVersion() < roll.PGVersion15 {
			t.Skip("Skipping test for postgres < 15 as `security_invoker` views are not supported")
		}

		// Start and complete a migration to create a simple `users` table
		if err := mig.Start(ctx, &migrations.Migration{Name: version, Operations: migrations.Operations{createTableOp("users")}}); err != nil {
			t.Fatalf("Failed to start migration: %v", err)
		}
		if err := mig.Complete(ctx); err != nil {
			t.Fatalf("Failed to complete migration: %v", err)
		}

		// Insert two rows into the underlying table
		_, err := db.ExecContext(ctx, "INSERT INTO users (id, name) VALUES (1, 'alice'), (2, 'bob')")
		if err != nil {
			t.Fatalf("Failed to insert rows into table: %v", err)
		}

		// Enable row level security on the underlying table
		_, err = db.ExecContext(ctx, "ALTER TABLE users ENABLE ROW LEVEL SECURITY")
		if err != nil {
			t.Fatalf("Failed to enable row level security: %v", err)
		}

		// Add a security policy to the underlying table
		_, err = db.ExecContext(ctx, "CREATE POLICY user_policy ON users USING (name = current_user)")
		if err != nil {
			t.Fatalf("Failed to create security policy: %v", err)
		}

		// Create user 'alice'
		_, err = db.ExecContext(ctx, "CREATE USER alice")
		if err != nil {
			t.Fatalf("Failed to create user: %v", err)
		}

		// Grant access to the underlying table to user 'alice'
		_, err = db.ExecContext(ctx, "GRANT SELECT ON users TO alice")
		if err != nil {
			t.Fatalf("Failed to grant access to user: %v", err)
		}

		// Grant access to the versioned schema to user 'alice'
		_, err = db.ExecContext(ctx, "GRANT USAGE ON SCHEMA public_1_create_table TO alice")
		if err != nil {
			t.Fatalf("Failed to grant usage on schema to user: %v", err)
		}

		// Grant access to the versioned view to user 'alice'
		_, err = db.ExecContext(ctx, "GRANT SELECT ON public_1_create_table.users TO alice")
		if err != nil {
			t.Fatalf("Failed to grant select on view to user: %v", err)
		}

		// Ensure that the superuser can see all rows
		rows := MustSelect(t, db, "public", "1_create_table", "users")
		assert.Equal(t, []map[string]any{
			{"id": 1, "name": "alice"},
			{"id": 2, "name": "bob"},
		}, rows)

		// Switch roles to 'alice'
		_, err = db.ExecContext(ctx, "SET ROLE alice")
		if err != nil {
			t.Fatalf("Failed to switch roles: %v", err)
		}

		// Ensure that 'alice' can only see her own row
		rows = MustSelect(t, db, "public", "1_create_table", "users")
		assert.Equal(t, []map[string]any{
			{"id": 1, "name": "alice"},
		}, rows)
	})
}

func TestStatusMethodReturnsCorrectStatus(t *testing.T) {
	t.Parallel()

	testutils.WithMigratorAndConnectionToContainer(t, func(mig *roll.Roll, db *sql.DB) {
		ctx := context.Background()

		// Get the initial migration status before any migrations are run
		status, err := mig.Status(ctx, "public")
		assert.NoError(t, err)

		// Ensure that the status shows "No migrations"
		assert.Equal(t, &state.Status{
			Schema:  "public",
			Version: "",
			Status:  state.NoneMigrationStatus,
		}, status)

		// Start a migration
		err = mig.Start(ctx, &migrations.Migration{
			Name:       "01_create_table",
			Operations: []migrations.Operation{createTableOp("table1")},
		})
		assert.NoError(t, err)

		// Get the migration status
		status, err = mig.Status(ctx, "public")
		assert.NoError(t, err)

		// Ensure that the status shows "In progress"
		assert.Equal(t, &state.Status{
			Schema:  "public",
			Version: "01_create_table",
			Status:  state.InProgressMigrationStatus,
		}, status)

		// Rollback the migration
		err = mig.Rollback(ctx)
		assert.NoError(t, err)

		// Get the migration status
		status, err = mig.Status(ctx, "public")
		assert.NoError(t, err)

		// Ensure that the status shows "No migrations"
		assert.Equal(t, &state.Status{
			Schema:  "public",
			Version: "",
			Status:  state.NoneMigrationStatus,
		}, status)

		// Start and complete a migration
		err = mig.Start(ctx, &migrations.Migration{
			Name:       "01_create_table",
			Operations: []migrations.Operation{createTableOp("table1")},
		})
		assert.NoError(t, err)
		err = mig.Complete(ctx)
		assert.NoError(t, err)

		// Get the migration status
		status, err = mig.Status(ctx, "public")
		assert.NoError(t, err)

		// Ensure that the status shows "Complete"
		assert.Equal(t, &state.Status{
			Schema:  "public",
			Version: "01_create_table",
			Status:  state.CompleteMigrationStatus,
		}, status)
	})
}

func TestRoleIsRespected(t *testing.T) {
	t.Parallel()

	testutils.WithMigratorInSchemaAndConnectionToContainerWithOptions(t, "public", []roll.Option{roll.WithRole("pgroll")}, func(mig *roll.Roll, db *sql.DB) {
		ctx := context.Background()

		// Start a create table migration
		err := mig.Start(ctx, &migrations.Migration{
			Name:       "01_create_table",
			Operations: migrations.Operations{createTableOp("table1")},
		})
		assert.NoError(t, err)

		// Complete the create table migration
		err = mig.Complete(ctx)
		assert.NoError(t, err)

		// Ensure that the table exists in the correct schema and owned by the correct role
		var exists bool
		err = db.QueryRow(`
			SELECT EXISTS(
				SELECT 1
				FROM pg_catalog.pg_tables
				WHERE tablename = $1
					AND schemaname = $2
					AND tableowner = $3
		)`, "table1", "public", "pgroll").Scan(&exists)
		assert.NoError(t, err)
		assert.True(t, exists)
	})
}

func TestMigrationHooksAreInvoked(t *testing.T) {
	t.Parallel()

	options := []roll.Option{roll.WithMigrationHooks(roll.MigrationHooks{
		BeforeStartDDL: func(m *roll.Roll) error {
			_, err := m.PgConn().ExecContext(context.Background(), "CREATE TABLE before_start_ddl (id integer)")
			return err
		},
		AfterStartDDL: func(m *roll.Roll) error {
			_, err := m.PgConn().ExecContext(context.Background(), "CREATE TABLE after_start_ddl (id integer)")
			return err
		},
	})}

	testutils.WithMigratorInSchemaAndConnectionToContainerWithOptions(t, "public", options, func(mig *roll.Roll, db *sql.DB) {
		ctx := context.Background()

		// Start a create table migration
		err := mig.Start(ctx, &migrations.Migration{
			Name:       "01_create_table",
			Operations: migrations.Operations{createTableOp("table1")},
		})
		assert.NoError(t, err)

		// Ensure that both the before_start_ddl and after_start_ddl tables were created
		assert.True(t, tableExists(t, db, "public", "before_start_ddl"))
		assert.True(t, tableExists(t, db, "public", "after_start_ddl"))

		// Complete the migration
		err = mig.Complete(ctx)
		assert.NoError(t, err)
	})
}

func TestRollSchemaMethodReturnsCorrectSchema(t *testing.T) {
	t.Parallel()

	t.Run("when the schema is public", func(t *testing.T) {
		testutils.WithMigratorInSchemaAndConnectionToContainer(t, "public", func(mig *roll.Roll, _ *sql.DB) {
			assert.Equal(t, "public", mig.Schema())
		})
	})

	t.Run("when the schema is non-public", func(t *testing.T) {
		testutils.WithMigratorInSchemaAndConnectionToContainer(t, "apples", func(mig *roll.Roll, _ *sql.DB) {
			assert.Equal(t, "apples", mig.Schema())
		})
	})
}

func createTableOp(tableName string) *migrations.OpCreateTable {
	return &migrations.OpCreateTable{
		Name: tableName,
		Columns: []migrations.Column{
			{
				Name: "id",
				Type: "integer",
				Pk:   ptr(true),
			},
			{
				Name:   "name",
				Type:   "varchar(255)",
				Unique: ptr(true),
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
			Nullable: ptr(true),
		},
	}
}

func MustSelect(t *testing.T, db *sql.DB, schema, version, table string) []map[string]any {
	t.Helper()
	versionSchema := roll.VersionedSchemaName(schema, version)

	//nolint:gosec // this is a test so we don't care about SQL injection
	selectStmt := fmt.Sprintf("SELECT * FROM %s.%s", versionSchema, table)

	q, err := db.Query(selectStmt)
	if err != nil {
		t.Fatal(err)
	}

	res := make([]map[string]any, 0)

	for q.Next() {
		cols, err := q.Columns()
		if err != nil {
			t.Fatal(err)
		}
		values := make([]any, len(cols))
		valuesPtr := make([]any, len(cols))
		for i := range values {
			valuesPtr[i] = &values[i]
		}
		if err := q.Scan(valuesPtr...); err != nil {
			t.Fatal(err)
		}

		row := map[string]any{}
		for i, col := range cols {
			// avoid having to cast int literals to int64 in tests
			if v, ok := values[i].(int64); ok {
				values[i] = int(v)
			}
			row[col] = values[i]
		}

		res = append(res, row)
	}

	return res
}

func schemaExists(t *testing.T, db *sql.DB, schema string) bool {
	t.Helper()
	var exists bool
	err := db.QueryRow(`
	SELECT EXISTS(
		SELECT 1
		FROM pg_catalog.pg_namespace
		WHERE nspname = $1
	)`, schema).Scan(&exists)
	if err != nil {
		t.Fatal(err)
	}
	return exists
}

func tableExists(t *testing.T, db *sql.DB, schema, table string) bool {
	t.Helper()

	var exists bool
	err := db.QueryRow(`
		SELECT EXISTS(
      SELECT 1
			FROM pg_catalog.pg_tables
			WHERE schemaname = $1
      AND tablename = $2
    )`,
		schema, table).Scan(&exists)
	if err != nil {
		t.Fatal(err)
	}

	return exists
}

func ptr[T any](v T) *T {
	return &v
}
