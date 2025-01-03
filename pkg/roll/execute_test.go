// SPDX-License-Identifier: Apache-2.0

package roll_test

import (
	"context"
	"database/sql"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/xataio/pgroll/internal/testutils"
	"github.com/xataio/pgroll/pkg/migrations"
	"github.com/xataio/pgroll/pkg/roll"
	"github.com/xataio/pgroll/pkg/state"
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

func TestNoVersionSchemaForRawSQLMigrationsOptionIsRespected(t *testing.T) {
	t.Parallel()

	opts := []roll.Option{roll.WithNoVersionSchemaForRawSQL()}

	testutils.WithMigratorAndStateAndConnectionToContainerWithOptions(t, opts, func(mig *roll.Roll, st *state.State, db *sql.DB) {
		ctx := context.Background()

		// Apply a create table migration
		err := mig.Start(ctx, &migrations.Migration{Name: "01_create_table", Operations: migrations.Operations{createTableOp("table1")}})
		require.NoError(t, err)
		err = mig.Complete(ctx)
		require.NoError(t, err)

		// Apply a raw SQL migration - no version schema should be created for this version
		err = mig.Start(ctx, &migrations.Migration{
			Name: "02_create_table",
			Operations: migrations.Operations{&migrations.OpRawSQL{
				Up: "CREATE TABLE table2(a int)",
			}},
		})
		require.NoError(t, err)
		err = mig.Complete(ctx)
		require.NoError(t, err)

		// Start a third create table migration
		err = mig.Start(ctx, &migrations.Migration{Name: "03_create_table", Operations: migrations.Operations{createTableOp("table3")}})
		require.NoError(t, err)

		// The previous version is migration 01 because there is no version schema
		// for migration 02 due to the `WithNoVersionSchemaForRawSQL` option
		prevVersion, err := st.PreviousVersion(ctx, "public")
		require.NoError(t, err)
		require.NotNil(t, prevVersion)
		assert.Equal(t, "01_create_table", *prevVersion)

		// Complete the third migration
		err = mig.Complete(ctx)
		require.NoError(t, err)

		// The latest version version is migration 03
		latestVersion, err := st.LatestVersion(ctx, "public")
		require.NoError(t, err)
		require.NotNil(t, latestVersion)
		assert.Equal(t, "03_create_table", *latestVersion)
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
						Up:     "invalid",
						Down:   "invalid",
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

func TestMigrationDDLIsRetriedOnLockTimeouts(t *testing.T) {
	t.Parallel()

	testutils.WithMigratorInSchemaAndConnectionToContainerWithOptions(t, "public", []roll.Option{roll.WithLockTimeoutMs(50)}, func(mig *roll.Roll, db *sql.DB) {
		ctx := context.Background()

		// Create a table
		_, err := db.ExecContext(ctx, "CREATE TABLE table1 (id integer, name text)")
		require.NoError(t, err)

		// Start a goroutine which takes an ACCESS_EXCLUSIVE lock on the table for
		// two seconds
		errCh := make(chan error)
		go func() {
			tx, err := db.Begin()
			if err != nil {
				errCh <- err
			}

			if _, err := tx.ExecContext(ctx, "LOCK TABLE table1 IN ACCESS EXCLUSIVE MODE"); err != nil {
				errCh <- err
			}
			errCh <- nil

			// Sleep for two seconds to hold the lock
			time.Sleep(2 * time.Second)

			// Commit the transaction
			tx.Commit()
		}()

		// Wait for lock to be taken
		err = <-errCh
		require.NoError(t, err)

		// Attempt to start a second migration on the table while the lock is held.
		// The migration should eventually succeed after the lock is released
		err = mig.Start(ctx, &migrations.Migration{
			Name:       "01_add_column",
			Operations: migrations.Operations{addColumnOp("table1")},
		})
		require.NoError(t, err)
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
		BeforeCompleteDDL: func(m *roll.Roll) error {
			_, err := m.PgConn().ExecContext(context.Background(), "CREATE TABLE before_complete_ddl (id integer)")
			return err
		},
		AfterCompleteDDL: func(m *roll.Roll) error {
			_, err := m.PgConn().ExecContext(context.Background(), "CREATE TABLE after_complete_ddl (id integer)")
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

		// Ensure that both the before_complete_ddl and after_complete_ddl tables were created
		assert.True(t, tableExists(t, db, "public", "before_complete_ddl"))
		assert.True(t, tableExists(t, db, "public", "after_complete_ddl"))
	})
}

func TestCallbacksAreInvokedOnMigrationStart(t *testing.T) {
	t.Parallel()

	testutils.WithMigratorAndConnectionToContainer(t, func(mig *roll.Roll, db *sql.DB) {
		ctx := context.Background()

		// Create a table
		_, err := db.ExecContext(ctx, "CREATE TABLE users (id SERIAL PRIMARY KEY, name text)")
		require.NoError(t, err)

		// Insert some data
		_, err = db.ExecContext(ctx,
			"INSERT INTO users (id, name) VALUES (1, 'alice'), (2, 'bob')")
		require.NoError(t, err)

		// Define a mock callback
		invoked := false
		cb := func(n, total int64) { invoked = true }

		// Start a migration that requires a backfill
		err = mig.Start(ctx, &migrations.Migration{
			Name: "02_change_type",
			Operations: migrations.Operations{
				&migrations.OpAlterColumn{
					Table:  "users",
					Column: "name",
					Type:   ptr("varchar(255)"),
					Up:     "name",
					Down:   "name",
				},
			},
		}, cb)
		require.NoError(t, err)

		// Ensure that the callback was invoked
		assert.True(t, invoked)
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

func TestSQLTransformerOptionIsUsedWhenCreatingTriggers(t *testing.T) {
	t.Parallel()

	t.Run("when the SQL transformer is used to rewrite SQL", func(t *testing.T) {
		t.Parallel()

		sqlTransformer := testutils.NewMockSQLTransformer(map[string]string{
			"'apples'": "'rewritten'",
		})
		opts := []roll.Option{roll.WithSQLTransformer(sqlTransformer)}

		testutils.WithMigratorAndConnectionToContainerWithOptions(t, opts, func(mig *roll.Roll, db *sql.DB) {
			ctx := context.Background()

			// Start a create table migration
			err := mig.Start(ctx, &migrations.Migration{
				Name:       "01_create_table",
				Operations: migrations.Operations{createTableOp("table1")},
			})
			require.NoError(t, err)

			// Complete the migration
			err = mig.Complete(ctx)
			require.NoError(t, err)

			// Insert some data
			_, err = db.ExecContext(ctx, "INSERT INTO table1 (id, name) VALUES (1, 'alice'), (2, 'bob')")
			require.NoError(t, err)

			// Start an add column migration that requires a backfill
			err = mig.Start(ctx, &migrations.Migration{
				Name: "02_add_column",
				Operations: migrations.Operations{
					&migrations.OpAddColumn{
						Table: "table1",
						Up:    "'apples'",
						Column: migrations.Column{
							Name:     "description",
							Type:     "text",
							Nullable: false,
						},
					},
				},
			})
			require.NoError(t, err)

			// Complete the migration
			err = mig.Complete(ctx)
			require.NoError(t, err)

			// Ensure that the backfill used the SQL rewritten by the transformer
			rows := MustSelect(t, db, "public", "02_add_column", "table1")
			assert.Equal(t, []map[string]any{
				{"id": 1, "name": "alice", "description": "rewritten"},
				{"id": 2, "name": "bob", "description": "rewritten"},
			}, rows)
		})
	})

	t.Run("when the SQL transformer returns an error", func(t *testing.T) {
		t.Parallel()

		sqlTransformer := testutils.NewMockSQLTransformer(map[string]string{
			"'apples'": testutils.MockSQLTransformerError,
		})
		opts := []roll.Option{roll.WithSQLTransformer(sqlTransformer)}

		testutils.WithMigratorAndConnectionToContainerWithOptions(t, opts, func(mig *roll.Roll, db *sql.DB) {
			ctx := context.Background()

			// Start a create table migration
			err := mig.Start(ctx, &migrations.Migration{
				Name:       "01_create_table",
				Operations: migrations.Operations{createTableOp("table1")},
			})
			require.NoError(t, err)

			// Complete the migration
			err = mig.Complete(ctx)
			require.NoError(t, err)

			// Insert some data
			_, err = db.ExecContext(ctx, "INSERT INTO table1 (id, name) VALUES (1, 'alice'), (2, 'bob')")
			require.NoError(t, err)

			// Start an add column migration that requires a backfill
			err = mig.Start(ctx, &migrations.Migration{
				Name: "02_add_column",
				Operations: migrations.Operations{
					&migrations.OpAddColumn{
						Table: "table1",
						Up:    "'apples'",
						Column: migrations.Column{
							Name:     "description",
							Type:     "text",
							Nullable: false,
						},
					},
				},
			})
			// Ensure that the start phase has failed with a SQL transformer error
			require.ErrorIs(t, err, testutils.ErrMockSQLTransformer)
		})
	})
}

func TestWithSearchPathOptionIsRespected(t *testing.T) {
	t.Parallel()

	opts := []roll.Option{roll.WithSearchPath("public")}

	testutils.WithMigratorInSchemaAndConnectionToContainerWithOptions(t, "foo", opts, func(mig *roll.Roll, db *sql.DB) {
		ctx := context.Background()

		// Create a function in the public schema
		_, err := db.ExecContext(ctx, `CREATE OR REPLACE FUNCTION say_hello()
      RETURNS TEXT AS $$
        SELECT 'hello world';
      $$ LANGUAGE sql;
    `)
		require.NoError(t, err)

		// Apply a migration in the foo schema that references the function in the public schema
		err = mig.Start(ctx, &migrations.Migration{
			Name: "01_raw_sql",
			Operations: migrations.Operations{
				&migrations.OpRawSQL{
					Up: "SELECT say_hello()",
				},
			},
		})
		require.NoError(t, err)

		// Complete the migration
		err = mig.Complete(ctx)
		require.NoError(t, err)

		// No assertions required as the migration would have failed if the
		// function reference was not found
	})
}

func createTableOp(tableName string) *migrations.OpCreateTable {
	return &migrations.OpCreateTable{
		Name: tableName,
		Columns: []migrations.Column{
			{
				Name: "id",
				Type: "integer",
				Pk:   true,
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
	assert.NoError(t, q.Err())

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
