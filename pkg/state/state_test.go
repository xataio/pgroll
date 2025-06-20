// SPDX-License-Identifier: Apache-2.0

package state_test

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/xataio/pgroll/internal/testutils"
	"github.com/xataio/pgroll/pkg/backfill"
	"github.com/xataio/pgroll/pkg/migrations"
	"github.com/xataio/pgroll/pkg/roll"
	"github.com/xataio/pgroll/pkg/schema"
	"github.com/xataio/pgroll/pkg/state"
)

func TestMain(m *testing.M) {
	testutils.SharedTestMain(m)
}

func TestSchemaOptionIsRespected(t *testing.T) {
	t.Parallel()

	testutils.WithStateAndConnectionToContainer(t, func(state *state.State, db *sql.DB) {
		ctx := context.Background()

		// create a table in the public schema
		if _, err := db.ExecContext(ctx, "CREATE TABLE public.table1 (id int)"); err != nil {
			t.Fatal(err)
		}

		// check that we can retrieve the already existing table
		currentSchema, err := state.ReadSchema(ctx, "public")
		assert.NoError(t, err)

		assert.Equal(t, 1, len(currentSchema.Tables))
		assert.Equal(t, "public", currentSchema.Name)

		// check that we can start the migration
		err = state.Start(ctx, "public", &migrations.Migration{
			Name: "1_add_column",
			Operations: migrations.Operations{
				&migrations.OpAddColumn{
					Table: "table1",
					Column: migrations.Column{
						Name: "test",
						Type: "text",
					},
				},
			},
		})
		assert.NoError(t, err)
	})
}

func TestInferredMigration(t *testing.T) {
	t.Parallel()

	testutils.WithStateAndConnectionToContainer(t, func(state *state.State, db *sql.DB) {
		ctx := context.Background()

		tests := []struct {
			name           string
			sqlStmts       []string
			wantMigrations []migrations.Migration
		}{
			{
				name:     "create table",
				sqlStmts: []string{"CREATE TABLE public.table1 (id int)"},
				wantMigrations: []migrations.Migration{
					{
						Operations: migrations.Operations{
							&migrations.OpRawSQL{Up: "CREATE TABLE public.table1 (id int)"},
						},
					},
				},
			},
			{
				name: "create/drop table",
				sqlStmts: []string{
					"CREATE TABLE table1 (id int)",
					"DROP TABLE table1",
				},
				wantMigrations: []migrations.Migration{
					{
						Operations: migrations.Operations{
							&migrations.OpRawSQL{Up: "CREATE TABLE table1 (id int)"},
						},
					},
					{
						Operations: migrations.Operations{
							&migrations.OpRawSQL{Up: "DROP TABLE table1"},
						},
					},
				},
			},
			{
				name: "create/drop column",
				sqlStmts: []string{
					"CREATE TABLE table1 (id int, b text)",
					"ALTER TABLE table1 DROP COLUMN b",
				},
				wantMigrations: []migrations.Migration{
					{
						Operations: migrations.Operations{
							&migrations.OpRawSQL{Up: "CREATE TABLE table1 (id int, b text)"},
						},
					},
					{
						Operations: migrations.Operations{
							&migrations.OpRawSQL{Up: "ALTER TABLE table1 DROP COLUMN b"},
						},
					},
				},
			},
			{
				name: "create/drop check constraint",
				sqlStmts: []string{
					"CREATE TABLE table1 (id int, age integer, CONSTRAINT check_age CHECK (age > 0))",
					"ALTER TABLE table1 DROP CONSTRAINT check_age",
				},
				wantMigrations: []migrations.Migration{
					{
						Operations: migrations.Operations{
							&migrations.OpRawSQL{Up: "CREATE TABLE table1 (id int, age integer, CONSTRAINT check_age CHECK (age > 0))"},
						},
					},
					{
						Operations: migrations.Operations{
							&migrations.OpRawSQL{Up: "ALTER TABLE table1 DROP CONSTRAINT check_age"},
						},
					},
				},
			},
			{
				name: "create/drop unique constraint",
				sqlStmts: []string{
					"CREATE TABLE table1 (id int, b text, CONSTRAINT unique_b UNIQUE(b))",
					"ALTER TABLE table1 DROP CONSTRAINT unique_b",
				},
				wantMigrations: []migrations.Migration{
					{
						Operations: migrations.Operations{
							&migrations.OpRawSQL{Up: "CREATE TABLE table1 (id int, b text, CONSTRAINT unique_b UNIQUE(b))"},
						},
					},
					{
						Operations: migrations.Operations{
							&migrations.OpRawSQL{Up: "ALTER TABLE table1 DROP CONSTRAINT unique_b"},
						},
					},
				},
			},
			{
				name: "create/drop index",
				sqlStmts: []string{
					"CREATE TABLE table1 (id int, b text)",
					"CREATE INDEX idx_b ON table1(b)",
					"DROP INDEX idx_b",
				},
				wantMigrations: []migrations.Migration{
					{
						Operations: migrations.Operations{
							&migrations.OpRawSQL{Up: "CREATE TABLE table1 (id int, b text)"},
						},
					},
					{
						Operations: migrations.Operations{
							&migrations.OpRawSQL{Up: "CREATE INDEX idx_b ON table1(b)"},
						},
					},
					{
						Operations: migrations.Operations{
							&migrations.OpRawSQL{Up: "DROP INDEX idx_b"},
						},
					},
				},
			},
			{
				name: "create/drop function",
				sqlStmts: []string{
					"CREATE FUNCTION foo() RETURNS void AS $$ BEGIN END; $$ LANGUAGE plpgsql",
					"DROP FUNCTION foo",
				},
				wantMigrations: []migrations.Migration{
					{
						Operations: migrations.Operations{
							&migrations.OpRawSQL{Up: "CREATE FUNCTION foo() RETURNS void AS $$ BEGIN END; $$ LANGUAGE plpgsql"},
						},
					},
					{
						Operations: migrations.Operations{
							&migrations.OpRawSQL{Up: "DROP FUNCTION foo"},
						},
					},
				},
			},
			{
				name: "create/drop schema",
				sqlStmts: []string{
					"CREATE SCHEMA foo",
					"DROP SCHEMA foo",
				},
				wantMigrations: []migrations.Migration{
					{
						Operations: migrations.Operations{
							&migrations.OpRawSQL{Up: "CREATE SCHEMA foo"},
						},
					},
					{
						Operations: migrations.Operations{
							&migrations.OpRawSQL{Up: "DROP SCHEMA foo"},
						},
					},
				},
			},
		}

		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				if _, err := db.ExecContext(ctx, "DROP SCHEMA public CASCADE; CREATE SCHEMA public"); err != nil {
					t.Fatal(err)
				}

				if _, err := db.ExecContext(ctx, fmt.Sprintf("TRUNCATE %s.migrations", state.Schema())); err != nil {
					t.Fatal(err)
				}

				for _, stmt := range tt.sqlStmts {
					if _, err := db.ExecContext(ctx, stmt); err != nil {
						t.Fatal(err)
					}
				}

				rows, err := db.QueryContext(ctx,
					fmt.Sprintf("SELECT migration FROM %s.migrations ORDER BY created_at ASC", state.Schema()))
				if err != nil {
					t.Fatal(err)
				}

				var gotMigrations []migrations.Migration
				for rows.Next() {
					var migrationStr []byte
					if err := rows.Scan(&migrationStr); err != nil {
						t.Fatal(err)
					}
					var gotMigration migrations.Migration
					if err := json.Unmarshal(migrationStr, &gotMigration); err != nil {
						t.Fatal(err)
					}
					gotMigrations = append(gotMigrations, gotMigration)
				}
				assert.NoError(t, rows.Err())

				assert.Equal(t, len(tt.wantMigrations), len(gotMigrations), "unexpected number of migrations")

				for i, wantMigration := range tt.wantMigrations {
					gotMigration := gotMigrations[i]

					// Ensure that the operations are equal; we don't care about the
					// randomly generated `VersionSchema` field for the migration.
					assert.Equal(t, wantMigration.Operations, gotMigration.Operations)
				}
			})
		}
	})
}

func TestInferredMigrationsHaveExpectedNamesAndVersionSchema(t *testing.T) {
	t.Parallel()

	t.Run("an inferred migration first in the history", func(t *testing.T) {
		testutils.WithStateAndConnectionToContainer(t, func(st *state.State, db *sql.DB) {
			ctx := context.Background()

			// Execute a SQL DDL statement
			_, err := db.ExecContext(ctx, "CREATE TABLE table1(id int)")
			require.NoError(t, err)

			// Get the migration history
			history, err := st.SchemaHistory(ctx, "public")
			require.NoError(t, err)

			// Assert that the history contains one migration with the expected name
			require.Len(t, history, 1)
			require.Regexp(t, `^00000_initial_\d{20}$`, history[0].Migration.Name)
			// Assert that the VersionSchema field matches the expected format
			require.Regexp(t, "^sql_[0-9a-f]{8}", history[0].Migration.VersionSchema)
		})
	})

	t.Run("an inferred migration following a pgroll migration", func(t *testing.T) {
		testutils.WithMigratorAndConnectionToContainer(t, func(m *roll.Roll, db *sql.DB) {
			ctx := context.Background()

			// Execute a pgroll migration
			err := m.Start(ctx, &migrations.Migration{
				Name:       "01_initial_migration",
				Operations: migrations.Operations{&migrations.OpRawSQL{Up: "SELECT 1"}},
			}, backfill.NewConfig())
			require.NoError(t, err)

			// Complete the migration
			err = m.Complete(ctx)
			require.NoError(t, err)

			// Execute a SQL DDL statement
			_, err = db.ExecContext(ctx, "CREATE TABLE table1(id int)")
			require.NoError(t, err)

			// Get the migration history
			history, err := m.State().SchemaHistory(ctx, "public")
			require.NoError(t, err)

			// Assert that the history contains two migrations with the expected
			// names. The inferred migration should have a name that starts with
			// the name of the preceding pgroll migration followed by a timestamp.
			require.Len(t, history, 2)
			require.Equal(t, "01_initial_migration", history[0].Migration.Name)
			require.Regexp(t, `^01_initial_migration_\d{20}$`, history[1].Migration.Name)
			// Assert that the VersionSchema field matches the expected format
			require.Regexp(t, "^sql_[0-9a-f]{8}", history[1].Migration.VersionSchema)
		})
	})
}

func TestInferredMigrationsInTransactionHaveDifferentTimestamps(t *testing.T) {
	ctx := context.Background()

	testutils.WithStateAndConnectionToContainer(t, func(state *state.State, db *sql.DB) {
		// Start a transaction
		tx, err := db.BeginTx(ctx, nil)
		require.NoError(t, err)
		defer tx.Rollback()

		// Create two tables in the transaction
		_, err = tx.ExecContext(ctx, "CREATE TABLE table1 (id int)")
		require.NoError(t, err)

		_, err = tx.ExecContext(ctx, "CREATE TABLE table2 (id int)")
		require.NoError(t, err)

		// Commit the transaction
		tx.Commit()

		// Read the migrations from the migrations table
		rows, err := db.QueryContext(ctx,
			fmt.Sprintf("SELECT name, created_at, updated_at FROM %s.migrations ORDER BY created_at ASC", state.Schema()))
		if err != nil {
			t.Fatal(err)
		}

		type m struct {
			name      string
			createdAt time.Time
			updatedAt time.Time
		}
		var migrations []m

		for rows.Next() {
			var migration m
			if err := rows.Scan(&migration.name, &migration.createdAt, &migration.updatedAt); err != nil {
				t.Fatal(err)
			}

			migrations = append(migrations, migration)
		}
		assert.NoError(t, rows.Err())

		// Ensure that the two inferred migrations have different timestamps
		assert.Equal(t, 2, len(migrations), "unexpected number of migrations")
		assert.NotEqual(t, migrations[0].createdAt, migrations[1].createdAt, "migrations have the same timestamp")
	})
}

func TestPgRollInitializationInANonDefaultSchema(t *testing.T) {
	t.Parallel()

	testutils.WithStateInSchemaAndConnectionToContainer(t, "pgroll_foo", func(state *state.State, _ *sql.DB) {
		ctx := context.Background()

		// Ensure that pgroll state has been correctly initialized in the
		// non-default schema `pgroll_foo` by performing a basic operation on the
		// state
		migrationActive, err := state.IsActiveMigrationPeriod(ctx, "public")
		if err != nil {
			t.Fatal(err)
		}

		assert.False(t, migrationActive)
	})
}

func TestIsInitializedMethodReturnsTrueAfterInitialization(t *testing.T) {
	t.Parallel()

	testutils.WithUninitializedState(t, func(state *state.State) {
		ctx := context.Background()

		// Get whether the state is initialized
		ok, err := state.IsInitialized(ctx)
		require.NoError(t, err)

		// Assert that the state is not initialized as `Init` has not been called
		require.False(t, ok)

		// Invoke `Init` to initialize the state
		err = state.Init(ctx)
		require.NoError(t, err)

		// Get whether the state is initialized
		ok, err = state.IsInitialized(ctx)
		require.NoError(t, err)

		// Assert that the state is initialized
		require.True(t, ok)
	})
}

func TestConcurrentInitialization(t *testing.T) {
	t.Parallel()

	testutils.WithUninitializedState(t, func(state *state.State) {
		ctx := context.Background()
		numGoroutines := 10

		wg := sync.WaitGroup{}
		wg.Add(numGoroutines)
		for i := 0; i < numGoroutines; i++ {
			go func() {
				defer wg.Done()

				if err := state.Init(ctx); err != nil {
					t.Error(err)
				}
			}()
		}

		wg.Wait()
	})
}

func TestReadSchema(t *testing.T) {
	t.Parallel()

	testutils.WithStateAndConnectionToContainer(t, func(state *state.State, db *sql.DB) {
		ctx := context.Background()

		tests := []struct {
			name       string
			createStmt string
			wantSchema *schema.Schema
		}{
			{
				name:       "empty schema",
				createStmt: "",
				wantSchema: &schema.Schema{
					Name:   "public",
					Tables: map[string]*schema.Table{},
				},
			},
			{
				name:       "one table without columns",
				createStmt: "CREATE TABLE public.table1 ()",
				wantSchema: &schema.Schema{
					Name: "public",
					Tables: map[string]*schema.Table{
						"table1": {
							Name: "table1",
						},
					},
				},
			},
			{
				name:       "one table with columns",
				createStmt: "CREATE TABLE public.table1 (id int)",
				wantSchema: &schema.Schema{
					Name: "public",
					Tables: map[string]*schema.Table{
						"table1": {
							Name: "table1",
							Columns: map[string]*schema.Column{
								"id": {
									Name:         "id",
									Type:         "integer",
									Nullable:     true,
									PostgresType: "base",
								},
							},
						},
					},
				},
			},
			{
				name:       "unique, not null",
				createStmt: "CREATE TABLE public.table1 (id int NOT NULL, CONSTRAINT id_unique UNIQUE(id))",
				wantSchema: &schema.Schema{
					Name: "public",
					Tables: map[string]*schema.Table{
						"table1": {
							Name: "table1",
							Columns: map[string]*schema.Column{
								"id": {
									Name:         "id",
									Type:         "integer",
									Nullable:     false,
									Unique:       true,
									PostgresType: "base",
								},
							},
							Indexes: map[string]*schema.Index{
								"id_unique": {
									Name:       "id_unique",
									Unique:     true,
									Columns:    []string{"id"},
									Method:     string(migrations.OpCreateIndexMethodBtree),
									Definition: "CREATE UNIQUE INDEX id_unique ON public.table1 USING btree (id)",
								},
							},
							UniqueConstraints: map[string]*schema.UniqueConstraint{
								"id_unique": {
									Name:    "id_unique",
									Columns: []string{"id"},
								},
							},
						},
					},
				},
			},
			{
				name:       "non-unique index",
				createStmt: "CREATE TABLE public.table1 (id int, name text); CREATE INDEX idx_name ON public.table1 (name)",
				wantSchema: &schema.Schema{
					Name: "public",
					Tables: map[string]*schema.Table{
						"table1": {
							Name: "table1",
							Columns: map[string]*schema.Column{
								"id": {
									Name:         "id",
									Type:         "integer",
									Nullable:     true,
									PostgresType: "base",
								},
								"name": {
									Name:         "name",
									Type:         "text",
									Nullable:     true,
									PostgresType: "base",
								},
							},
							Indexes: map[string]*schema.Index{
								"idx_name": {
									Name:       "idx_name",
									Unique:     false,
									Columns:    []string{"name"},
									Method:     string(migrations.OpCreateIndexMethodBtree),
									Definition: "CREATE INDEX idx_name ON public.table1 USING btree (name)",
								},
							},
						},
					},
				},
			},
			{
				name:       "foreign key",
				createStmt: "CREATE TABLE public.table1 (id int PRIMARY KEY); CREATE TABLE public.table2 (fk int NOT NULL, CONSTRAINT fk_fkey FOREIGN KEY (fk) REFERENCES public.table1 (id))",
				wantSchema: &schema.Schema{
					Name: "public",
					Tables: map[string]*schema.Table{
						"table1": {
							Name: "table1",
							Columns: map[string]*schema.Column{
								"id": {
									Name:         "id",
									Type:         "integer",
									Nullable:     false,
									Unique:       true,
									PostgresType: "base",
								},
							},
							PrimaryKey: []string{"id"},
							Indexes: map[string]*schema.Index{
								"table1_pkey": {
									Name:       "table1_pkey",
									Unique:     true,
									Columns:    []string{"id"},
									Method:     string(migrations.OpCreateIndexMethodBtree),
									Definition: "CREATE UNIQUE INDEX table1_pkey ON public.table1 USING btree (id)",
								},
							},
						},
						"table2": {
							Name: "table2",
							Columns: map[string]*schema.Column{
								"fk": {
									Name:         "fk",
									Type:         "integer",
									Nullable:     false,
									PostgresType: "base",
								},
							},
							ForeignKeys: map[string]*schema.ForeignKey{
								"fk_fkey": {
									Name:              "fk_fkey",
									Columns:           []string{"fk"},
									ReferencedTable:   "table1",
									ReferencedColumns: []string{"id"},
									MatchType:         "SIMPLE",
									OnDelete:          "NO ACTION",
									OnUpdate:          "NO ACTION",
								},
							},
						},
					},
				},
			},
			{
				name:       "foreign key with ON DELETE CASCADE",
				createStmt: "CREATE TABLE public.table1 (id int PRIMARY KEY); CREATE TABLE public.table2 (fk int NOT NULL, CONSTRAINT fk_fkey FOREIGN KEY (fk) REFERENCES public.table1 (id) ON DELETE CASCADE)",
				wantSchema: &schema.Schema{
					Name: "public",
					Tables: map[string]*schema.Table{
						"table1": {
							Name: "table1",
							Columns: map[string]*schema.Column{
								"id": {
									Name:         "id",
									Type:         "integer",
									Nullable:     false,
									Unique:       true,
									PostgresType: "base",
								},
							},
							PrimaryKey: []string{"id"},
							Indexes: map[string]*schema.Index{
								"table1_pkey": {
									Name:       "table1_pkey",
									Unique:     true,
									Columns:    []string{"id"},
									Method:     string(migrations.OpCreateIndexMethodBtree),
									Definition: "CREATE UNIQUE INDEX table1_pkey ON public.table1 USING btree (id)",
								},
							},
						},
						"table2": {
							Name: "table2",
							Columns: map[string]*schema.Column{
								"fk": {
									Name:         "fk",
									Type:         "integer",
									Nullable:     false,
									PostgresType: "base",
								},
							},
							ForeignKeys: map[string]*schema.ForeignKey{
								"fk_fkey": {
									Name:              "fk_fkey",
									Columns:           []string{"fk"},
									ReferencedTable:   "table1",
									ReferencedColumns: []string{"id"},
									MatchType:         "SIMPLE",
									OnDelete:          "CASCADE",
									OnUpdate:          "NO ACTION",
								},
							},
						},
					},
				},
			},
			{
				name:       "foreign key with ON DELETE CASCADE ON UPDATE CASCADE",
				createStmt: "CREATE TABLE public.table1 (id int PRIMARY KEY); CREATE TABLE public.table2 (fk int NOT NULL, CONSTRAINT fk_fkey FOREIGN KEY (fk) REFERENCES public.table1 (id) ON DELETE CASCADE ON UPDATE CASCADE)",
				wantSchema: &schema.Schema{
					Name: "public",
					Tables: map[string]*schema.Table{
						"table1": {
							Name: "table1",
							Columns: map[string]*schema.Column{
								"id": {
									Name:         "id",
									Type:         "integer",
									Nullable:     false,
									Unique:       true,
									PostgresType: "base",
								},
							},
							PrimaryKey: []string{"id"},
							Indexes: map[string]*schema.Index{
								"table1_pkey": {
									Name:       "table1_pkey",
									Unique:     true,
									Columns:    []string{"id"},
									Method:     string(migrations.OpCreateIndexMethodBtree),
									Definition: "CREATE UNIQUE INDEX table1_pkey ON public.table1 USING btree (id)",
								},
							},
						},
						"table2": {
							Name: "table2",
							Columns: map[string]*schema.Column{
								"fk": {
									Name:         "fk",
									Type:         "integer",
									Nullable:     false,
									PostgresType: "base",
								},
							},
							ForeignKeys: map[string]*schema.ForeignKey{
								"fk_fkey": {
									Name:              "fk_fkey",
									Columns:           []string{"fk"},
									ReferencedTable:   "table1",
									ReferencedColumns: []string{"id"},
									MatchType:         "SIMPLE",
									OnDelete:          "CASCADE",
									OnUpdate:          "CASCADE",
								},
							},
						},
					},
				},
			},
			{
				name:       "foreign key with MATCH full ON DELETE CASCADE",
				createStmt: "CREATE TABLE public.table1 (id int PRIMARY KEY); CREATE TABLE public.table2 (fk int NOT NULL, CONSTRAINT fk_fkey FOREIGN KEY (fk) REFERENCES public.table1 (id) MATCH FULL ON DELETE CASCADE)",
				wantSchema: &schema.Schema{
					Name: "public",
					Tables: map[string]*schema.Table{
						"table1": {
							Name: "table1",
							Columns: map[string]*schema.Column{
								"id": {
									Name:         "id",
									Type:         "integer",
									Nullable:     false,
									Unique:       true,
									PostgresType: "base",
								},
							},
							PrimaryKey: []string{"id"},
							Indexes: map[string]*schema.Index{
								"table1_pkey": {
									Name:       "table1_pkey",
									Unique:     true,
									Columns:    []string{"id"},
									Method:     string(migrations.OpCreateIndexMethodBtree),
									Definition: "CREATE UNIQUE INDEX table1_pkey ON public.table1 USING btree (id)",
								},
							},
						},
						"table2": {
							Name: "table2",
							Columns: map[string]*schema.Column{
								"fk": {
									Name:         "fk",
									Type:         "integer",
									Nullable:     false,
									PostgresType: "base",
								},
							},
							ForeignKeys: map[string]*schema.ForeignKey{
								"fk_fkey": {
									Name:              "fk_fkey",
									Columns:           []string{"fk"},
									ReferencedTable:   "table1",
									ReferencedColumns: []string{"id"},
									MatchType:         "FULL",
									OnDelete:          "CASCADE",
									OnUpdate:          "NO ACTION",
								},
							},
						},
					},
				},
			},
			{
				name:       "check constraint",
				createStmt: "CREATE TABLE public.table1 (id int PRIMARY KEY, age INTEGER, CONSTRAINT age_check CHECK (age > 18));",
				wantSchema: &schema.Schema{
					Name: "public",
					Tables: map[string]*schema.Table{
						"table1": {
							Name: "table1",
							Columns: map[string]*schema.Column{
								"id": {
									Name:         "id",
									Type:         "integer",
									Nullable:     false,
									Unique:       true,
									PostgresType: "base",
								},
								"age": {
									Name:         "age",
									Type:         "integer",
									Nullable:     true,
									PostgresType: "base",
								},
							},
							PrimaryKey: []string{"id"},
							Indexes: map[string]*schema.Index{
								"table1_pkey": {
									Name:       "table1_pkey",
									Unique:     true,
									Columns:    []string{"id"},
									Method:     string(migrations.OpCreateIndexMethodBtree),
									Definition: "CREATE UNIQUE INDEX table1_pkey ON public.table1 USING btree (id)",
								},
							},
							CheckConstraints: map[string]*schema.CheckConstraint{
								"age_check": {
									Name:       "age_check",
									Columns:    []string{"age"},
									Definition: "CHECK ((age > 18))",
									NoInherit:  false,
								},
							},
						},
					},
				},
			},
			{
				name:       "check constraint with no inherit",
				createStmt: "CREATE TABLE public.table1 (age INTEGER, CONSTRAINT age_check CHECK (age > 18) NO INHERIT);",
				wantSchema: &schema.Schema{
					Name: "public",
					Tables: map[string]*schema.Table{
						"table1": {
							Name: "table1",
							Columns: map[string]*schema.Column{
								"age": {
									Name:         "age",
									Type:         "integer",
									Nullable:     true,
									PostgresType: "base",
								},
							},
							CheckConstraints: map[string]*schema.CheckConstraint{
								"age_check": {
									Name:       "age_check",
									Columns:    []string{"age"},
									Definition: "CHECK ((age > 18)) NO INHERIT",
									NoInherit:  true,
								},
							},
						},
					},
				},
			},
			{
				name:       "unique constraint",
				createStmt: "CREATE TABLE public.table1 (id int PRIMARY KEY, name TEXT, CONSTRAINT name_unique UNIQUE(name) );",
				wantSchema: &schema.Schema{
					Name: "public",
					Tables: map[string]*schema.Table{
						"table1": {
							Name: "table1",
							Columns: map[string]*schema.Column{
								"id": {
									Name:         "id",
									Type:         "integer",
									Nullable:     false,
									Unique:       true,
									PostgresType: "base",
								},
								"name": {
									Name:         "name",
									Type:         "text",
									Unique:       true,
									Nullable:     true,
									PostgresType: "base",
								},
							},
							PrimaryKey: []string{"id"},
							Indexes: map[string]*schema.Index{
								"table1_pkey": {
									Name:       "table1_pkey",
									Unique:     true,
									Columns:    []string{"id"},
									Method:     string(migrations.OpCreateIndexMethodBtree),
									Definition: "CREATE UNIQUE INDEX table1_pkey ON public.table1 USING btree (id)",
								},
								"name_unique": {
									Name:       "name_unique",
									Unique:     true,
									Columns:    []string{"name"},
									Method:     string(migrations.OpCreateIndexMethodBtree),
									Definition: "CREATE UNIQUE INDEX name_unique ON public.table1 USING btree (name)",
								},
							},
							UniqueConstraints: map[string]*schema.UniqueConstraint{
								"name_unique": {
									Name:    "name_unique",
									Columns: []string{"name"},
								},
							},
						},
					},
				},
			},
			{
				name:       "multicolumn unique constraint",
				createStmt: "CREATE TABLE public.table1 (id int PRIMARY KEY, name TEXT, CONSTRAINT name_id_unique UNIQUE(id, name));",
				wantSchema: &schema.Schema{
					Name: "public",
					Tables: map[string]*schema.Table{
						"table1": {
							Name: "table1",
							Columns: map[string]*schema.Column{
								"id": {
									Name:         "id",
									Type:         "integer",
									Nullable:     false,
									Unique:       true,
									PostgresType: "base",
								},
								"name": {
									Name:         "name",
									Type:         "text",
									Nullable:     true,
									Unique:       false,
									PostgresType: "base",
								},
							},
							PrimaryKey: []string{"id"},
							Indexes: map[string]*schema.Index{
								"table1_pkey": {
									Name:       "table1_pkey",
									Unique:     true,
									Columns:    []string{"id"},
									Method:     string(migrations.OpCreateIndexMethodBtree),
									Definition: "CREATE UNIQUE INDEX table1_pkey ON public.table1 USING btree (id)",
								},
								"name_id_unique": {
									Name:       "name_id_unique",
									Unique:     true,
									Columns:    []string{"id", "name"},
									Method:     string(migrations.OpCreateIndexMethodBtree),
									Definition: "CREATE UNIQUE INDEX name_id_unique ON public.table1 USING btree (id, name)",
								},
							},
							UniqueConstraints: map[string]*schema.UniqueConstraint{
								"name_id_unique": {
									Name:    "name_id_unique",
									Columns: []string{"id", "name"},
								},
							},
						},
					},
				},
			},
			{
				name:       "exclusion constraint",
				createStmt: "CREATE TABLE public.table1 (name TEXT, CONSTRAINT name_unique EXCLUDE USING btree (name WITH =));",
				wantSchema: &schema.Schema{
					Name: "public",
					Tables: map[string]*schema.Table{
						"table1": {
							Name: "table1",
							Columns: map[string]*schema.Column{
								"name": {
									Name:         "name",
									Type:         "text",
									Nullable:     true,
									PostgresType: "base",
								},
							},
							Indexes: map[string]*schema.Index{
								"name_unique": {
									Name:       "name_unique",
									Exclusion:  true,
									Unique:     false,
									Columns:    []string{"name"},
									Method:     string(migrations.OpCreateIndexMethodBtree),
									Definition: "CREATE INDEX name_unique ON public.table1 USING btree (name)",
								},
							},
							ExcludeConstraints: map[string]*schema.ExcludeConstraint{
								"name_unique": {
									Name:       "name_unique",
									Columns:    []string{"name"},
									Method:     string(migrations.OpCreateIndexMethodBtree),
									Definition: "EXCLUDE USING btree (name WITH =)",
								},
							},
						},
					},
				},
			},
			{
				name: "multicolumn foreign key constraint",
				createStmt: `CREATE TABLE products(
          customer_id INT NOT NULL, 
          product_id INT NOT NULL, 
          PRIMARY KEY(customer_id, product_id));

          CREATE TABLE orders(
            customer_id INT NOT NULL, 
            product_id INT NOT NULL, 
            CONSTRAINT fk_customer_product FOREIGN KEY (customer_id, product_id) REFERENCES products (customer_id, product_id));`,
				wantSchema: &schema.Schema{
					Name: "public",
					Tables: map[string]*schema.Table{
						"products": {
							Name: "products",
							Columns: map[string]*schema.Column{
								"customer_id": {
									Name:         "customer_id",
									Type:         "integer",
									Nullable:     false,
									PostgresType: "base",
								},
								"product_id": {
									Name:         "product_id",
									Type:         "integer",
									Nullable:     false,
									PostgresType: "base",
								},
							},
							PrimaryKey: []string{"customer_id", "product_id"},
							Indexes: map[string]*schema.Index{
								"products_pkey": {
									Name:       "products_pkey",
									Unique:     true,
									Columns:    []string{"customer_id", "product_id"},
									Method:     string(migrations.OpCreateIndexMethodBtree),
									Definition: "CREATE UNIQUE INDEX products_pkey ON public.products USING btree (customer_id, product_id)",
								},
							},
						},
						"orders": {
							Name: "orders",
							Columns: map[string]*schema.Column{
								"customer_id": {
									Name:         "customer_id",
									Type:         "integer",
									Nullable:     false,
									PostgresType: "base",
								},
								"product_id": {
									Name:         "product_id",
									Type:         "integer",
									Nullable:     false,
									PostgresType: "base",
								},
							},
							ForeignKeys: map[string]*schema.ForeignKey{
								"fk_customer_product": {
									Name:              "fk_customer_product",
									Columns:           []string{"customer_id", "product_id"},
									ReferencedTable:   "products",
									ReferencedColumns: []string{"customer_id", "product_id"},
									MatchType:         "SIMPLE",
									OnDelete:          "NO ACTION",
									OnUpdate:          "NO ACTION",
								},
							},
						},
					},
				},
			},
			{
				name: "multicolumn foreign key constraint with on update action",
				createStmt: `CREATE TABLE products(
          customer_id INT NOT NULL, 
          product_id INT NOT NULL, 
          PRIMARY KEY(customer_id, product_id));

          CREATE TABLE orders(
            customer_id INT NOT NULL, 
            product_id INT NOT NULL, 
            CONSTRAINT fk_customer_product FOREIGN KEY (customer_id, product_id) REFERENCES products (customer_id, product_id) ON UPDATE CASCADE);`,
				wantSchema: &schema.Schema{
					Name: "public",
					Tables: map[string]*schema.Table{
						"products": {
							Name: "products",
							Columns: map[string]*schema.Column{
								"customer_id": {
									Name:         "customer_id",
									Type:         "integer",
									Nullable:     false,
									PostgresType: "base",
								},
								"product_id": {
									Name:         "product_id",
									Type:         "integer",
									Nullable:     false,
									PostgresType: "base",
								},
							},
							PrimaryKey: []string{"customer_id", "product_id"},
							Indexes: map[string]*schema.Index{
								"products_pkey": {
									Name:       "products_pkey",
									Unique:     true,
									Columns:    []string{"customer_id", "product_id"},
									Method:     string(migrations.OpCreateIndexMethodBtree),
									Definition: "CREATE UNIQUE INDEX products_pkey ON public.products USING btree (customer_id, product_id)",
								},
							},
						},
						"orders": {
							Name: "orders",
							Columns: map[string]*schema.Column{
								"customer_id": {
									Name:         "customer_id",
									Type:         "integer",
									Nullable:     false,
									PostgresType: "base",
								},
								"product_id": {
									Name:         "product_id",
									Type:         "integer",
									Nullable:     false,
									PostgresType: "base",
								},
							},
							ForeignKeys: map[string]*schema.ForeignKey{
								"fk_customer_product": {
									Name:              "fk_customer_product",
									Columns:           []string{"customer_id", "product_id"},
									ReferencedTable:   "products",
									ReferencedColumns: []string{"customer_id", "product_id"},
									MatchType:         "SIMPLE",
									OnDelete:          "NO ACTION",
									OnUpdate:          "CASCADE",
								},
							},
						},
					},
				},
			},
			{
				name:       "multi-column index",
				createStmt: "CREATE TABLE public.table1 (a text, b text); CREATE INDEX idx_ab ON public.table1 (a, b);",
				wantSchema: &schema.Schema{
					Name: "public",
					Tables: map[string]*schema.Table{
						"table1": {
							Name: "table1",
							Columns: map[string]*schema.Column{
								"a": {
									Name:         "a",
									Type:         "text",
									Nullable:     true,
									PostgresType: "base",
								},
								"b": {
									Name:         "b",
									Type:         "text",
									Nullable:     true,
									PostgresType: "base",
								},
							},
							Indexes: map[string]*schema.Index{
								"idx_ab": {
									Name:       "idx_ab",
									Unique:     false,
									Columns:    []string{"a", "b"},
									Method:     string(migrations.OpCreateIndexMethodBtree),
									Definition: "CREATE INDEX idx_ab ON public.table1 USING btree (a, b)",
								},
							},
						},
					},
				},
			},
			{
				name:       "column whose type is a UDT in another schema should have the type prefixed with the schema",
				createStmt: "CREATE DOMAIN email_type AS varchar(255); CREATE TABLE public.table1 (a email_type);",
				wantSchema: &schema.Schema{
					Name: "public",
					Tables: map[string]*schema.Table{
						"table1": {
							Name: "table1",
							Columns: map[string]*schema.Column{
								"a": {
									Name:         "a",
									Type:         "public.email_type",
									Nullable:     true,
									PostgresType: "domain",
								},
							},
						},
					},
				},
			},
			{
				name:       "custom enum types",
				createStmt: "CREATE TYPE review AS ENUM ('good', 'bad', 'ugly'); CREATE TABLE public.table1 (name text, review review);",
				wantSchema: &schema.Schema{
					Name: "public",
					Tables: map[string]*schema.Table{
						"table1": {
							Name: "table1",
							Columns: map[string]*schema.Column{
								"name": {
									Name:         "name",
									Type:         "text",
									Nullable:     true,
									PostgresType: "base",
								},
								"review": {
									Name:         "review",
									Type:         "public.review",
									Nullable:     true,
									EnumValues:   []string{"good", "bad", "ugly"},
									PostgresType: "enum",
								},
							},
						},
					},
				},
			},
			{
				name: "postgres type types",
				createStmt: `
					CREATE TYPE comptype AS (f1 int, f2 text);
					CREATE TYPE review AS ENUM ('good', 'bad', 'ugly');
					CREATE TYPE float8_range AS RANGE (subtype = float8, subtype_diff = float8mi);
					CREATE DOMAIN us_postal_code AS TEXT
						CHECK(
							VALUE ~ '^\d{5}$'
							OR VALUE ~ '^\d{5}-\d{4}$'
						);
					CREATE TABLE public.table1 (id bigint, comp_col comptype, enum_col review, range_col float8_range, domain_col us_postal_code);`,
				wantSchema: &schema.Schema{
					Name: "public",
					Tables: map[string]*schema.Table{
						"table1": {
							Name: "table1",
							Columns: map[string]*schema.Column{
								"id": {
									Name:         "id",
									Type:         "bigint",
									Nullable:     true,
									PostgresType: "base",
								},
								"comp_col": {
									Name:         "comp_col",
									Type:         "public.comptype",
									Nullable:     true,
									PostgresType: "composite",
								},
								"enum_col": {
									Name:         "enum_col",
									Type:         "public.review",
									Nullable:     true,
									PostgresType: "enum",
									EnumValues:   []string{"good", "bad", "ugly"},
								},
								"range_col": {
									Name:         "range_col",
									Type:         "public.float8_range",
									Nullable:     true,
									PostgresType: "range",
								},
								"domain_col": {
									Name:         "domain_col",
									Type:         "public.us_postal_code",
									Nullable:     true,
									PostgresType: "domain",
								},
							},
						},
					},
				},
			},
		}

		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				if _, err := db.ExecContext(ctx, "DROP SCHEMA public CASCADE; CREATE SCHEMA public"); err != nil {
					t.Fatal(err)
				}

				if _, err := db.ExecContext(ctx, tt.createStmt); err != nil {
					t.Fatal(err)
				}

				gotSchema, err := state.ReadSchema(ctx, "public")
				if err != nil {
					t.Fatal(err)
				}
				clearOIDS(gotSchema)
				assert.Equal(t, tt.wantSchema, gotSchema)
			})
		}
	})
}

func TestPgrollSchemaVersionUpgrades(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	tests := []struct {
		name                  string
		initialSchemaVersion  string
		pgrollVersion         string
		expectedSchemaVersion string
		expectedError         error
	}{
		{
			name:                  "pgroll schema is older than the pgroll version - pgroll schema is updated",
			initialSchemaVersion:  "0.13.0",
			pgrollVersion:         "0.14.0",
			expectedSchemaVersion: "0.14.0",
		},
		{
			name:                 "pgroll schema is newer than the pgroll version - state initialization fails",
			initialSchemaVersion: "0.15.0",
			pgrollVersion:        "0.14.0",
			expectedError:        state.ErrNewPgrollSchema,
		},
		{
			name:                  "pgroll schema is the same as the pgroll version - pgroll schema is not updated",
			initialSchemaVersion:  "0.13.0",
			pgrollVersion:         "0.13.0",
			expectedSchemaVersion: "0.13.0",
		},
		{
			name:                  "development versions of pgroll never cause a pgroll schema update",
			initialSchemaVersion:  "0.13.0",
			pgrollVersion:         "development",
			expectedSchemaVersion: "0.13.0",
		},
		{
			name:                  "development versions of the pgroll schema are never upgraded",
			initialSchemaVersion:  "development",
			pgrollVersion:         "0.13.0",
			expectedSchemaVersion: "development",
		},
		{
			name:                  "invalid pgroll version - pgroll schema is not updated",
			initialSchemaVersion:  "0.14.0",
			pgrollVersion:         "banana",
			expectedSchemaVersion: "0.14.0",
		},
		{
			name:                  "invalid pgroll schema version - pgroll schema is not updated",
			initialSchemaVersion:  "banana",
			pgrollVersion:         "0.14.0",
			expectedSchemaVersion: "banana",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			testutils.WithStateAtVersionAndConnectionToContainer(t, tt.initialSchemaVersion, func(st *state.State, connStr string, _ *sql.DB) {
				// Create a new state instance with the specified pgroll version. This
				// will upgrade the pgroll schema if necessary.
				s, err := state.New(ctx, connStr, "pgroll", state.WithPgrollVersion(tt.pgrollVersion))

				if tt.expectedError != nil {
					require.ErrorIs(t, err, tt.expectedError)
				} else {
					require.NoError(t, err)
					// Get the version of the pgroll schema
					schemaVersion, err := s.SchemaVersion(ctx)
					require.NoError(t, err)

					// Ensure the expected pgroll schema version
					require.Equal(t, tt.expectedSchemaVersion, schemaVersion)
				}
			})
		})
	}
}

func clearOIDS(s *schema.Schema) {
	for k := range s.Tables {
		c := s.Tables[k]
		c.OID = ""
		s.Tables[k] = c
	}
}
