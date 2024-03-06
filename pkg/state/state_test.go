// SPDX-License-Identifier: Apache-2.0

package state_test

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"strings"
	"sync"
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
	"github.com/stretchr/testify/assert"
	"github.com/xataio/pgroll/pkg/migrations"
	"github.com/xataio/pgroll/pkg/schema"
	"github.com/xataio/pgroll/pkg/state"
	"github.com/xataio/pgroll/pkg/testutils"
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

		// check that starting a new migration returns the already existing table
		currentSchema, err := state.Start(ctx, "public", &migrations.Migration{
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

		assert.Equal(t, 1, len(currentSchema.Tables))
		assert.Equal(t, "public", currentSchema.Name)
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
				name: "drop table",
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
				name: "drop column",
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
				name: "drop constraint",
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
				defer rows.Close()

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

				assert.Equal(t, len(tt.wantMigrations), len(gotMigrations), "unexpected number of migrations")

				for i, wantMigration := range tt.wantMigrations {
					gotMigration := gotMigrations[i]

					// test there is a name for the migration, then remove it for the comparison
					assert.True(t, strings.HasPrefix(gotMigration.Name, "sql_") && len(gotMigration.Name) > 10)
					gotMigration.Name = ""

					if diff := cmp.Diff(wantMigration, gotMigration); diff != "" {
						t.Errorf("expected schema mismatch (-want +got):\n%s", diff)
					}
				}
			})
		}
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
					Tables: map[string]schema.Table{},
				},
			},
			{
				name:       "one table without columns",
				createStmt: "CREATE TABLE public.table1 ()",
				wantSchema: &schema.Schema{
					Name: "public",
					Tables: map[string]schema.Table{
						"table1": {
							Name:              "table1",
							Columns:           map[string]schema.Column{},
							PrimaryKey:        []string{},
							Indexes:           map[string]schema.Index{},
							CheckConstraints:  map[string]schema.CheckConstraint{},
							UniqueConstraints: map[string]schema.UniqueConstraint{},
							ForeignKeys:       map[string]schema.ForeignKey{},
						},
					},
				},
			},
			{
				name:       "one table with columns",
				createStmt: "CREATE TABLE public.table1 (id int)",
				wantSchema: &schema.Schema{
					Name: "public",
					Tables: map[string]schema.Table{
						"table1": {
							Name: "table1",
							Columns: map[string]schema.Column{
								"id": {
									Name:     "id",
									Type:     "integer",
									Nullable: true,
								},
							},
							PrimaryKey:        []string{},
							Indexes:           map[string]schema.Index{},
							CheckConstraints:  map[string]schema.CheckConstraint{},
							UniqueConstraints: map[string]schema.UniqueConstraint{},
							ForeignKeys:       map[string]schema.ForeignKey{},
						},
					},
				},
			},
			{
				name:       "unique, not null",
				createStmt: "CREATE TABLE public.table1 (id int NOT NULL, CONSTRAINT id_unique UNIQUE(id))",
				wantSchema: &schema.Schema{
					Name: "public",
					Tables: map[string]schema.Table{
						"table1": {
							Name: "table1",
							Columns: map[string]schema.Column{
								"id": {
									Name:     "id",
									Type:     "integer",
									Nullable: false,
									Unique:   true,
								},
							},
							PrimaryKey: []string{},
							Indexes: map[string]schema.Index{
								"id_unique": {
									Name:    "id_unique",
									Unique:  true,
									Columns: []string{"id"},
								},
							},
							CheckConstraints: map[string]schema.CheckConstraint{},
							UniqueConstraints: map[string]schema.UniqueConstraint{
								"id_unique": {
									Name:    "id_unique",
									Columns: []string{"id"},
								},
							},
							ForeignKeys: map[string]schema.ForeignKey{},
						},
					},
				},
			},
			{
				name:       "non-unique index",
				createStmt: "CREATE TABLE public.table1 (id int, name text); CREATE INDEX idx_name ON public.table1 (name)",
				wantSchema: &schema.Schema{
					Name: "public",
					Tables: map[string]schema.Table{
						"table1": {
							Name: "table1",
							Columns: map[string]schema.Column{
								"id": {
									Name:     "id",
									Type:     "integer",
									Nullable: true,
								},
								"name": {
									Name:     "name",
									Type:     "text",
									Nullable: true,
								},
							},
							PrimaryKey: []string{},
							Indexes: map[string]schema.Index{
								"idx_name": {
									Name:    "idx_name",
									Unique:  false,
									Columns: []string{"name"},
								},
							},
							CheckConstraints:  map[string]schema.CheckConstraint{},
							UniqueConstraints: map[string]schema.UniqueConstraint{},
							ForeignKeys:       map[string]schema.ForeignKey{},
						},
					},
				},
			},
			{
				name:       "foreign key",
				createStmt: "CREATE TABLE public.table1 (id int PRIMARY KEY); CREATE TABLE public.table2 (fk int NOT NULL, CONSTRAINT fk_fkey FOREIGN KEY (fk) REFERENCES public.table1 (id))",
				wantSchema: &schema.Schema{
					Name: "public",
					Tables: map[string]schema.Table{
						"table1": {
							Name: "table1",
							Columns: map[string]schema.Column{
								"id": {
									Name:     "id",
									Type:     "integer",
									Nullable: false,
									Unique:   true,
								},
							},
							PrimaryKey: []string{"id"},
							Indexes: map[string]schema.Index{
								"table1_pkey": {
									Name:    "table1_pkey",
									Unique:  true,
									Columns: []string{"id"},
								},
							},
							CheckConstraints:  map[string]schema.CheckConstraint{},
							UniqueConstraints: map[string]schema.UniqueConstraint{},
							ForeignKeys:       map[string]schema.ForeignKey{},
						},
						"table2": {
							Name: "table2",
							Columns: map[string]schema.Column{
								"fk": {
									Name:     "fk",
									Type:     "integer",
									Nullable: false,
								},
							},
							PrimaryKey: []string{},
							Indexes:    map[string]schema.Index{},
							ForeignKeys: map[string]schema.ForeignKey{
								"fk_fkey": {
									Name:              "fk_fkey",
									Columns:           []string{"fk"},
									ReferencedTable:   "table1",
									ReferencedColumns: []string{"id"},
								},
							},
							CheckConstraints:  map[string]schema.CheckConstraint{},
							UniqueConstraints: map[string]schema.UniqueConstraint{},
						},
					},
				},
			},
			{
				name:       "check constraint",
				createStmt: "CREATE TABLE public.table1 (id int PRIMARY KEY, age INTEGER, CONSTRAINT age_check CHECK (age > 18));",
				wantSchema: &schema.Schema{
					Name: "public",
					Tables: map[string]schema.Table{
						"table1": {
							Name: "table1",
							Columns: map[string]schema.Column{
								"id": {
									Name:     "id",
									Type:     "integer",
									Nullable: false,
									Unique:   true,
								},
								"age": {
									Name:     "age",
									Type:     "integer",
									Nullable: true,
								},
							},
							PrimaryKey: []string{"id"},
							Indexes: map[string]schema.Index{
								"table1_pkey": {
									Name:    "table1_pkey",
									Unique:  true,
									Columns: []string{"id"},
								},
							},
							ForeignKeys: map[string]schema.ForeignKey{},
							CheckConstraints: map[string]schema.CheckConstraint{
								"age_check": {
									Name:       "age_check",
									Columns:    []string{"age"},
									Definition: "CHECK ((age > 18))",
								},
							},
							UniqueConstraints: map[string]schema.UniqueConstraint{},
						},
					},
				},
			},
			{
				name:       "unique constraint",
				createStmt: "CREATE TABLE public.table1 (id int PRIMARY KEY, name TEXT, CONSTRAINT name_unique UNIQUE(name) );",
				wantSchema: &schema.Schema{
					Name: "public",
					Tables: map[string]schema.Table{
						"table1": {
							Name: "table1",
							Columns: map[string]schema.Column{
								"id": {
									Name:     "id",
									Type:     "integer",
									Nullable: false,
									Unique:   true,
								},
								"name": {
									Name:     "name",
									Type:     "text",
									Unique:   true,
									Nullable: true,
								},
							},
							PrimaryKey: []string{"id"},
							Indexes: map[string]schema.Index{
								"table1_pkey": {
									Name:    "table1_pkey",
									Unique:  true,
									Columns: []string{"id"},
								},
								"name_unique": {
									Name:    "name_unique",
									Unique:  true,
									Columns: []string{"name"},
								},
							},
							ForeignKeys:      map[string]schema.ForeignKey{},
							CheckConstraints: map[string]schema.CheckConstraint{},
							UniqueConstraints: map[string]schema.UniqueConstraint{
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
					Tables: map[string]schema.Table{
						"table1": {
							Name: "table1",
							Columns: map[string]schema.Column{
								"id": {
									Name:     "id",
									Type:     "integer",
									Nullable: false,
									Unique:   true,
								},
								"name": {
									Name:     "name",
									Type:     "text",
									Nullable: true,
									Unique:   false,
								},
							},
							PrimaryKey: []string{"id"},
							Indexes: map[string]schema.Index{
								"table1_pkey": {
									Name:    "table1_pkey",
									Unique:  true,
									Columns: []string{"id"},
								},
								"name_id_unique": {
									Name:    "name_id_unique",
									Unique:  true,
									Columns: []string{"id", "name"},
								},
							},
							ForeignKeys:      map[string]schema.ForeignKey{},
							CheckConstraints: map[string]schema.CheckConstraint{},
							UniqueConstraints: map[string]schema.UniqueConstraint{
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
				name:       "multi-column index",
				createStmt: "CREATE TABLE public.table1 (a text, b text); CREATE INDEX idx_ab ON public.table1 (a, b);",
				wantSchema: &schema.Schema{
					Name: "public",
					Tables: map[string]schema.Table{
						"table1": {
							Name: "table1",
							Columns: map[string]schema.Column{
								"a": {
									Name:     "a",
									Type:     "text",
									Nullable: true,
								},
								"b": {
									Name:     "b",
									Type:     "text",
									Nullable: true,
								},
							},
							PrimaryKey: []string{},
							Indexes: map[string]schema.Index{
								"idx_ab": {
									Name:    "idx_ab",
									Unique:  false,
									Columns: []string{"a", "b"},
								},
							},
							ForeignKeys:       map[string]schema.ForeignKey{},
							CheckConstraints:  map[string]schema.CheckConstraint{},
							UniqueConstraints: map[string]schema.UniqueConstraint{},
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
				if diff := cmp.Diff(tt.wantSchema, gotSchema, cmpopts.IgnoreFields(schema.Table{}, "OID")); diff != "" {
					t.Errorf("expected schema mismatch (-want +got):\n%s", diff)
				}
			})
		}
	})
}
