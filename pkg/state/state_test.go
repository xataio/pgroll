// SPDX-License-Identifier: Apache-2.0

package state_test

import (
	"context"
	"database/sql"
	"os"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
	"github.com/stretchr/testify/assert"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/postgres"
	"github.com/testcontainers/testcontainers-go/wait"
	"github.com/xataio/pgroll/pkg/migrations"
	"github.com/xataio/pgroll/pkg/schema"
	"github.com/xataio/pgroll/pkg/state"
)

// The version of postgres against which the tests are run
// if the POSTGRES_VERSION environment variable is not set.
const defaultPostgresVersion = "15.3"

func TestSchemaOptionIsRespected(t *testing.T) {
	t.Parallel()

	witStateAndConnectionToContainer(t, func(state *state.State, db *sql.DB) {
		ctx := context.Background()

		// create a table in the public schema
		if _, err := db.ExecContext(ctx, "CREATE TABLE public.table1 (id int)"); err != nil {
			t.Fatal(err)
		}

		// init the state
		if err := state.Init(ctx); err != nil {
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

func TestReadSchema(t *testing.T) {
	t.Parallel()

	witStateAndConnectionToContainer(t, func(state *state.State, db *sql.DB) {
		ctx := context.Background()

		tests := []struct {
			name       string
			createStmt string
			wantSchema *schema.Schema
		}{
			{
				name:       "one table",
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
							Indexes: map[string]schema.Index{
								"id_unique": {
									Name: "id_unique",
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
									Name: "table1_pkey",
								},
							},
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
							ForeignKeys: []schema.ForeignKey{
								{
									Name:              "fk_fkey",
									Columns:           []string{"fk"},
									ReferencedTable:   "table1",
									ReferencedColumns: []string{"id"},
								},
							},
						},
					},
				},
			},
		}

		// init the state
		if err := state.Init(ctx); err != nil {
			t.Fatal(err)
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

func witStateAndConnectionToContainer(t *testing.T, fn func(*state.State, *sql.DB)) {
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

	db, err := sql.Open("postgres", cStr)
	if err != nil {
		t.Fatal(err)
	}

	t.Cleanup(func() {
		if err := db.Close(); err != nil {
			t.Fatalf("Failed to close database connection: %v", err)
		}
	})

	st, err := state.New(ctx, cStr, "pgroll")
	if err != nil {
		t.Fatal(err)
	}

	fn(st, db)
}
