// SPDX-License-Identifier: Apache-2.0

package migrations_test

import (
	"database/sql"
	"fmt"
	"strings"
	"testing"

	"github.com/xataio/pgroll/pkg/migrations"
)

// testColumns creates OpCreateIndexColumns from ordered column definitions for test fixtures.
// Accepts columns as alternating name (string) and settings (IndexField) pairs.
func testColumns(pairs ...interface{}) migrations.OpCreateIndexColumns {
	cols := migrations.NewOpCreateIndexColumns()
	for i := 0; i < len(pairs); i += 2 {
		name := pairs[i].(string)
		settings := pairs[i+1].(migrations.IndexField)
		cols.Set(name, settings)
	}
	return cols
}

func TestCreateIndex(t *testing.T) {
	t.Parallel()

	invalidName := strings.Repeat("x", 64)
	ExecuteTests(t, TestCases{
		{
			name: "create index",
			migrations: []migrations.Migration{
				{
					Name:          "01_add_table",
					VersionSchema: "add_table",
					Operations: migrations.Operations{
						&migrations.OpCreateTable{
							Name: "users",
							Columns: []migrations.Column{
								{
									Name: "id",
									Type: "serial",
									Pk:   true,
								},
								{
									Name:     "name",
									Type:     "varchar(255)",
									Nullable: false,
								},
							},
						},
					},
				},
			{
				Name:          "02_create_index",
				VersionSchema: "create_index",
				Operations: migrations.Operations{
					&migrations.OpCreateIndex{
						Name:    "idx_users_name",
						Table:   "users",
						Columns: testColumns("name", migrations.IndexField{}),
					},
				},
			},
			},
			afterStart: func(t *testing.T, db *sql.DB, schema string) {
				// The index has been created on the underlying table.
				IndexMustExist(t, db, schema, "users", "idx_users_name")
			},
			afterRollback: func(t *testing.T, db *sql.DB, schema string) {
				// The index has been dropped from the the underlying table.
				IndexMustNotExist(t, db, schema, "users", "idx_users_name")
			},
			afterComplete: func(t *testing.T, db *sql.DB, schema string) {
				// Complete is a no-op.
			},
		},
		{
			name: "create index with a mixed case name",
			migrations: []migrations.Migration{
				{
					Name: "01_add_table",
					Operations: migrations.Operations{
						&migrations.OpCreateTable{
							Name: "users",
							Columns: []migrations.Column{
								{
									Name: "id",
									Type: "serial",
									Pk:   true,
								},
								{
									Name:     "name",
									Type:     "varchar(255)",
									Nullable: false,
								},
							},
						},
					},
				},
				{
					Name: "02_create_index",
					Operations: migrations.Operations{
						&migrations.OpCreateIndex{
							Name:    "idx_USERS_name",
							Table:   "users",
							Columns: testColumns("name", migrations.IndexField{}),
						},
					},
				},
			},
			afterStart: func(t *testing.T, db *sql.DB, schema string) {
				// The index has been created on the underlying table.
				IndexMustExist(t, db, schema, "users", "idx_USERS_name")
			},
			afterRollback: func(t *testing.T, db *sql.DB, schema string) {
				// The index has been dropped from the the underlying table.
				IndexMustNotExist(t, db, schema, "users", "idx_USERS_name")
			},
			afterComplete: func(t *testing.T, db *sql.DB, schema string) {
				// Complete is a no-op.
			},
		},
		{
			name: "create partial index",
			migrations: []migrations.Migration{
				{
					Name: "01_add_table",
					Operations: migrations.Operations{
						&migrations.OpCreateTable{
							Name: "users",
							Columns: []migrations.Column{
								{
									Name: "id",
									Type: "serial",
									Pk:   true,
								},
								{
									Name:     "name",
									Type:     "varchar(255)",
									Nullable: false,
								},
								{
									Name:     "registered_at_year",
									Type:     "integer",
									Nullable: false,
								},
							},
						},
					},
				},
				{
					Name: "02_create_index_for_new_users_after_2019",
					Operations: migrations.Operations{
					&migrations.OpCreateIndex{
						Name:      "idx_users_name_after_2019",
						Table:     "users",
						Columns:   testColumns("registered_at_year", migrations.IndexField{}),
						Predicate: "registered_at_year > 2019",
					},
					},
				},
			},
			afterStart: func(t *testing.T, db *sql.DB, schema string) {
				// The index has been created on the underlying table.
				IndexMustExist(t, db, schema, "users", "idx_users_name_after_2019")
				CheckIndexDefinition(t, db, schema, "users", "idx_users_name_after_2019", fmt.Sprintf("CREATE INDEX idx_users_name_after_2019 ON %s.users USING btree (registered_at_year) WHERE (registered_at_year > 2019)", schema))
			},
			afterRollback: func(t *testing.T, db *sql.DB, schema string) {
				// The index has been dropped from the the underlying table.
				IndexMustNotExist(t, db, schema, "users", "idx_users_name_after_2019")
			},
			afterComplete: func(t *testing.T, db *sql.DB, schema string) {
				// Complete is a no-op.
			},
		},
		{
			name: "create index with descending order",
			migrations: []migrations.Migration{
				{
					Name: "01_add_table",
					Operations: migrations.Operations{
						&migrations.OpCreateTable{
							Name: "users",
							Columns: []migrations.Column{
								{
									Name: "id",
									Type: "serial",
									Pk:   true,
								},
								{
									Name:     "name",
									Type:     "varchar(255)",
									Nullable: false,
								},
							},
						},
					},
				},
				{
					Name: "02_create_index",
					Operations: migrations.Operations{
						&migrations.OpCreateIndex{
							Name:  "idx_users_name",
							Table: "users",
							Columns: testColumns("name", migrations.IndexField{
								Sort: migrations.IndexFieldSortDESC,
							}),
						},
					},
				},
			},
			afterStart: func(t *testing.T, db *sql.DB, schema string) {
				// The index has been created on the underlying table.
				IndexDescendingMustExist(t, db, schema, "users", "idx_users_name", 0)
			},
			afterRollback: func(t *testing.T, db *sql.DB, schema string) {
				// The index has been dropped from the the underlying table.
				IndexMustNotExist(t, db, schema, "users", "idx_users_name")
			},
			afterComplete: func(t *testing.T, db *sql.DB, schema string) {
				// Complete is a no-op.
			},
		},
		{
			name: "invalid name",
			migrations: []migrations.Migration{
				{
					Name: "01_add_table",
					Operations: migrations.Operations{
						&migrations.OpCreateTable{
							Name: "users",
							Columns: []migrations.Column{
								{
									Name: "id",
									Type: "serial",
									Pk:   true,
								},
								{
									Name:     "name",
									Type:     "varchar(255)",
									Nullable: false,
								},
								{
									Name:     "registered_at_year",
									Type:     "integer",
									Nullable: false,
								},
							},
						},
					},
				},
				{
					Name: "02_create_index_with_invalid_name",
					Operations: migrations.Operations{
						&migrations.OpCreateIndex{
							Name:    invalidName,
							Table:   "users",
							Columns: testColumns("registered_at_year", migrations.IndexField{}),
						},
					},
				},
			},
			wantStartErr:  migrations.ValidateIdentifierLength(invalidName),
			afterStart:    func(t *testing.T, db *sql.DB, schema string) {},
			afterRollback: func(t *testing.T, db *sql.DB, schema string) {},
			afterComplete: func(t *testing.T, db *sql.DB, schema string) {},
		},
		{
			name: "create hash index with option",
			migrations: []migrations.Migration{
				{
					Name: "01_add_table",
					Operations: migrations.Operations{
						&migrations.OpCreateTable{
							Name: "users",
							Columns: []migrations.Column{
								{
									Name: "id",
									Type: "serial",
									Pk:   true,
								},
								{
									Name:     "name",
									Type:     "varchar(255)",
									Nullable: false,
								},
							},
						},
					},
				},
				{
					Name: "02_create_hash_index",
					Operations: migrations.Operations{
						&migrations.OpCreateIndex{
							Name:              "idx_users_name_hash",
							Table:             "users",
							Columns:           testColumns("name", migrations.IndexField{}),
							Method:            migrations.OpCreateIndexMethodHash,
							StorageParameters: "fillfactor = 70",
						},
					},
				},
			},
			afterStart: func(t *testing.T, db *sql.DB, schema string) {
				// The index has been created on the underlying table.
				IndexMustExist(t, db, schema, "users", "idx_users_name_hash")
				// Check the index definition.
				CheckIndexDefinition(t, db, schema, "users", "idx_users_name_hash", fmt.Sprintf("CREATE INDEX idx_users_name_hash ON %s.users USING hash (name) WITH (fillfactor='70')", schema))
			},
			afterRollback: func(t *testing.T, db *sql.DB, schema string) {
				// The index has been dropped from the the underlying table.
				IndexMustNotExist(t, db, schema, "users", "idx_users_name_hash")
			},
			afterComplete: func(t *testing.T, db *sql.DB, schema string) {
				// Complete is a no-op.
			},
		},
	})
}

func TestCreateIndexOnMultipleColumns(t *testing.T) {
	t.Parallel()

	ExecuteTests(t, TestCases{{
		name: "create index on multiple columns",
		migrations: []migrations.Migration{
			{
				Name: "01_add_table",
				Operations: migrations.Operations{
					&migrations.OpCreateTable{
						Name: "users",
						Columns: []migrations.Column{
							{
								Name: "id",
								Type: "serial",
								Pk:   true,
							},
							{
								Name:     "name",
								Type:     "varchar(255)",
								Nullable: false,
							},
							{
								Name:     "email",
								Type:     "varchar(255)",
								Nullable: false,
							},
						},
					},
				},
			},
			{
				Name: "02_create_index",
				Operations: migrations.Operations{
					&migrations.OpCreateIndex{
						Name:    "idx_users_name_email",
						Table:   "users",
						Columns: testColumns("name", migrations.IndexField{}, "email", migrations.IndexField{}),
					},
				},
			},
		},
		afterStart: func(t *testing.T, db *sql.DB, schema string) {
			// The index has been created on the underlying table.
			IndexMustExist(t, db, schema, "users", "idx_users_name_email")
		},
		afterRollback: func(t *testing.T, db *sql.DB, schema string) {
			// The index has been dropped from the the underlying table.
			IndexMustNotExist(t, db, schema, "users", "idx_users_name_email")
		},
		afterComplete: func(t *testing.T, db *sql.DB, schema string) {
			// Complete is a no-op.
		},
	}})
}

func TestCreateIndexInMultiOperationMigrations(t *testing.T) {
	t.Parallel()

	ExecuteTests(t, TestCases{
		{
			name: "rename table, create index",
			migrations: []migrations.Migration{
				{
					Name: "01_create_table",
					Operations: migrations.Operations{
						&migrations.OpCreateTable{
							Name: "items",
							Columns: []migrations.Column{
								{
									Name: "id",
									Type: "int",
									Pk:   true,
								},
								{
									Name:     "name",
									Type:     "varchar(255)",
									Nullable: true,
								},
							},
						},
					},
				},
				{
					Name: "02_multi_operation",
					Operations: migrations.Operations{
						&migrations.OpRenameTable{
							From: "items",
							To:   "products",
						},
					&migrations.OpCreateIndex{
						Table:   "products",
						Columns: testColumns("name", migrations.IndexField{}),
						Name:    "idx_products_name",
					},
					},
				},
			},
			afterStart: func(t *testing.T, db *sql.DB, schema string) {
				// The index has been created on the underlying table.
				IndexMustExist(t, db, schema, "items", "idx_products_name")
			},
			afterRollback: func(t *testing.T, db *sql.DB, schema string) {
				// The index has been dropped from the the underlying table.
				IndexMustNotExist(t, db, schema, "items", "idx_products_name")
			},
			afterComplete: func(t *testing.T, db *sql.DB, schema string) {
				// The index remains on the underlying table.
				IndexMustExist(t, db, schema, "products", "idx_products_name")
			},
		},
		{
			name: "rename table, rename column, create index",
			migrations: []migrations.Migration{
				{
					Name: "01_create_table",
					Operations: migrations.Operations{
						&migrations.OpCreateTable{
							Name: "items",
							Columns: []migrations.Column{
								{
									Name: "id",
									Type: "int",
									Pk:   true,
								},
								{
									Name:     "name",
									Type:     "varchar(255)",
									Nullable: true,
								},
							},
						},
					},
				},
				{
					Name: "02_multi_operation",
					Operations: migrations.Operations{
						&migrations.OpRenameTable{
							From: "items",
							To:   "products",
						},
						&migrations.OpRenameColumn{
							Table: "products",
							From:  "name",
							To:    "item_name",
						},
					&migrations.OpCreateIndex{
						Table:   "products",
						Columns: testColumns("item_name", migrations.IndexField{}),
						Name:    "idx_products_item_name",
					},
					},
				},
			},
			afterStart: func(t *testing.T, db *sql.DB, schema string) {
				// The index has been created on the underlying table.
				IndexMustExist(t, db, schema, "items", "idx_products_item_name")
			},
			afterRollback: func(t *testing.T, db *sql.DB, schema string) {
				// The index has been dropped from the the underlying table.
				IndexMustNotExist(t, db, schema, "items", "idx_products_item_name")
			},
			afterComplete: func(t *testing.T, db *sql.DB, schema string) {
				// The index remains on the underlying table.
				IndexMustExist(t, db, schema, "products", "idx_products_item_name")
			},
		},
		{
			name: "create index on newly created table",
			migrations: []migrations.Migration{
				{
					Name: "01_add_table",
					Operations: migrations.Operations{
						&migrations.OpCreateTable{
							Name: "users",
							Columns: []migrations.Column{
								{
									Name: "id",
									Type: "serial",
									Pk:   true,
								},
								{
									Name:     "name",
									Type:     "varchar(255)",
									Nullable: false,
							},
						},
					},
					&migrations.OpCreateIndex{
						Name:    "idx_users_name",
						Table:   "users",
						Columns: testColumns("name", migrations.IndexField{}),
					},
				},
			},
		},
		afterStart: func(t *testing.T, db *sql.DB, schema string) {
			// The index has been created on the underlying table.
			IndexMustExist(t, db, schema, "users", "idx_users_name")
			},
			afterRollback: func(t *testing.T, db *sql.DB, schema string) {
				// The index has been dropped from the the underlying table.
				IndexMustNotExist(t, db, schema, "users", "idx_users_name")
			},
			afterComplete: func(t *testing.T, db *sql.DB, schema string) {
				// The index remains on the underlying table.
				IndexMustExist(t, db, schema, "users", "idx_users_name")
			},
		},
		{
			name: "create index on newly created column",
			migrations: []migrations.Migration{
				{
					Name: "01_add_table",
					Operations: migrations.Operations{
						&migrations.OpCreateTable{
							Name: "users",
							Columns: []migrations.Column{
								{
									Name: "id",
									Type: "serial",
									Pk:   true,
								},
								{
									Name:     "name",
									Type:     "varchar(255)",
									Nullable: false,
								},
							},
						},
					},
				},
				{
					Name: "02_add_column_and_index",
					Operations: migrations.Operations{
						&migrations.OpAddColumn{
							Table: "users",
							Column: migrations.Column{
								Name:     "age",
								Type:     "integer",
								Nullable: true,
							},
							Up: "18",
						},
						&migrations.OpCreateIndex{
							Name:    "idx_users_age",
							Table:   "users",
							Columns: testColumns("age", migrations.IndexField{}),
						},
					},
				},
			},
			afterStart: func(t *testing.T, db *sql.DB, schema string) {
				// The index has been created on the underlying table.
				IndexMustExist(t, db, schema, "users", "idx_users_age")
			},
			afterRollback: func(t *testing.T, db *sql.DB, schema string) {
				// The index has been dropped from the the underlying table.
				IndexMustNotExist(t, db, schema, "users", "idx_users_age")
			},
			afterComplete: func(t *testing.T, db *sql.DB, schema string) {
				// The index has been created on the underlying table.
				IndexMustExist(t, db, schema, "users", "idx_users_age")
			},
		},
		{
			name: "create index with multiple columns preserves order",
			migrations: []migrations.Migration{
				{
					Name: "01_add_table",
					Operations: migrations.Operations{
						&migrations.OpCreateTable{
							Name: "users",
							Columns: []migrations.Column{
								{
									Name: "id",
									Type: "serial",
									Pk:   true,
								},
								{
									Name:     "tool_call_id",
									Type:     "integer",
									Nullable: false,
								},
								{
									Name:     "created_at",
									Type:     "timestamp",
									Nullable: false,
								},
							},
						},
					},
				},
				{
					Name: "02_create_index",
					Operations: migrations.Operations{
						&migrations.OpCreateIndex{
							Name:  "idx_tool_call_created",
							Table: "users",
							Columns: func() migrations.OpCreateIndexColumns {
								cols := migrations.NewOpCreateIndexColumns()
								cols.Set("tool_call_id", migrations.IndexField{})
								cols.Set("created_at", migrations.IndexField{})
								return cols
							}(),
						},
					},
				},
			},
			afterStart: func(t *testing.T, db *sql.DB, schema string) {
				// Verify index created with correct column order
				// Query pg_indexes to verify: tool_call_id, created_at (not created_at, tool_call_id)
				var indexdef string
				err := db.QueryRow(`
					SELECT indexdef 
					FROM pg_indexes 
					WHERE schemaname = $1 AND indexname = 'idx_tool_call_created'
				`, schema).Scan(&indexdef)
				if err != nil {
					t.Fatal(err)
				}
				// Verify the correct order: tool_call_id should come before created_at
				if !strings.Contains(indexdef, "(tool_call_id, created_at)") {
					t.Fatalf("Expected index to have columns in order (tool_call_id, created_at), got: %s", indexdef)
				}
				// Verify it's NOT in the wrong order
				if strings.Contains(indexdef, "(created_at, tool_call_id)") {
					t.Fatalf("Index has columns in wrong order (created_at, tool_call_id), got: %s", indexdef)
				}
			},
			afterRollback: func(t *testing.T, db *sql.DB, schema string) {
				// The index has been dropped from the the underlying table.
				IndexMustNotExist(t, db, schema, "users", "idx_tool_call_created")
			},
			afterComplete: func(t *testing.T, db *sql.DB, schema string) {
				// The index has been created on the underlying table with correct order.
				IndexMustExist(t, db, schema, "users", "idx_tool_call_created")
				var indexdef string
				err := db.QueryRow(`
					SELECT indexdef 
					FROM pg_indexes 
					WHERE schemaname = $1 AND indexname = 'idx_tool_call_created'
				`, schema).Scan(&indexdef)
				if err != nil {
					t.Fatal(err)
				}
				if !strings.Contains(indexdef, "(tool_call_id, created_at)") {
					t.Fatalf("Expected index to have columns in order (tool_call_id, created_at), got: %s", indexdef)
				}
			},
		},
	})
}
