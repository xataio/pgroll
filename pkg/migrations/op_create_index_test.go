// SPDX-License-Identifier: Apache-2.0

package migrations_test

import (
	"database/sql"
	"fmt"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/xataio/pgroll/pkg/migrations"
)

func TestCreateIndex(t *testing.T) {
	t.Parallel()

	invalidName := strings.Repeat("x", 64)
	ExecuteTests(t, TestCases{
		{
			name: "create multi-column index with array format preserves order",
			migrations: []migrations.Migration{
				{
					Name: "01_add_table",
					Operations: migrations.Operations{
						&migrations.OpCreateTable{
							Name: "products",
							Columns: []migrations.Column{
								{Name: "id", Type: "serial", Pk: true},
								{Name: "user_id", Type: "integer"},
								{Name: "status", Type: "text"},
								{Name: "created_at", Type: "timestamp"},
							},
						},
					},
				},
				{
					Name: "02_create_multicolumn_index",
					Operations: migrations.Operations{
						&migrations.OpCreateIndex{
							Name:  "idx_products_composite",
							Table: "products",
							// Non-alphabetical order to catch map-based bugs
							Columns: []migrations.IndexColumn{
								{Name: "status"},
								{Name: "user_id"},
								{Name: "created_at", Sort: migrations.IndexFieldSortDESC},
							},
						},
					},
				},
			},
			afterStart: func(t *testing.T, db *sql.DB, schema string) {
				IndexMustExist(t, db, schema, "products", "idx_products_composite")
				
				// Verify column order in the actual index definition
				var indexdef string
				err := db.QueryRow(`
					SELECT indexdef 
					FROM pg_indexes 
					WHERE schemaname = $1 
					AND tablename = 'products' 
					AND indexname = 'idx_products_composite'
				`, schema).Scan(&indexdef)
				require.NoError(t, err)
				
				// Check that columns appear in correct order: status, user_id, created_at
				statusPos := strings.Index(indexdef, "status")
				userIdPos := strings.Index(indexdef, "user_id")
				createdAtPos := strings.Index(indexdef, "created_at")
				
				require.True(t, statusPos < userIdPos && userIdPos < createdAtPos,
					"Index columns should be in order: status < user_id < created_at. Got: %s", indexdef)
				require.Contains(t, indexdef, "created_at DESC",
					"created_at should have DESC modifier: %s", indexdef)
			},
			afterRollback: func(t *testing.T, db *sql.DB, schema string) {
				IndexMustNotExist(t, db, schema, "products", "idx_products_composite")
			},
			afterComplete: func(t *testing.T, db *sql.DB, schema string) {},
		},

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
						Columns: []migrations.IndexColumn{{Name: "name"}},
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
							Columns: []migrations.IndexColumn{{Name: "name"}},
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
							Columns:   []migrations.IndexColumn{{Name: "registered_at_year"}},
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
							Columns: []migrations.IndexColumn{
								{
									Name: "name",
									Sort: migrations.IndexFieldSortDESC,
								},
							},
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
							Columns: []migrations.IndexColumn{{Name: "registered_at_year"}},
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
							Columns:           []migrations.IndexColumn{{Name: "name"}},
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
						Columns: []migrations.IndexColumn{{Name: "name"}, {Name: "email"}},
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
							Columns: []migrations.IndexColumn{{Name: "name"}},
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
							Columns: []migrations.IndexColumn{{Name: "item_name"}},
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
						Columns: []migrations.IndexColumn{{Name: "name"}},
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
							Columns: []migrations.IndexColumn{{Name: "age"}},
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
	})
}
