// SPDX-License-Identifier: Apache-2.0

package migrations_test

import (
	"database/sql"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/xataio/pgroll/internal/testutils"
	"github.com/xataio/pgroll/pkg/migrations"
)

func TestOpRenameColumn(t *testing.T) {
	t.Parallel()

	ExecuteTests(t, TestCases{
		{
			name: "rename column",
			migrations: []migrations.Migration{
				{
					Name: "01_create_table",
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
									Name:     "username",
									Type:     "varchar(255)",
									Nullable: false,
								},
							},
						},
					},
				},
				{
					Name: "02_rename_column",
					Operations: migrations.Operations{
						&migrations.OpRenameColumn{
							Table: "users",
							From:  "username",
							To:    "name",
						},
					},
				},
			},
			afterStart: func(t *testing.T, db *sql.DB, schema string) {
				// The column in the underlying table has not been renamed.
				ColumnMustExist(t, db, schema, "users", "username")

				// Insertions to the new column name in the new version schema should work.
				MustInsert(t, db, schema, "02_rename_column", "users", map[string]string{
					"name": "alice",
				})

				// Insertions to the old column name in the old version schema should work.
				MustInsert(t, db, schema, "01_create_table", "users", map[string]string{
					"username": "bob",
				})

				// Data can be read from the view in the new version schema.
				rows := MustSelect(t, db, schema, "02_rename_column", "users")
				assert.Equal(t, []map[string]any{
					{"id": 1, "name": "alice"},
					{"id": 2, "name": "bob"},
				}, rows)
			},
			afterRollback: func(t *testing.T, db *sql.DB, schema string) {
				// no-op
			},
			afterComplete: func(t *testing.T, db *sql.DB, schema string) {
				// The column in the underlying table has been renamed.
				ColumnMustExist(t, db, schema, "users", "name")

				// Data can be read from the view in the new version schema.
				rows := MustSelect(t, db, schema, "02_rename_column", "users")
				assert.Equal(t, []map[string]any{
					{"id": 1, "name": "alice"},
					{"id": 2, "name": "bob"},
				}, rows)
			},
		},
		{
			name: "rename serial column",
			migrations: []migrations.Migration{
				{
					Name: "01_create_table",
					Operations: migrations.Operations{
						&migrations.OpCreateTable{
							Name: "orders",
							Columns: []migrations.Column{
								{
									Name: "order_id",
									Type: "serial",
									Pk:   true,
								},
								{
									Name: "description",
									Type: "varchar(255)",
								},
							},
						},
					},
				},
				{
					Name: "02_rename_serial_column",
					Operations: migrations.Operations{
						&migrations.OpRenameColumn{
							Table: "orders",
							From:  "order_id",
							To:    "id",
						},
					},
				},
			},
			afterStart: func(t *testing.T, db *sql.DB, schema string) {
				// The column in the underlying table has not been renamed.
				ColumnMustExist(t, db, schema, "orders", "order_id")

				// Insertions to the new column name in the new version schema should work.
				MustInsert(t, db, schema, "02_rename_serial_column", "orders", map[string]string{
					"id":          "1",
					"description": "first order",
				})

				// Insertions to the old column name in the old version schema should work.
				MustInsert(t, db, schema, "01_create_table", "orders", map[string]string{
					"order_id":    "2",
					"description": "second order",
				})

				// Data can be read from the view in the new version schema.
				rows := MustSelect(t, db, schema, "02_rename_serial_column", "orders")
				assert.Equal(t, []map[string]any{
					{"id": 1, "description": "first order"},
					{"id": 2, "description": "second order"},
				}, rows)
			},
			afterRollback: func(t *testing.T, db *sql.DB, schema string) {
				// no-op
			},
			afterComplete: func(t *testing.T, db *sql.DB, schema string) {
				// The column in the underlying table has been renamed.
				ColumnMustExist(t, db, schema, "orders", "id")

				// Data can be read from the view in the new version schema.
				rows := MustSelect(t, db, schema, "02_rename_serial_column", "orders")
				assert.Equal(t, []map[string]any{
					{"id": 1, "description": "first order"},
					{"id": 2, "description": "second order"},
				}, rows)
			},
		},
	})
}

func TestRenameColumnInMultiOperationMigrations(t *testing.T) {
	t.Parallel()

	ExecuteTests(t, TestCases{
		{
			name: "add column, rename column",
			migrations: []migrations.Migration{
				{
					Name: "01_create_table",
					Operations: migrations.Operations{
						&migrations.OpCreateTable{
							Name: "items",
							Columns: []migrations.Column{
								{
									Name: "id",
									Type: "serial",
									Pk:   true,
								},
								{
									Name: "name",
									Type: "varchar(255)",
								},
							},
						},
					},
				},
				{
					Name: "02_multi_operation",
					Operations: migrations.Operations{
						&migrations.OpAddColumn{
							Table: "items",
							Column: migrations.Column{
								Name:     "description",
								Type:     "text",
								Nullable: true,
							},
						},
						&migrations.OpRenameColumn{
							Table: "items",
							From:  "description",
							To:    "item_description",
						},
					},
				},
			},
			afterStart: func(t *testing.T, db *sql.DB, schema string) {
				// Can insert into the new column under its new name
				MustInsert(t, db, schema, "02_multi_operation", "items", map[string]string{
					"name":             "apples",
					"item_description": "amazing",
				})

				// Can't insert into the new column under its old name
				MustNotInsert(t, db, schema, "02_multi_operation", "items", map[string]string{
					"name":        "bananas",
					"description": "brilliant",
				}, testutils.UndefinedColumnErrorCode)

				// The table has the expected rows in the new schema
				rows := MustSelect(t, db, schema, "02_multi_operation", "items")
				assert.Equal(t, []map[string]any{
					{"id": 1, "name": "apples", "item_description": "amazing"},
				}, rows)
			},
			afterRollback: func(t *testing.T, db *sql.DB, schema string) {
				// The table has been cleaned up
				TableMustBeCleanedUp(t, db, schema, "items", "description")
			},
			afterComplete: func(t *testing.T, db *sql.DB, schema string) {
				// Can insert into the new column under its new name
				MustInsert(t, db, schema, "02_multi_operation", "items", map[string]string{
					"name":             "bananas",
					"item_description": "brilliant",
				})

				// The table has the expected rows in the new schema
				rows := MustSelect(t, db, schema, "02_multi_operation", "items")
				assert.Equal(t, []map[string]any{
					{"id": 1, "name": "apples", "item_description": nil},
					{"id": 2, "name": "bananas", "item_description": "brilliant"},
				}, rows)
			},
		},
		{
			name: "rename table, rename column",
			migrations: []migrations.Migration{
				{
					Name: "01_create_table",
					Operations: migrations.Operations{
						&migrations.OpCreateTable{
							Name: "items",
							Columns: []migrations.Column{
								{
									Name: "id",
									Type: "serial",
									Pk:   true,
								},
								{
									Name: "name",
									Type: "varchar(255)",
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
					},
				},
			},
			afterStart: func(t *testing.T, db *sql.DB, schema string) {
				// Can insert using the old version schema (old table name, and old
				// column name)
				MustInsert(t, db, schema, "01_create_table", "items", map[string]string{
					"name": "apples",
				})

				// Can insert using the new version schema (new table name, and new
				// column name)
				MustInsert(t, db, schema, "02_multi_operation", "products", map[string]string{
					"item_name": "bananas",
				})

				// The table has the expected rows
				rows := MustSelect(t, db, schema, "02_multi_operation", "products")
				assert.Equal(t, []map[string]any{
					{"id": 1, "item_name": "apples"},
					{"id": 2, "item_name": "bananas"},
				}, rows)
			},
			afterRollback: func(t *testing.T, db *sql.DB, schema string) {
				// Can insert using the old version schema (old table name, and old
				// column name)
				MustInsert(t, db, schema, "01_create_table", "items", map[string]string{
					"name": "carrots",
				})
			},
			afterComplete: func(t *testing.T, db *sql.DB, schema string) {
				// Can insert using the new version schema (new table name, and new
				// column name)
				MustInsert(t, db, schema, "02_multi_operation", "products", map[string]string{
					"item_name": "durian",
				})

				// The table has the expected rows
				rows := MustSelect(t, db, schema, "02_multi_operation", "products")
				assert.Equal(t, []map[string]any{
					{"id": 1, "item_name": "apples"},
					{"id": 2, "item_name": "bananas"},
					{"id": 3, "item_name": "carrots"},
					{"id": 4, "item_name": "durian"},
				}, rows)
			},
		},
		{
			name: "rename column, drop column",
			migrations: []migrations.Migration{
				{
					Name: "01_create_table",
					Operations: migrations.Operations{
						&migrations.OpCreateTable{
							Name: "items",
							Columns: []migrations.Column{
								{
									Name: "id",
									Type: "serial",
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
						&migrations.OpRenameColumn{
							Table: "items",
							From:  "name",
							To:    "item_name",
						},
						&migrations.OpDropColumn{
							Table:  "items",
							Column: "item_name",
						},
					},
				},
			},
			afterStart: func(t *testing.T, db *sql.DB, schema string) {
				// Can't insert into the dropped column in the new schema
				MustNotInsert(t, db, schema, "02_multi_operation", "items", map[string]string{
					"item_name": "apples",
				}, testutils.UndefinedColumnErrorCode)

				// Can insert into the old column name in the old schema
				MustInsert(t, db, schema, "01_create_table", "items", map[string]string{
					"name": "apples",
				})
			},
			afterRollback: func(t *testing.T, db *sql.DB, schema string) {
				// Can insert into the old column name in the old schema
				MustInsert(t, db, schema, "01_create_table", "items", map[string]string{
					"name": "bananas",
				})
			},
			afterComplete: func(t *testing.T, db *sql.DB, schema string) {
				// Can't insert into the dropped column in the new schema
				MustNotInsert(t, db, schema, "02_multi_operation", "items", map[string]string{
					"item_name": "apples",
				}, testutils.UndefinedColumnErrorCode)
			},
		},
	})
}

func TestOpRenameColumnValidation(t *testing.T) {
	t.Parallel()

	createTableMigration := migrations.Migration{
		Name: "01_create_table",
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
						Name:     "username",
						Type:     "varchar(255)",
						Nullable: false,
					},
				},
			},
		},
	}

	ExecuteTests(t, TestCases{
		{
			name: "from field must be specified",
			migrations: []migrations.Migration{
				createTableMigration,
				{
					Name: "02_rename_column",
					Operations: migrations.Operations{
						&migrations.OpRenameColumn{
							Table: "users",
							To:    "name",
						},
					},
				},
			},
			wantStartErr: migrations.FieldRequiredError{Name: "from"},
		},
		{
			name: "to field must be specified",
			migrations: []migrations.Migration{
				createTableMigration,
				{
					Name: "02_rename_column",
					Operations: migrations.Operations{
						&migrations.OpRenameColumn{
							Table: "users",
							From:  "username",
						},
					},
				},
			},
			wantStartErr: migrations.FieldRequiredError{Name: "to"},
		},
		{
			name: "table must exist",
			migrations: []migrations.Migration{
				createTableMigration,
				{
					Name: "02_rename_column",
					Operations: migrations.Operations{
						&migrations.OpRenameColumn{
							Table: "doesntexist",
							From:  "username",
							To:    "id",
						},
					},
				},
			},
			wantStartErr: migrations.TableDoesNotExistError{Name: "doesntexist"},
		},
		{
			name: "source column must exist",
			migrations: []migrations.Migration{
				createTableMigration,
				{
					Name: "02_rename_column",
					Operations: migrations.Operations{
						&migrations.OpRenameColumn{
							Table: "users",
							From:  "doesntexist",
							To:    "id",
						},
					},
				},
			},
			wantStartErr: migrations.ColumnDoesNotExistError{Table: "users", Name: "doesntexist"},
		},
		{
			name: "target column must not exist",
			migrations: []migrations.Migration{
				createTableMigration,
				{
					Name: "02_rename_column",
					Operations: migrations.Operations{
						&migrations.OpRenameColumn{
							Table: "users",
							From:  "username",
							To:    "id",
						},
					},
				},
			},
			wantStartErr: migrations.ColumnAlreadyExistsError{Table: "users", Name: "id"},
		},
	})
}
