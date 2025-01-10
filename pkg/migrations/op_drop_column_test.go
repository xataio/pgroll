// SPDX-License-Identifier: Apache-2.0

package migrations_test

import (
	"database/sql"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/xataio/pgroll/internal/testutils"
	"github.com/xataio/pgroll/pkg/migrations"
	"github.com/xataio/pgroll/pkg/roll"
)

func TestDropColumnWithDownSQL(t *testing.T) {
	t.Parallel()

	ExecuteTests(t, TestCases{
		{
			name: "drop column",
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
					Name: "02_drop_column",
					Operations: migrations.Operations{
						&migrations.OpDropColumn{
							Table:  "users",
							Column: "name",
							Down:   "UPPER(email)",
						},
					},
				},
			},
			afterStart: func(t *testing.T, db *sql.DB, schema string) {
				// The deleted column is not present on the view in the new version schema.
				versionSchema := roll.VersionedSchemaName(schema, "02_drop_column")
				ColumnMustNotExist(t, db, versionSchema, "users", "name")

				// But the column is still present on the underlying table.
				ColumnMustExist(t, db, schema, "users", "name")

				// Inserting into the view in the new version schema should succeed.
				MustInsert(t, db, schema, "02_drop_column", "users", map[string]string{
					"email": "foo@example.com",
				})

				// The "down" SQL has populated the removed column ("name")
				results := MustSelect(t, db, schema, "01_add_table", "users")
				assert.Equal(t, []map[string]any{
					{"id": 1, "name": "FOO@EXAMPLE.COM", "email": "foo@example.com"},
				}, results)
			},
			afterRollback: func(t *testing.T, db *sql.DB, schema string) {
				// The trigger function has been dropped.
				triggerFnName := migrations.TriggerFunctionName("users", "name")
				FunctionMustNotExist(t, db, schema, triggerFnName)

				// The trigger has been dropped.
				triggerName := migrations.TriggerName("users", "name")
				TriggerMustNotExist(t, db, schema, "users", triggerName)
			},
			afterComplete: func(t *testing.T, db *sql.DB, schema string) {
				// The column has been deleted from the underlying table.
				ColumnMustNotExist(t, db, schema, "users", "name")

				// The trigger function has been dropped.
				triggerFnName := migrations.TriggerFunctionName("users", "name")
				FunctionMustNotExist(t, db, schema, triggerFnName)

				// The trigger has been dropped.
				triggerName := migrations.TriggerName("users", "name")
				TriggerMustNotExist(t, db, schema, "users", triggerName)

				// Inserting into the view in the new version schema should succeed.
				MustInsert(t, db, schema, "02_drop_column", "users", map[string]string{
					"email": "bar@example.com",
				})
				results := MustSelect(t, db, schema, "02_drop_column", "users")
				assert.Equal(t, []map[string]any{
					{"id": 1, "email": "foo@example.com"},
					{"id": 2, "email": "bar@example.com"},
				}, results)
			},
		},
		{
			name: "can drop a column with a reserved word as its name",
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
									Name: "array",
									Type: "int[]",
								},
							},
						},
					},
				},
				{
					Name: "02_drop_column",
					Operations: migrations.Operations{
						&migrations.OpDropColumn{
							Table:  "users",
							Column: "array",
							Down:   "UPPER(email)",
						},
					},
				},
			},
			afterStart: func(t *testing.T, db *sql.DB, schema string) {
			},
			afterRollback: func(t *testing.T, db *sql.DB, schema string) {
			},
			afterComplete: func(t *testing.T, db *sql.DB, schema string) {
				// The column has been deleted from the underlying table.
				ColumnMustNotExist(t, db, schema, "users", "array")
			},
		},
		{
			name: "can drop a column in a table with a reserved word as its name",
			migrations: []migrations.Migration{
				{
					Name: "01_add_table",
					Operations: migrations.Operations{
						&migrations.OpCreateTable{
							Name: "array",
							Columns: []migrations.Column{
								{
									Name: "id",
									Type: "serial",
									Pk:   true,
								},
								{
									Name: "name",
									Type: "text",
								},
							},
						},
					},
				},
				{
					Name: "02_drop_column",
					Operations: migrations.Operations{
						&migrations.OpDropColumn{
							Table:  "array",
							Column: "name",
							Down:   "UPPER(email)",
						},
					},
				},
			},
			afterStart: func(t *testing.T, db *sql.DB, schema string) {
			},
			afterRollback: func(t *testing.T, db *sql.DB, schema string) {
			},
			afterComplete: func(t *testing.T, db *sql.DB, schema string) {
				// The column has been deleted from the underlying table.
				ColumnMustNotExist(t, db, schema, "users", "array")
			},
		},
	})
}

func TestDropColumnInMultiOperationMigrations(t *testing.T) {
	t.Parallel()

	ExecuteTests(t, TestCases{
		{
			name: "rename table, drop column",
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
								{
									Name: "description",
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
						&migrations.OpDropColumn{
							Table:  "products",
							Column: "description",
							Down:   "'foo'",
						},
					},
				},
			},
			afterStart: func(t *testing.T, db *sql.DB, schema string) {
				// Can insert using the new table name in the new schema if the dropped
				// column is not specified
				MustInsert(t, db, schema, "02_multi_operation", "products", map[string]string{
					"name": "apples",
				})

				// Can't insert using the new table name in the new schema if the
				// dropped column is specified
				MustNotInsert(t, db, schema, "02_multi_operation", "products", map[string]string{
					"name":        "apples",
					"description": "green",
				}, testutils.UndefinedColumnErrorCode)

				// Can't insert into the dropped column in the new schema using the new table name
				MustNotInsert(t, db, schema, "02_multi_operation", "products", map[string]string{
					"name":        "bananas",
					"description": "yellow",
				}, testutils.UndefinedColumnErrorCode)

				// Can't insert into the dropped column in the new schema using the old table name
				MustNotInsert(t, db, schema, "02_multi_operation", "items", map[string]string{
					"name":        "bananas",
					"description": "yellow",
				}, testutils.UndefinedTableErrorCode)

				// The table has the expected rows in the old schema
				rows := MustSelect(t, db, schema, "01_create_table", "items")
				assert.Equal(t, []map[string]any{
					{"id": 1, "name": "apples", "description": "foo"},
				}, rows)

				// The table has the expected rows in the new schema
				rows = MustSelect(t, db, schema, "02_multi_operation", "products")
				assert.Equal(t, []map[string]any{
					{"id": 1, "name": "apples"},
				}, rows)
			},
			afterRollback: func(t *testing.T, db *sql.DB, schema string) {
				// Can insert into the dropped column in the old schema using the old table name
				MustInsert(t, db, schema, "01_create_table", "items", map[string]string{
					"name":        "bananas",
					"description": "yellow",
				})

				// The down trigger has been removed from the underlying table
				TableMustBeCleanedUp(t, db, schema, "items", "description")
			},
			afterComplete: func(t *testing.T, db *sql.DB, schema string) {
				// The underlying table has been renamed
				TableMustExist(t, db, schema, "products")

				// Can insert into the table in the new schema if the dropped column is not specified
				MustInsert(t, db, schema, "02_multi_operation", "products", map[string]string{
					"name": "carrots",
				})

				// The table has the new name in the new schema and has the expected
				// rows.
				rows := MustSelect(t, db, schema, "02_multi_operation", "products")
				assert.Equal(t, []map[string]any{
					{"id": 1, "name": "apples"},
					{"id": 2, "name": "bananas"},
					{"id": 3, "name": "carrots"},
				}, rows)

				// The down trigger has been removed from the underlying table
				TableMustBeCleanedUp(t, db, schema, "products", "description")
			},
		},
	})
}
