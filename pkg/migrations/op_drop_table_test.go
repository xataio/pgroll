// SPDX-License-Identifier: Apache-2.0

package migrations_test

import (
	"database/sql"
	"testing"

	"github.com/xataio/pgroll/pkg/migrations"
)

func TestDropTable(t *testing.T) {
	t.Parallel()

	ExecuteTests(t, TestCases{
		{
			name: "drop table",
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
									Name:   "name",
									Type:   "varchar(255)",
									Unique: true,
								},
							},
						},
					},
				},
				{
					Name: "02_drop_table",
					Operations: migrations.Operations{
						&migrations.OpDropTable{
							Name: "users",
						},
					},
				},
			},
			afterStart: func(t *testing.T, db *sql.DB, schema string) {
				// The view for the deleted table does not exist in the new version schema.
				ViewMustNotExist(t, db, schema, "02_drop_table", "users")

				// But the underlying table has not been deleted.
				TableMustExist(t, db, schema, "users")
			},
			afterRollback: func(t *testing.T, db *sql.DB, schema string) {
				// Rollback is a no-op.
			},
			afterComplete: func(t *testing.T, db *sql.DB, schema string) {
				// The underlying table has been deleted.
				TableMustNotExist(t, db, schema, "users")
			},
		},
	})
}

func TestDropTableInMultiOperationMigrations(t *testing.T) {
	t.Parallel()

	ExecuteTests(t, TestCases{
		{
			name: "create table, drop table",
			migrations: []migrations.Migration{
				{
					Name: "01_multi_operation",
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
						&migrations.OpDropTable{
							Name: "items",
						},
					},
				},
			},
			afterStart: func(t *testing.T, db *sql.DB, schema string) {
				// OpDropTable drops tables on migration completion, so the table
				// created by OpCreateTable is present after migration start.
				TableMustExist(t, db, schema, "items")

				// There is no view for the "items" table in the new schema
				ViewMustNotExist(t, db, schema, "01_multi_operation", "items")
			},
			afterRollback: func(t *testing.T, db *sql.DB, schema string) {
				// The table is not present
				TableMustNotExist(t, db, schema, "items")
			},
			afterComplete: func(t *testing.T, db *sql.DB, schema string) {
				// The table is not present
				TableMustNotExist(t, db, schema, "items")

				// There is no view for the "items" table in the new schema
				ViewMustNotExist(t, db, schema, "01_multi_operation", "items")
			},
		},
		{
			name: "create table, rename table, drop table",
			migrations: []migrations.Migration{
				{
					Name: "01_multi_operation",
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
						&migrations.OpRenameTable{
							From: "items",
							To:   "products",
						},
						&migrations.OpDropTable{
							Name: "products",
						},
					},
				},
			},
			afterStart: func(t *testing.T, db *sql.DB, schema string) {
				// OpDropTable drops tables on migration completion, so the table
				// created by OpCreateTable is present after migration start.
				TableMustExist(t, db, schema, "items")

				// There is no view for the "items" table in the new schema
				ViewMustNotExist(t, db, schema, "01_multi_operation", "items")

				// There is no view for the "products" table in the new schema
				ViewMustNotExist(t, db, schema, "01_multi_operation", "products")
			},
			afterRollback: func(t *testing.T, db *sql.DB, schema string) {
				// The table is not present
				TableMustNotExist(t, db, schema, "items")
			},
			afterComplete: func(t *testing.T, db *sql.DB, schema string) {
				// The table is not present
				TableMustNotExist(t, db, schema, "items")

				// There is no view for the "items" table in the new schema
				ViewMustNotExist(t, db, schema, "01_multi_operation", "items")

				// There is no view for the "products" table in the new schema
				ViewMustNotExist(t, db, schema, "01_multi_operation", "products")
			},
		},
	})
}

func TestDropTableValidationInMultiOperationMigrations(t *testing.T) {
	t.Parallel()

	ExecuteTests(t, TestCases{
		{
			name: "drop table, drop table fails to validate",
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
						&migrations.OpDropTable{
							Name: "items",
						},
						&migrations.OpDropTable{
							Name: "items",
						},
					},
				},
			},
			wantStartErr: migrations.TableDoesNotExistError{Name: "items"},
		},
		{
			name: "drop table, rename table fails to validate",
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
						&migrations.OpDropTable{
							Name: "items",
						},
						&migrations.OpRenameTable{
							From: "items",
							To:   "products",
						},
					},
				},
			},
			wantStartErr: migrations.TableDoesNotExistError{Name: "items"},
		},
	})
}
