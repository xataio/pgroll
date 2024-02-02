// SPDX-License-Identifier: Apache-2.0

package migrations_test

import (
	"database/sql"
	"testing"

	"github.com/xataio/pgroll/pkg/migrations"
	"github.com/xataio/pgroll/pkg/testutils"

	"github.com/stretchr/testify/assert"
)

func TestCreateTable(t *testing.T) {
	t.Parallel()

	ExecuteTests(t, TestCases{
		{
			name: "create table",
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
									Pk:   ptr(true),
								},
								{
									Name:   "name",
									Type:   "varchar(255)",
									Unique: ptr(true),
								},
							},
						},
					},
				},
			},
			afterStart: func(t *testing.T, db *sql.DB) {
				// The new view exists in the new version schema.
				ViewMustExist(t, db, "public", "01_create_table", "users")

				// Data can be inserted into the new view.
				MustInsert(t, db, "public", "01_create_table", "users", map[string]string{
					"name": "Alice",
				})

				// Data can be retrieved from the new view.
				rows := MustSelect(t, db, "public", "01_create_table", "users")
				assert.Equal(t, []map[string]any{
					{"id": 1, "name": "Alice"},
				}, rows)
			},
			afterRollback: func(t *testing.T, db *sql.DB) {
				// The underlying table has been dropped.
				TableMustNotExist(t, db, "public", "users")
			},
			afterComplete: func(t *testing.T, db *sql.DB) {
				// The view still exists
				ViewMustExist(t, db, "public", "01_create_table", "users")

				// Data can be inserted into the new view.
				MustInsert(t, db, "public", "01_create_table", "users", map[string]string{
					"name": "Alice",
				})

				// Data can be retrieved from the new view.
				rows := MustSelect(t, db, "public", "01_create_table", "users")
				assert.Equal(t, []map[string]any{
					{"id": 1, "name": "Alice"},
				}, rows)
			},
		},
		{
			name: "create table with foreign key",
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
									Pk:   ptr(true),
								},
								{
									Name:   "name",
									Type:   "varchar(255)",
									Unique: ptr(true),
								},
							},
						},
					},
				},
				{
					Name: "02_create_table_with_fk",
					Operations: migrations.Operations{
						&migrations.OpCreateTable{
							Name: "orders",
							Columns: []migrations.Column{
								{
									Name: "id",
									Type: "serial",
									Pk:   ptr(true),
								},
								{
									Name: "user_id",
									Type: "integer",
									References: &migrations.ForeignKeyReference{
										Name:   "fk_users_id",
										Table:  "users",
										Column: "id",
									},
								},
								{
									Name: "quantity",
									Type: "integer",
								},
							},
						},
					},
				},
			},
			afterStart: func(t *testing.T, db *sql.DB) {
				// The foreign key constraint exists on the new table.
				ValidatedForeignKeyMustExist(t, db, "public", migrations.TemporaryName("orders"), "fk_users_id")

				// Inserting a row into the referenced table succeeds.
				MustInsert(t, db, "public", "01_create_table", "users", map[string]string{
					"name": "alice",
				})

				// Inserting a row into the referencing table succeeds as the referenced row exists.
				MustInsert(t, db, "public", "02_create_table_with_fk", "orders", map[string]string{
					"user_id":  "1",
					"quantity": "100",
				})

				// Inserting a row into the referencing table fails as the referenced row does not exist.
				MustNotInsert(t, db, "public", "02_create_table_with_fk", "orders", map[string]string{
					"user_id":  "2",
					"quantity": "200",
				}, testutils.FKViolationErrorCode)
			},
			afterRollback: func(t *testing.T, db *sql.DB) {
				// The table has been dropped, so the foreign key constraint is gone.
			},
			afterComplete: func(t *testing.T, db *sql.DB) {
				ValidatedForeignKeyMustExist(t, db, "public", "orders", "fk_users_id")

				// Inserting a row into the referenced table succeeds.
				MustInsert(t, db, "public", "02_create_table_with_fk", "users", map[string]string{
					"name": "bob",
				})

				// Inserting a row into the referencing table succeeds as the referenced row exists.
				MustInsert(t, db, "public", "02_create_table_with_fk", "orders", map[string]string{
					"user_id":  "2",
					"quantity": "200",
				})

				// Inserting a row into the referencing table fails as the referenced row does not exist.
				MustNotInsert(t, db, "public", "02_create_table_with_fk", "orders", map[string]string{
					"user_id":  "3",
					"quantity": "300",
				}, testutils.FKViolationErrorCode)
			},
		},
		{
			name: "create table with a check constraint",
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
									Pk:   ptr(true),
								},
								{
									Name: "name",
									Type: "text",
									Check: &migrations.CheckConstraint{
										Name:       "check_name_length",
										Constraint: "length(name) > 3",
									},
								},
							},
						},
					},
				},
			},
			afterStart: func(t *testing.T, db *sql.DB) {
				// The check constraint exists on the new table.
				CheckConstraintMustExist(t, db, "public", migrations.TemporaryName("users"), "check_name_length")

				// Inserting a row into the table succeeds when the check constraint is satisfied.
				MustInsert(t, db, "public", "01_create_table", "users", map[string]string{
					"name": "alice",
				})

				// Inserting a row into the table fails when the check constraint is not satisfied.
				MustNotInsert(t, db, "public", "01_create_table", "users", map[string]string{
					"name": "b",
				}, testutils.CheckViolationErrorCode)
			},
			afterRollback: func(t *testing.T, db *sql.DB) {
				// The table has been dropped, so the check constraint is gone.
			},
			afterComplete: func(t *testing.T, db *sql.DB) {
				// The check constraint exists on the new table.
				CheckConstraintMustExist(t, db, "public", "users", "check_name_length")

				// Inserting a row into the table succeeds when the check constraint is satisfied.
				MustInsert(t, db, "public", "01_create_table", "users", map[string]string{
					"name": "bobby",
				})

				// Inserting a row into the table fails when the check constraint is not satisfied.
				MustNotInsert(t, db, "public", "01_create_table", "users", map[string]string{
					"name": "c",
				}, testutils.CheckViolationErrorCode)
			},
		},
		{
			name: "create table with column and table comments",
			migrations: []migrations.Migration{
				{
					Name: "01_create_table",
					Operations: migrations.Operations{
						&migrations.OpCreateTable{
							Name:    "users",
							Comment: ptr("the users table"),
							Columns: []migrations.Column{
								{
									Name: "id",
									Type: "serial",
									Pk:   ptr(true),
								},
								{
									Name:    "name",
									Type:    "varchar(255)",
									Unique:  ptr(true),
									Comment: ptr("the username"),
								},
							},
						},
					},
				},
			},
			afterStart: func(t *testing.T, db *sql.DB) {
				tableName := migrations.TemporaryName("users")
				// The comment has been added to the underlying table.
				TableMustHaveComment(t, db, "public", tableName, "the users table")
				// The comment has been added to the underlying column.
				ColumnMustHaveComment(t, db, "public", tableName, "name", "the username")
			},
			afterRollback: func(t *testing.T, db *sql.DB) {
			},
			afterComplete: func(t *testing.T, db *sql.DB) {
				// The comment is still present on the underlying table.
				TableMustHaveComment(t, db, "public", "users", "the users table")
				// The comment is still present on the underlying column.
				ColumnMustHaveComment(t, db, "public", "users", "name", "the username")
			},
		},
	})
}

func TestCreateTableValidation(t *testing.T) {
	t.Parallel()

	ExecuteTests(t, TestCases{TestCase{
		name: "foreign key validity",
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
								Pk:   ptr(true),
							},
							{
								Name:   "name",
								Type:   "varchar(255)",
								Unique: ptr(true),
							},
						},
					},
				},
			},
			{
				Name: "02_create_table_with_fk",
				Operations: migrations.Operations{
					&migrations.OpCreateTable{
						Name: "orders",
						Columns: []migrations.Column{
							{
								Name: "id",
								Type: "serial",
								Pk:   ptr(true),
							},
							{
								Name: "user_id",
								Type: "integer",
								References: &migrations.ForeignKeyReference{
									Name:   "fk_users_doesntexist",
									Table:  "users",
									Column: "doesntexist",
								},
							},
							{
								Name: "quantity",
								Type: "integer",
							},
						},
					},
				},
			},
		},
		wantStartErr: migrations.ColumnReferenceError{
			Table:  "orders",
			Column: "user_id",
			Err:    migrations.ColumnDoesNotExistError{Table: "users", Name: "doesntexist"},
		},
	}})
}
