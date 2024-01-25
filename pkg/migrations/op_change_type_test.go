// SPDX-License-Identifier: Apache-2.0

package migrations_test

import (
	"database/sql"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/xataio/pgroll/pkg/migrations"
	"github.com/xataio/pgroll/pkg/roll"
	"github.com/xataio/pgroll/pkg/testutils"
)

func TestChangeColumnType(t *testing.T) {
	t.Parallel()

	ExecuteTests(t, TestCases{
		{
			name: "change column type",
			migrations: []migrations.Migration{
				{
					Name: "01_add_table",
					Operations: migrations.Operations{
						&migrations.OpCreateTable{
							Name: "reviews",
							Columns: []migrations.Column{
								{
									Name: "id",
									Type: "serial",
									Pk:   true,
								},
								{
									Name: "username",
									Type: "text",
								},
								{
									Name: "product",
									Type: "text",
								},
								{
									Name:    "rating",
									Type:    "text",
									Default: ptr("0"),
								},
							},
						},
					},
				},
				{
					Name: "02_change_type",
					Operations: migrations.Operations{
						&migrations.OpAlterColumn{
							Table:  "reviews",
							Column: "rating",
							Type:   "integer",
							Up:     "CAST (rating AS integer)",
							Down:   "CAST (rating AS text)",
						},
					},
				},
			},
			afterStart: func(t *testing.T, db *sql.DB) {
				newVersionSchema := roll.VersionedSchemaName("public", "02_change_type")

				// The new (temporary) `rating` column should exist on the underlying table.
				ColumnMustExist(t, db, "public", "reviews", migrations.TemporaryName("rating"))

				// The `rating` column in the new view must have the correct type.
				ColumnMustHaveType(t, db, newVersionSchema, "reviews", "rating", "integer")

				// Inserting into the new `rating` column should work.
				MustInsert(t, db, "public", "02_change_type", "reviews", map[string]string{
					"username": "alice",
					"product":  "apple",
					"rating":   "5",
				})

				// The value inserted into the new `rating` column has been backfilled into
				// the old `rating` column.
				rows := MustSelect(t, db, "public", "01_add_table", "reviews")
				assert.Equal(t, []map[string]any{
					{"id": 1, "username": "alice", "product": "apple", "rating": "5"},
				}, rows)

				// Inserting into the old `rating` column should work.
				MustInsert(t, db, "public", "01_add_table", "reviews", map[string]string{
					"username": "bob",
					"product":  "banana",
					"rating":   "8",
				})

				// The value inserted into the old `rating` column has been backfilled into
				// the new `rating` column.
				rows = MustSelect(t, db, "public", "02_change_type", "reviews")
				assert.Equal(t, []map[string]any{
					{"id": 1, "username": "alice", "product": "apple", "rating": 5},
					{"id": 2, "username": "bob", "product": "banana", "rating": 8},
				}, rows)
			},
			afterRollback: func(t *testing.T, db *sql.DB) {
				// The new (temporary) `rating` column should not exist on the underlying table.
				ColumnMustNotExist(t, db, "public", "reviews", migrations.TemporaryName("rating"))

				// The up function no longer exists.
				FunctionMustNotExist(t, db, "public", migrations.TriggerFunctionName("reviews", "rating"))
				// The down function no longer exists.
				FunctionMustNotExist(t, db, "public", migrations.TriggerFunctionName("reviews", migrations.TemporaryName("rating")))

				// The up trigger no longer exists.
				TriggerMustNotExist(t, db, "public", "reviews", migrations.TriggerName("reviews", "rating"))
				// The down trigger no longer exists.
				TriggerMustNotExist(t, db, "public", "reviews", migrations.TriggerName("reviews", migrations.TemporaryName("rating")))
			},
			afterComplete: func(t *testing.T, db *sql.DB) {
				newVersionSchema := roll.VersionedSchemaName("public", "02_change_type")

				// The new (temporary) `rating` column should not exist on the underlying table.
				ColumnMustNotExist(t, db, "public", "reviews", migrations.TemporaryName("rating"))

				// The `rating` column in the new view must have the correct type.
				ColumnMustHaveType(t, db, newVersionSchema, "reviews", "rating", "integer")

				// Inserting into the new view should work.
				MustInsert(t, db, "public", "02_change_type", "reviews", map[string]string{
					"username": "carl",
					"product":  "carrot",
					"rating":   "3",
				})

				// Selecting from the new view should succeed.
				rows := MustSelect(t, db, "public", "02_change_type", "reviews")
				assert.Equal(t, []map[string]any{
					{"id": 1, "username": "alice", "product": "apple", "rating": 5},
					{"id": 2, "username": "bob", "product": "banana", "rating": 8},
					{"id": 3, "username": "carl", "product": "carrot", "rating": 3},
				}, rows)

				// The up function no longer exists.
				FunctionMustNotExist(t, db, "public", migrations.TriggerFunctionName("reviews", "rating"))
				// The down function no longer exists.
				FunctionMustNotExist(t, db, "public", migrations.TriggerFunctionName("reviews", migrations.TemporaryName("rating")))

				// The up trigger no longer exists.
				TriggerMustNotExist(t, db, "public", "reviews", migrations.TriggerName("reviews", "rating"))
				// The down trigger no longer exists.
				TriggerMustNotExist(t, db, "public", "reviews", migrations.TriggerName("reviews", migrations.TemporaryName("rating")))
			},
		},
		{
			name: "changing column type preserves any foreign key constraints on the column",
			migrations: []migrations.Migration{
				{
					Name: "01_add_departments_table",
					Operations: migrations.Operations{
						&migrations.OpCreateTable{
							Name: "departments",
							Columns: []migrations.Column{
								{
									Name: "id",
									Type: "serial",
									Pk:   true,
								},
								{
									Name:     "name",
									Type:     "text",
									Nullable: false,
								},
							},
						},
					},
				},
				{
					Name: "02_add_employees_table",
					Operations: migrations.Operations{
						&migrations.OpCreateTable{
							Name: "employees",
							Columns: []migrations.Column{
								{
									Name: "id",
									Type: "serial",
									Pk:   true,
								},
								{
									Name:     "name",
									Type:     "text",
									Nullable: false,
								},
								{
									Name: "department_id",
									Type: "integer",
									References: &migrations.ForeignKeyReference{
										Name:   "fk_employee_department",
										Table:  "departments",
										Column: "id",
									},
								},
							},
						},
					},
				},
				{
					Name: "03_change_type",
					Operations: migrations.Operations{
						&migrations.OpAlterColumn{
							Table:  "employees",
							Column: "department_id",
							Type:   "bigint",
							Up:     "department_id",
							Down:   "department_id",
						},
					},
				},
			},
			afterStart: func(t *testing.T, db *sql.DB) {
				// A temporary FK constraint has been created on the temporary column
				ValidatedForeignKeyMustExist(t, db, "public", "employees", migrations.DuplicationName("fk_employee_department"))
			},
			afterRollback: func(t *testing.T, db *sql.DB) {
			},
			afterComplete: func(t *testing.T, db *sql.DB) {
				// The foreign key constraint still exists on the column
				ValidatedForeignKeyMustExist(t, db, "public", "employees", "fk_employee_department")
			},
		},
		{
			name: "changing column type preserves any defaults on the column",
			migrations: []migrations.Migration{
				{
					Name: "01_add_table",
					Operations: migrations.Operations{
						&migrations.OpCreateTable{
							Name: "users",
							Columns: []migrations.Column{
								{
									Name: "id",
									Type: "integer",
									Pk:   true,
								},
								{
									Name:     "username",
									Type:     "text",
									Default:  ptr("'alice'"),
									Nullable: true,
								},
							},
						},
					},
				},
				{
					Name: "02_change_type",
					Operations: migrations.Operations{
						&migrations.OpAlterColumn{
							Table:  "users",
							Column: "username",
							Type:   "varchar(255)",
							Up:     "username",
							Down:   "username",
						},
					},
				},
			},
			afterStart: func(t *testing.T, db *sql.DB) {
				// A row can be inserted into the new version of the table.
				MustInsert(t, db, "public", "02_change_type", "users", map[string]string{
					"id": "1",
				})

				// The newly inserted row respects the default value of the column.
				rows := MustSelect(t, db, "public", "02_change_type", "users")
				assert.Equal(t, []map[string]any{
					{"id": 1, "username": "alice"},
				}, rows)
			},
			afterRollback: func(t *testing.T, db *sql.DB) {
			},
			afterComplete: func(t *testing.T, db *sql.DB) {
				// A row can be inserted into the new version of the table.
				MustInsert(t, db, "public", "02_change_type", "users", map[string]string{
					"id": "2",
				})

				// The newly inserted row respects the default value of the column.
				rows := MustSelect(t, db, "public", "02_change_type", "users")
				assert.Equal(t, []map[string]any{
					{"id": 1, "username": "alice"},
					{"id": 2, "username": "alice"},
				}, rows)
			},
		},
		{
			name: "changing column type preserves any check constraints on the column",
			migrations: []migrations.Migration{
				{
					Name: "01_add_table",
					Operations: migrations.Operations{
						&migrations.OpCreateTable{
							Name: "users",
							Columns: []migrations.Column{
								{
									Name: "id",
									Type: "integer",
									Pk:   true,
								},
								{
									Name:     "username",
									Type:     "text",
									Nullable: true,
									Check: &migrations.CheckConstraint{
										Name:       "username_length",
										Constraint: "length(username) > 3",
									},
								},
							},
						},
					},
				},
				{
					Name: "02_change_type",
					Operations: migrations.Operations{
						&migrations.OpAlterColumn{
							Table:  "users",
							Column: "username",
							Type:   "varchar(255)",
							Up:     "username",
							Down:   "username",
						},
					},
				},
			},
			afterStart: func(t *testing.T, db *sql.DB) {
				// Inserting a row that violates the check constraint should fail.
				MustNotInsert(t, db, "public", "02_change_type", "users", map[string]string{
					"id":       "1",
					"username": "a",
				}, testutils.CheckViolationErrorCode)
			},
			afterRollback: func(t *testing.T, db *sql.DB) {
			},
			afterComplete: func(t *testing.T, db *sql.DB) {
				// Inserting a row that violates the check constraint should fail.
				MustNotInsert(t, db, "public", "02_change_type", "users", map[string]string{
					"id":       "2",
					"username": "b",
				}, testutils.CheckViolationErrorCode)
			},
		},
		{
			name: "changing column type preserves column not null",
			migrations: []migrations.Migration{
				{
					Name: "01_add_table",
					Operations: migrations.Operations{
						&migrations.OpCreateTable{
							Name: "users",
							Columns: []migrations.Column{
								{
									Name: "id",
									Type: "integer",
									Pk:   true,
								},
								{
									Name:     "username",
									Type:     "text",
									Nullable: false,
								},
							},
						},
					},
				},
				{
					Name: "02_change_type",
					Operations: migrations.Operations{
						&migrations.OpAlterColumn{
							Table:  "users",
							Column: "username",
							Type:   "varchar(255)",
							Up:     "username",
							Down:   "username",
						},
					},
				},
			},
			afterStart: func(t *testing.T, db *sql.DB) {
				// Inserting a row that violates the NOT NULL constraint fails.
				MustNotInsert(t, db, "public", "02_change_type", "users", map[string]string{
					"id": "1",
				}, testutils.NotNullViolationErrorCode)
			},
			afterRollback: func(t *testing.T, db *sql.DB) {
			},
			afterComplete: func(t *testing.T, db *sql.DB) {
				// Inserting a row that violates the NOT NULL constraint fails.
				MustNotInsert(t, db, "public", "02_change_type", "users", map[string]string{
					"id": "2",
				}, testutils.NotNullViolationErrorCode)
			},
		},
		{
			name: "changing column type preserves any unique constraints on the column",
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
									Name:   "username",
									Type:   "text",
									Unique: true,
								},
							},
						},
					},
				},
				{
					Name: "02_change_type",
					Operations: migrations.Operations{
						&migrations.OpAlterColumn{
							Table:  "users",
							Column: "username",
							Type:   "varchar(255)",
							Up:     "username",
							Down:   "username",
						},
					},
				},
			},
			afterStart: func(t *testing.T, db *sql.DB) {
				// Inserting an initial row succeeds
				MustInsert(t, db, "public", "02_change_type", "users", map[string]string{
					"username": "alice",
				})

				// Inserting a row with a duplicate `username` value fails
				MustNotInsert(t, db, "public", "02_change_type", "users", map[string]string{
					"username": "alice",
				}, testutils.UniqueViolationErrorCode)
			},
			afterRollback: func(t *testing.T, db *sql.DB) {
			},
			afterComplete: func(t *testing.T, db *sql.DB) {
				// Inserting a row with a duplicate `username` value fails
				MustNotInsert(t, db, "public", "02_change_type", "users", map[string]string{
					"username": "alice",
				}, testutils.UniqueViolationErrorCode)

				// Inserting a row with a different `username` value succeeds
				MustInsert(t, db, "public", "02_change_type", "users", map[string]string{
					"username": "bob",
				})
			},
		},
	})
}

func TestChangeColumnTypeValidation(t *testing.T) {
	t.Parallel()

	createTableMigration := migrations.Migration{
		Name: "01_add_table",
		Operations: migrations.Operations{
			&migrations.OpCreateTable{
				Name: "reviews",
				Columns: []migrations.Column{
					{
						Name: "id",
						Type: "serial",
						Pk:   true,
					},
					{
						Name: "username",
						Type: "text",
					},
					{
						Name: "product",
						Type: "text",
					},
					{
						Name: "rating",
						Type: "text",
					},
				},
			},
		},
	}

	ExecuteTests(t, TestCases{
		{
			name: "up SQL is mandatory",
			migrations: []migrations.Migration{
				createTableMigration,
				{
					Name: "02_change_type",
					Operations: migrations.Operations{
						&migrations.OpAlterColumn{
							Table:  "reviews",
							Column: "rating",
							Type:   "integer",
							Down:   "CAST (rating AS text)",
						},
					},
				},
			},
			wantStartErr: migrations.FieldRequiredError{Name: "up"},
		},
		{
			name: "down SQL is mandatory",
			migrations: []migrations.Migration{
				createTableMigration,
				{
					Name: "02_change_type",
					Operations: migrations.Operations{
						&migrations.OpAlterColumn{
							Table:  "reviews",
							Column: "rating",
							Type:   "integer",
							Up:     "CAST (rating AS integer)",
						},
					},
				},
			},
			wantStartErr: migrations.FieldRequiredError{Name: "down"},
		},
	})
}
