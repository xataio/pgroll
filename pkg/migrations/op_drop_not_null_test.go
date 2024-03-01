// SPDX-License-Identifier: Apache-2.0

package migrations_test

import (
	"database/sql"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/xataio/pgroll/pkg/migrations"
	"github.com/xataio/pgroll/pkg/testutils"
)

func TestDropNotNull(t *testing.T) {
	t.Parallel()

	ExecuteTests(t, TestCases{
		{
			name: "remove not null with default up sql",
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
									Pk:   ptr(true),
								},
								{
									Name:     "username",
									Type:     "text",
									Nullable: ptr(false),
								},
								{
									Name:     "product",
									Type:     "text",
									Nullable: ptr(false),
								},
								{
									Name:     "review",
									Type:     "text",
									Nullable: ptr(false),
								},
							},
						},
					},
				},
				{
					Name: "02_set_nullable",
					Operations: migrations.Operations{
						&migrations.OpDropNotNull{
							Table:  "reviews",
							Column: "review",
							Down:   "(SELECT CASE WHEN review IS NULL THEN product || ' is good' ELSE review END)",
						},
					},
				},
			},
			afterStart: func(t *testing.T, db *sql.DB, schema string) {
				// The new (temporary) `review` column should exist on the underlying table.
				ColumnMustExist(t, db, schema, "reviews", migrations.TemporaryName("review"))

				// Inserting a NULL into the new `review` column should succeed
				MustInsert(t, db, schema, "02_set_nullable", "reviews", map[string]string{
					"username": "alice",
					"product":  "apple",
				})

				// Inserting a non-NULL value into the new `review` column should succeed
				MustInsert(t, db, schema, "02_set_nullable", "reviews", map[string]string{
					"username": "bob",
					"product":  "banana",
					"review":   "brilliant",
				})

				// The rows inserted into the new `review` column have been backfilled into the
				// old `review` column.
				rows := MustSelect(t, db, schema, "01_add_table", "reviews")
				assert.Equal(t, []map[string]any{
					{"id": 1, "username": "alice", "product": "apple", "review": "apple is good"},
					{"id": 2, "username": "bob", "product": "banana", "review": "brilliant"},
				}, rows)

				// Inserting a NULL value into the old `review` column should fail
				MustNotInsert(t, db, schema, "01_add_table", "reviews", map[string]string{
					"username": "carl",
					"product":  "carrot",
				}, testutils.NotNullViolationErrorCode)

				// Inserting a non-NULL value into the old `review` column should succeed
				MustInsert(t, db, schema, "01_add_table", "reviews", map[string]string{
					"username": "dana",
					"product":  "durian",
					"review":   "delicious",
				})

				// The non-NULL value inserted into the old `review` column has been copied
				// unchanged into the new `review` column.
				rows = MustSelect(t, db, schema, "02_set_nullable", "reviews")
				assert.Equal(t, []map[string]any{
					{"id": 1, "username": "alice", "product": "apple", "review": nil},
					{"id": 2, "username": "bob", "product": "banana", "review": "brilliant"},
					{"id": 4, "username": "dana", "product": "durian", "review": "delicious"},
				}, rows)
			},
			afterRollback: func(t *testing.T, db *sql.DB, schema string) {
				// The new (temporary) `review` column should not exist on the underlying table.
				ColumnMustNotExist(t, db, schema, "reviews", migrations.TemporaryName("review"))

				// The up function no longer exists.
				FunctionMustNotExist(t, db, schema, migrations.TriggerFunctionName("reviews", "review"))
				// The down function no longer exists.
				FunctionMustNotExist(t, db, schema, migrations.TriggerFunctionName("reviews", migrations.TemporaryName("review")))

				// The up trigger no longer exists.
				TriggerMustNotExist(t, db, schema, "reviews", migrations.TriggerName("reviews", "review"))
				// The down trigger no longer exists.
				TriggerMustNotExist(t, db, schema, "reviews", migrations.TriggerName("reviews", migrations.TemporaryName("review")))
			},
			afterComplete: func(t *testing.T, db *sql.DB, schema string) {
				// The new (temporary) `review` column should not exist on the underlying table.
				ColumnMustNotExist(t, db, schema, "reviews", migrations.TemporaryName("review"))

				// Writing a NULL review into the `review` column should succeed.
				MustInsert(t, db, schema, "02_set_nullable", "reviews", map[string]string{
					"username": "earl",
					"product":  "eggplant",
				})

				// Selecting from the `reviews` view should succeed.
				rows := MustSelect(t, db, schema, "02_set_nullable", "reviews")
				assert.Equal(t, []map[string]any{
					{"id": 1, "username": "alice", "product": "apple", "review": "apple is good"},
					{"id": 2, "username": "bob", "product": "banana", "review": "brilliant"},
					{"id": 4, "username": "dana", "product": "durian", "review": "delicious"},
					{"id": 5, "username": "earl", "product": "eggplant", "review": nil},
				}, rows)

				// The up function no longer exists.
				FunctionMustNotExist(t, db, schema, migrations.TriggerFunctionName("reviews", "review"))
				// The down function no longer exists.
				FunctionMustNotExist(t, db, schema, migrations.TriggerFunctionName("reviews", migrations.TemporaryName("review")))

				// The up trigger no longer exists.
				TriggerMustNotExist(t, db, schema, "reviews", migrations.TriggerName("reviews", "review"))
				// The down trigger no longer exists.
				TriggerMustNotExist(t, db, schema, "reviews", migrations.TriggerName("reviews", migrations.TemporaryName("review")))
			},
		},
		{
			name: "remove not null with user-supplied up sql",
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
									Pk:   ptr(true),
								},
								{
									Name:     "username",
									Type:     "text",
									Nullable: ptr(false),
								},
								{
									Name:     "product",
									Type:     "text",
									Nullable: ptr(false),
								},
								{
									Name:     "review",
									Type:     "text",
									Nullable: ptr(false),
								},
							},
						},
					},
				},
				{
					Name: "02_set_nullable",
					Operations: migrations.Operations{
						&migrations.OpDropNotNull{
							Table:  "reviews",
							Column: "review",
							Down:   "(SELECT CASE WHEN review IS NULL THEN product || ' is good' ELSE review END)",
							Up:     "review || ' (from the old column)'",
						},
					},
				},
			},
			afterStart: func(t *testing.T, db *sql.DB, schema string) {
				// Inserting a non-NULL value into the old `review` column should succeed
				MustInsert(t, db, schema, "01_add_table", "reviews", map[string]string{
					"username": "alice",
					"product":  "apple",
					"review":   "amazing",
				})

				// The value inserted into the old `review` column has been backfilled into the
				// new `review` column using the user-supplied `up` SQL.
				rows := MustSelect(t, db, schema, "02_set_nullable", "reviews")
				assert.Equal(t, []map[string]any{
					{"id": 1, "username": "alice", "product": "apple", "review": "amazing (from the old column)"},
				}, rows)
			},
			afterRollback: func(t *testing.T, db *sql.DB, schema string) {
			},
			afterComplete: func(t *testing.T, db *sql.DB, schema string) {
			},
		},
		{
			name: "dropping not null from a foreign key column retains the foreign key constraint",
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
									Pk:   ptr(true),
								},
								{
									Name:     "name",
									Type:     "text",
									Nullable: ptr(false),
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
									Pk:   ptr(true),
								},
								{
									Name:     "name",
									Type:     "text",
									Nullable: ptr(false),
								},
								{
									Name:     "department_id",
									Type:     "integer",
									Nullable: ptr(false),
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
					Name: "03_set_nullable",
					Operations: migrations.Operations{
						&migrations.OpDropNotNull{
							Table:  "employees",
							Column: "department_id",
							Down:   "(SELECT CASE WHEN department_id IS NULL THEN 1 ELSE department_id END)",
							Up:     "department_id",
						},
					},
				},
			},
			afterStart: func(t *testing.T, db *sql.DB, schema string) {
				// A temporary FK constraint has been created on the temporary column
				ValidatedForeignKeyMustExist(t, db, schema, "employees", migrations.DuplicationName("fk_employee_department"))
			},
			afterRollback: func(t *testing.T, db *sql.DB, schema string) {
			},
			afterComplete: func(t *testing.T, db *sql.DB, schema string) {
				// The foreign key constraint still exists on the column
				ValidatedForeignKeyMustExist(t, db, schema, "employees", "fk_employee_department")
			},
		},
		{
			name: "dropping not null retains any default defined on the column",
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
									Pk:   ptr(true),
								},
								{
									Name:     "name",
									Type:     "text",
									Nullable: ptr(false),
									Default:  ptr("'anonymous'"),
								},
							},
						},
					},
				},
				{
					Name: "02_set_nullable",
					Operations: migrations.Operations{
						&migrations.OpDropNotNull{
							Table:  "users",
							Column: "name",
							Up:     "name",
							Down:   "(SELECT CASE WHEN name IS NULL THEN 'anonymous' ELSE name END)",
						},
					},
				},
			},
			afterStart: func(t *testing.T, db *sql.DB, schema string) {
				// A row can be inserted into the new version of the table.
				MustInsert(t, db, schema, "02_set_nullable", "users", map[string]string{
					"id": "1",
				})

				// The newly inserted row respects the default value of the column.
				rows := MustSelect(t, db, schema, "02_set_nullable", "users")
				assert.Equal(t, []map[string]any{
					{"id": 1, "name": "anonymous"},
				}, rows)
			},
			afterRollback: func(t *testing.T, db *sql.DB, schema string) {
			},
			afterComplete: func(t *testing.T, db *sql.DB, schema string) {
				// A row can be inserted into the new version of the table.
				MustInsert(t, db, schema, "02_set_nullable", "users", map[string]string{
					"id": "2",
				})

				// The newly inserted row respects the default value of the column.
				rows := MustSelect(t, db, schema, "02_set_nullable", "users")
				assert.Equal(t, []map[string]any{
					{"id": 1, "name": "anonymous"},
					{"id": 2, "name": "anonymous"},
				}, rows)
			},
		},
		{
			name: "dropping not null retains any check constraints defined on the column",
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
									Pk:   ptr(true),
								},
								{
									Name:     "name",
									Type:     "text",
									Nullable: ptr(false),
									Check: &migrations.CheckConstraint{
										Name:       "name_length",
										Constraint: "length(name) > 3",
									},
								},
							},
						},
					},
				},
				{
					Name: "02_set_nullable",
					Operations: migrations.Operations{
						&migrations.OpDropNotNull{
							Table:  "users",
							Column: "name",
							Up:     "name",
							Down:   "(SELECT CASE WHEN name IS NULL THEN 'anonymous' ELSE name END)",
						},
					},
				},
			},
			afterStart: func(t *testing.T, db *sql.DB, schema string) {
				// Inserting a row that violates the check constraint should fail.
				MustNotInsert(t, db, schema, "02_set_nullable", "users", map[string]string{
					"id":   "1",
					"name": "a",
				}, testutils.CheckViolationErrorCode)
			},
			afterRollback: func(t *testing.T, db *sql.DB, schema string) {
			},
			afterComplete: func(t *testing.T, db *sql.DB, schema string) {
				// Inserting a row that violates the check constraint should fail.
				MustNotInsert(t, db, schema, "02_set_nullable", "users", map[string]string{
					"id":   "2",
					"name": "b",
				}, testutils.CheckViolationErrorCode)
			},
		},
		{
			name: "dropping not null retains any unique constraints defined on the column",
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
									Pk:   ptr(true),
								},
								{
									Name:     "name",
									Type:     "text",
									Nullable: ptr(false),
								},
							},
						},
					},
				},
				{
					Name: "02_set_unique",
					Operations: migrations.Operations{
						&migrations.OpSetUnique{
							Table:  "users",
							Column: "name",
							Name:   "unique_name",
							Up:     "name",
							Down:   "name",
						},
					},
				},
				{
					Name: "03_set_nullable",
					Operations: migrations.Operations{
						&migrations.OpDropNotNull{
							Table:  "users",
							Column: "name",
							Up:     "name",
							Down:   "(SELECT CASE WHEN name IS NULL THEN 'anonymous' ELSE name END)",
						},
					},
				},
			},
			afterStart: func(t *testing.T, db *sql.DB, schema string) {
				// Inserting an initial row succeeds
				MustInsert(t, db, schema, "03_set_nullable", "users", map[string]string{
					"name": "alice",
				})

				// Inserting a row with a duplicate `name` value fails
				MustNotInsert(t, db, schema, "03_set_nullable", "users", map[string]string{
					"name": "alice",
				}, testutils.UniqueViolationErrorCode)
			},
			afterRollback: func(t *testing.T, db *sql.DB, schema string) {
			},
			afterComplete: func(t *testing.T, db *sql.DB, schema string) {
				// The table has a unique constraint defined on it
				UniqueConstraintMustExist(t, db, schema, "users", "unique_name")

				// Inserting a row with a duplicate `name` value fails
				MustNotInsert(t, db, schema, "03_set_nullable", "users", map[string]string{
					"name": "alice",
				}, testutils.UniqueViolationErrorCode)

				// Inserting a row with a different `name` value succeeds
				MustInsert(t, db, schema, "03_set_nullable", "users", map[string]string{
					"name": "bob",
				})
			},
		},
	})
}

func TestDropNotNullValidation(t *testing.T) {
	t.Parallel()

	ExecuteTests(t, TestCases{
		{
			name: "down SQL is mandatory",
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
									Pk:   ptr(true),
								},
								{
									Name:     "name",
									Type:     "text",
									Nullable: ptr(false),
								},
							},
						},
					},
				},
				{
					Name: "02_set_nullable",
					Operations: migrations.Operations{
						&migrations.OpDropNotNull{
							Table:  "users",
							Column: "name",
							Up:     "name",
						},
					},
				},
			},
			wantStartErr: migrations.FieldRequiredError{Name: "down"},
		},
		{
			name: "can't remove not null from a column that is already nullable",
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
									Pk:   ptr(true),
								},
								{
									Name:     "name",
									Type:     "text",
									Nullable: ptr(true),
								},
							},
						},
					},
				},
				{
					Name: "02_set_nullable",
					Operations: migrations.Operations{
						&migrations.OpDropNotNull{
							Table:  "users",
							Column: "name",
							Up:     "name",
							Down:   "(SELECT CASE WHEN name IS NULL THEN 'placeholder' ELSE name END)",
						},
					},
				},
			},
			wantStartErr: migrations.ColumnIsNullableError{Table: "users", Name: "name"},
		},
	})
}
