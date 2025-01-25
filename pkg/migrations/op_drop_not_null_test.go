// SPDX-License-Identifier: Apache-2.0

package migrations_test

import (
	"database/sql"
	"testing"

	"github.com/xataio/pgroll/internal/testutils"

	"github.com/stretchr/testify/assert"

	"github.com/xataio/pgroll/pkg/migrations"
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
									Pk:   true,
								},
								{
									Name:     "username",
									Type:     "text",
									Nullable: false,
								},
								{
									Name:     "product",
									Type:     "text",
									Nullable: false,
								},
								{
									Name:     "review",
									Type:     "text",
									Nullable: false,
								},
							},
						},
					},
				},
				{
					Name: "02_set_nullable",
					Operations: migrations.Operations{
						&migrations.OpAlterColumn{
							Table:    "reviews",
							Column:   "review",
							Nullable: ptr(true),
							Down:     "SELECT CASE WHEN review IS NULL THEN product || ' is good' ELSE review END",
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
				// The table is cleaned up; temporary columns, trigger functions and triggers no longer exist.
				TableMustBeCleanedUp(t, db, schema, "reviews", "review")
			},
			afterComplete: func(t *testing.T, db *sql.DB, schema string) {
				// The table is cleaned up; temporary columns, trigger functions and triggers no longer exist.
				TableMustBeCleanedUp(t, db, schema, "reviews", "review")

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
									Pk:   true,
								},
								{
									Name:     "username",
									Type:     "text",
									Nullable: false,
								},
								{
									Name:     "product",
									Type:     "text",
									Nullable: false,
								},
								{
									Name:     "review",
									Type:     "text",
									Nullable: false,
								},
							},
						},
					},
				},
				{
					Name: "02_set_nullable",
					Operations: migrations.Operations{
						&migrations.OpAlterColumn{
							Table:    "reviews",
							Column:   "review",
							Nullable: ptr(true),
							Down:     "SELECT CASE WHEN review IS NULL THEN product || ' is good' ELSE review END",
							Up:       "review || ' (from the old column)'",
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
									Name:     "department_id",
									Type:     "integer",
									Nullable: false,
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
					Name: "03_set_not_null",
					Operations: migrations.Operations{
						&migrations.OpAlterColumn{
							Table:    "employees",
							Column:   "department_id",
							Nullable: ptr(true),
							Down:     "SELECT CASE WHEN department_id IS NULL THEN 1 ELSE department_id END",
							Up:       "department_id",
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
									Pk:   true,
								},
								{
									Name:     "name",
									Type:     "text",
									Nullable: false,
									Default:  ptr("'anonymous'"),
								},
							},
						},
					},
				},
				{
					Name: "02_set_not_null",
					Operations: migrations.Operations{
						&migrations.OpAlterColumn{
							Table:    "users",
							Column:   "name",
							Nullable: ptr(true),
							Up:       "name",
							Down:     "SELECT CASE WHEN name IS NULL THEN 'anonymous' ELSE name END",
						},
					},
				},
			},
			afterStart: func(t *testing.T, db *sql.DB, schema string) {
				// A row can be inserted into the new version of the table.
				MustInsert(t, db, schema, "02_set_not_null", "users", map[string]string{
					"id": "1",
				})

				// The newly inserted row respects the default value of the column.
				rows := MustSelect(t, db, schema, "02_set_not_null", "users")
				assert.Equal(t, []map[string]any{
					{"id": 1, "name": "anonymous"},
				}, rows)
			},
			afterRollback: func(t *testing.T, db *sql.DB, schema string) {
			},
			afterComplete: func(t *testing.T, db *sql.DB, schema string) {
				// A row can be inserted into the new version of the table.
				MustInsert(t, db, schema, "02_set_not_null", "users", map[string]string{
					"id": "2",
				})

				// The newly inserted row respects the default value of the column.
				rows := MustSelect(t, db, schema, "02_set_not_null", "users")
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
									Pk:   true,
								},
								{
									Name:     "name",
									Type:     "text",
									Nullable: false,
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
					Name: "02_set_not_null",
					Operations: migrations.Operations{
						&migrations.OpAlterColumn{
							Table:    "users",
							Column:   "name",
							Nullable: ptr(true),
							Up:       "name",
							Down:     "SELECT CASE WHEN name IS NULL THEN 'anonymous' ELSE name END",
						},
					},
				},
			},
			afterStart: func(t *testing.T, db *sql.DB, schema string) {
				// Inserting a row that violates the check constraint should fail.
				MustNotInsert(t, db, schema, "02_set_not_null", "users", map[string]string{
					"id":   "1",
					"name": "a",
				}, testutils.CheckViolationErrorCode)
			},
			afterRollback: func(t *testing.T, db *sql.DB, schema string) {
			},
			afterComplete: func(t *testing.T, db *sql.DB, schema string) {
				// Inserting a row that violates the check constraint should fail.
				MustNotInsert(t, db, schema, "02_set_not_null", "users", map[string]string{
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
					Name: "02_set_unique",
					Operations: migrations.Operations{
						&migrations.OpAlterColumn{
							Table:  "users",
							Column: "name",
							Unique: &migrations.UniqueConstraint{Name: "unique_name"},
							Up:     "name",
							Down:   "name",
						},
					},
				},
				{
					Name: "03_set_not_null",
					Operations: migrations.Operations{
						&migrations.OpAlterColumn{
							Table:    "users",
							Column:   "name",
							Nullable: ptr(true),
							Up:       "name",
							Down:     "SELECT CASE WHEN name IS NULL THEN 'anonymous' ELSE name END",
						},
					},
				},
			},
			afterStart: func(t *testing.T, db *sql.DB, schema string) {
				// Inserting an initial row succeeds
				MustInsert(t, db, schema, "03_set_not_null", "users", map[string]string{
					"name": "alice",
				})

				// Inserting a row with a duplicate `name` value fails
				MustNotInsert(t, db, schema, "03_set_not_null", "users", map[string]string{
					"name": "alice",
				}, testutils.UniqueViolationErrorCode)
			},
			afterRollback: func(t *testing.T, db *sql.DB, schema string) {
			},
			afterComplete: func(t *testing.T, db *sql.DB, schema string) {
				// The table has a unique constraint defined on it
				UniqueConstraintMustExist(t, db, schema, "users", "unique_name")

				// Inserting a row with a duplicate `name` value fails
				MustNotInsert(t, db, schema, "03_set_not_null", "users", map[string]string{
					"name": "alice",
				}, testutils.UniqueViolationErrorCode)

				// Inserting a row with a different `name` value succeeds
				MustInsert(t, db, schema, "03_set_not_null", "users", map[string]string{
					"name": "bob",
				})
			},
		},
		{
			name: "dropping not null retains any comments defined on the column",
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
									Type:     "text",
									Nullable: false,
									Comment:  ptr("the name of the user"),
								},
							},
						},
					},
				},
				{
					Name: "02_set_not_null",
					Operations: migrations.Operations{
						&migrations.OpAlterColumn{
							Table:    "users",
							Column:   "name",
							Nullable: ptr(true),
							Up:       "name",
							Down:     "SELECT CASE WHEN name IS NULL THEN 'anonymous' ELSE name END",
						},
					},
				},
			},
			afterStart: func(t *testing.T, db *sql.DB, schema string) {
				// The duplicated column has a comment defined on it
				ColumnMustHaveComment(t, db, schema, "users", migrations.TemporaryName("name"), "the name of the user")
			},
			afterRollback: func(t *testing.T, db *sql.DB, schema string) {
			},
			afterComplete: func(t *testing.T, db *sql.DB, schema string) {
				// The final column has a comment defined on it
				ColumnMustHaveComment(t, db, schema, "users", "name", "the name of the user")
			},
		},
	})
}

func TestDropNotNullInMultiOperationMigrations(t *testing.T) {
	t.Parallel()

	ExecuteTests(t, TestCases{
		{
			name: "rename table, drop not null",
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
						&migrations.OpAlterColumn{
							Table:    "products",
							Column:   "name",
							Nullable: ptr(true),
							Down:     "SELECT CASE WHEN name IS NULL THEN 'unknown' ELSE name END",
							Up:       "name",
						},
					},
				},
			},
			afterStart: func(t *testing.T, db *sql.DB, schema string) {
				// Can insert a row into the new schema with a NULL name
				MustInsert(t, db, schema, "02_multi_operation", "products", map[string]string{
					"id": "1",
				})

				// Can insert a row into the new schema with a non-NULL name
				MustInsert(t, db, schema, "02_multi_operation", "products", map[string]string{
					"id":   "2",
					"name": "apple",
				})

				// Can't insert a row into the old schema with a NULL name
				MustNotInsert(t, db, schema, "01_create_table", "items", map[string]string{
					"id": "3",
				}, testutils.NotNullViolationErrorCode)

				// The new view has the expected rows
				rows := MustSelect(t, db, schema, "02_multi_operation", "products")
				assert.Equal(t, []map[string]any{
					{"id": 1, "name": nil},
					{"id": 2, "name": "apple"},
				}, rows)

				// The old view has the expected rows
				rows = MustSelect(t, db, schema, "01_create_table", "items")
				assert.Equal(t, []map[string]any{
					{"id": 1, "name": "unknown"},
					{"id": 2, "name": "apple"},
				}, rows)
			},
			afterRollback: func(t *testing.T, db *sql.DB, schema string) {
				// The table has been cleaned up
				TableMustBeCleanedUp(t, db, schema, "items", "name")
			},
			afterComplete: func(t *testing.T, db *sql.DB, schema string) {
				// Can insert a row into the new schema with a NULL name
				MustInsert(t, db, schema, "02_multi_operation", "products", map[string]string{
					"id": "3",
				})

				// Can't insert a row into the new schema with a non-NULL name
				MustInsert(t, db, schema, "02_multi_operation", "products", map[string]string{
					"id":   "4",
					"name": "banana",
				})

				// The new view has the expected rows
				rows := MustSelect(t, db, schema, "02_multi_operation", "products")
				assert.Equal(t, []map[string]any{
					{"id": 1, "name": "unknown"},
					{"id": 2, "name": "apple"},
					{"id": 3, "name": nil},
					{"id": 4, "name": "banana"},
				}, rows)

				// The table has been cleaned up
				TableMustBeCleanedUp(t, db, schema, "products", "name")
			},
		},
		{
			name: "rename table, rename column, drop not null",
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
						&migrations.OpAlterColumn{
							Table:    "products",
							Column:   "item_name",
							Nullable: ptr(true),
							Down:     "SELECT CASE WHEN item_name IS NULL THEN 'unknown' ELSE item_name END",
							Up:       "item_name",
						},
					},
				},
			},
			afterStart: func(t *testing.T, db *sql.DB, schema string) {
				// Can insert a row via the new schema with a NULL item_name
				MustInsert(t, db, schema, "02_multi_operation", "products", map[string]string{
					"id": "1",
				})

				// Can insert a row via the new schema with a non-NULL item_name
				MustInsert(t, db, schema, "02_multi_operation", "products", map[string]string{
					"id":        "2",
					"item_name": "apple",
				})

				// Can't insert a row with a NULL name via the old schema
				MustNotInsert(t, db, schema, "01_create_table", "items", map[string]string{
					"id": "2",
				}, testutils.NotNullViolationErrorCode)

				// The new view has the expected rows
				rows := MustSelect(t, db, schema, "02_multi_operation", "products")
				assert.Equal(t, []map[string]any{
					{"id": 1, "item_name": nil},
					{"id": 2, "item_name": "apple"},
				}, rows)

				// The old view has the expected rows
				rows = MustSelect(t, db, schema, "01_create_table", "items")
				assert.Equal(t, []map[string]any{
					{"id": 1, "name": "unknown"},
					{"id": 2, "name": "apple"},
				}, rows)
			},
			afterRollback: func(t *testing.T, db *sql.DB, schema string) {
				// The table has been cleaned up
				TableMustBeCleanedUp(t, db, schema, "items", "name")
				TableMustBeCleanedUp(t, db, schema, "items", "item_name")
			},
			afterComplete: func(t *testing.T, db *sql.DB, schema string) {
				// Can insert a row via the new schema with a NULL item_name
				MustInsert(t, db, schema, "02_multi_operation", "products", map[string]string{
					"id": "3",
				})

				// Can insert a row via the new schema with a non-NULL item_name
				MustInsert(t, db, schema, "02_multi_operation", "products", map[string]string{
					"id":        "4",
					"item_name": "banana",
				})

				// The new view has the expected rows
				rows := MustSelect(t, db, schema, "02_multi_operation", "products")
				assert.Equal(t, []map[string]any{
					{"id": 1, "item_name": "unknown"},
					{"id": 2, "item_name": "apple"},
					{"id": 3, "item_name": nil},
					{"id": 4, "item_name": "banana"},
				}, rows)

				// The table has been cleaned up
				TableMustBeCleanedUp(t, db, schema, "products", "name")
				TableMustBeCleanedUp(t, db, schema, "products", "item_name")
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
					Name: "02_set_nullable",
					Operations: migrations.Operations{
						&migrations.OpAlterColumn{
							Table:    "users",
							Column:   "name",
							Nullable: ptr(true),
							Up:       "name",
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
									Pk:   true,
								},
								{
									Name:     "name",
									Type:     "text",
									Nullable: true,
								},
							},
						},
					},
				},
				{
					Name: "02_set_nullable",
					Operations: migrations.Operations{
						&migrations.OpAlterColumn{
							Table:    "users",
							Column:   "name",
							Nullable: ptr(true),
							Up:       "name",
							Down:     "SELECT CASE WHEN name IS NULL THEN 'placeholder' ELSE name END",
						},
					},
				},
			},
			wantStartErr: migrations.ColumnIsNullableError{Table: "users", Name: "name"},
		},
	})
}
