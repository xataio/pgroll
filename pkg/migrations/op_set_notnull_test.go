// SPDX-License-Identifier: Apache-2.0

package migrations_test

import (
	"database/sql"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/xataio/pgroll/pkg/migrations"

	"github.com/xataio/pgroll/internal/testutils"
)

func TestSetNotNull(t *testing.T) {
	t.Parallel()

	ExecuteTests(t, TestCases{
		{
			name: "set not null with default down sql",
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
									Nullable: ptr(true),
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
							Nullable: ptr(false),
							Up:       "SELECT CASE WHEN review IS NULL THEN product || ' is good' ELSE review END",
						},
					},
				},
			},
			afterStart: func(t *testing.T, db *sql.DB, schema string) {
				// The new (temporary) `review` column should exist on the underlying table.
				ColumnMustExist(t, db, schema, "reviews", migrations.TemporaryName("review"))

				// Inserting a NULL into the new `review` column should fail
				MustNotInsert(t, db, schema, "02_set_nullable", "reviews", map[string]string{
					"username": "alice",
					"product":  "apple",
				}, testutils.CheckViolationErrorCode)

				// Inserting a non-NULL value into the new `review` column should succeed
				MustInsert(t, db, schema, "02_set_nullable", "reviews", map[string]string{
					"username": "alice",
					"product":  "apple",
					"review":   "amazing",
				})

				// The value inserted into the new `review` column has been backfilled into the
				// old `review` column.
				rows := MustSelect(t, db, schema, "01_add_table", "reviews")
				assert.Equal(t, []map[string]any{
					{"id": 2, "username": "alice", "product": "apple", "review": "amazing"},
				}, rows)

				// Inserting a NULL value into the old `review` column should succeed
				MustInsert(t, db, schema, "01_add_table", "reviews", map[string]string{
					"username": "bob",
					"product":  "banana",
				})

				// The NULL value inserted into the old `review` column has been written into
				// the new `review` column using the `up` SQL.
				rows = MustSelect(t, db, schema, "02_set_nullable", "reviews")
				assert.Equal(t, []map[string]any{
					{"id": 2, "username": "alice", "product": "apple", "review": "amazing"},
					{"id": 3, "username": "bob", "product": "banana", "review": "banana is good"},
				}, rows)

				// Inserting a non-NULL value into the old `review` column should succeed
				MustInsert(t, db, schema, "01_add_table", "reviews", map[string]string{
					"username": "carl",
					"product":  "carrot",
					"review":   "crunchy",
				})

				// The non-NULL value inserted into the old `review` column has been copied
				// unchanged into the new `review` column.
				rows = MustSelect(t, db, schema, "02_set_nullable", "reviews")
				assert.Equal(t, []map[string]any{
					{"id": 2, "username": "alice", "product": "apple", "review": "amazing"},
					{"id": 3, "username": "bob", "product": "banana", "review": "banana is good"},
					{"id": 4, "username": "carl", "product": "carrot", "review": "crunchy"},
				}, rows)
			},
			afterRollback: func(t *testing.T, db *sql.DB, schema string) {
				// The table is cleaned up; temporary columns, trigger functions and triggers no longer exist.
				TableMustBeCleanedUp(t, db, schema, "reviews", "review")
			},
			afterComplete: func(t *testing.T, db *sql.DB, schema string) {
				// The new (temporary) `review` column should not exist on the underlying table.
				ColumnMustNotExist(t, db, schema, "reviews", migrations.TemporaryName("review"))

				// Selecting from the `reviews` view should succeed.
				rows := MustSelect(t, db, schema, "02_set_nullable", "reviews")
				assert.Equal(t, []map[string]any{
					{"id": 2, "username": "alice", "product": "apple", "review": "amazing"},
					{"id": 3, "username": "bob", "product": "banana", "review": "banana is good"},
					{"id": 4, "username": "carl", "product": "carrot", "review": "crunchy"},
				}, rows)

				// Writing NULL reviews into the `review` column should fail.
				MustNotInsert(t, db, schema, "02_set_nullable", "reviews", map[string]string{
					"username": "daisy",
					"product":  "durian",
				}, testutils.NotNullViolationErrorCode)

				// The table is cleaned up; temporary columns, trigger functions and triggers no longer exist.
				TableMustBeCleanedUp(t, db, schema, "reviews", "review")
			},
		},
		{
			name: "set not null with user-supplied down sql",
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
									Nullable: ptr(true),
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
							Nullable: ptr(false),
							Up:       "SELECT CASE WHEN review IS NULL THEN product || ' is good' ELSE review END",
							Down:     "review || ' (from new column)'",
						},
					},
				},
			},
			afterStart: func(t *testing.T, db *sql.DB, schema string) {
				// Inserting a non-NULL value into the new `review` column should succeed
				MustInsert(t, db, schema, "02_set_nullable", "reviews", map[string]string{
					"username": "alice",
					"product":  "apple",
					"review":   "amazing",
				})

				// The value inserted into the new `review` column has been backfilled into the
				// old `review` column using the user-supplied `down` SQL.
				rows := MustSelect(t, db, schema, "01_add_table", "reviews")
				assert.Equal(t, []map[string]any{
					{"id": 1, "username": "alice", "product": "apple", "review": "amazing (from new column)"},
				}, rows)
			},
			afterRollback: func(t *testing.T, db *sql.DB, schema string) {
			},
			afterComplete: func(t *testing.T, db *sql.DB, schema string) {
			},
		},
		{
			name: "setting a foreign key column to not null retains the foreign key constraint",
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
									Pk:   true,
								},
								{
									Name:     "name",
									Type:     "text",
									Nullable: ptr(false),
								},
								{
									Name:     "department_id",
									Type:     "integer",
									Nullable: ptr(true),
									References: &migrations.ForeignKeyReference{
										Name:     "fk_employee_department",
										Table:    "departments",
										Column:   "id",
										OnDelete: migrations.ForeignKeyReferenceOnDeleteCASCADE,
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
							Nullable: ptr(false),
							Up:       "SELECT CASE WHEN department_id IS NULL THEN 1 ELSE department_id END",
							Down:     "department_id",
						},
					},
				},
			},
			afterStart: func(t *testing.T, db *sql.DB, schema string) {
				// A temporary FK constraint has been created on the temporary column
				ValidatedForeignKeyMustExist(t, db, schema, "employees", migrations.DuplicationName("fk_employee_department"), withOnDeleteCascade())
			},
			afterRollback: func(t *testing.T, db *sql.DB, schema string) {
			},
			afterComplete: func(t *testing.T, db *sql.DB, schema string) {
				// The foreign key constraint still exists on the column
				ValidatedForeignKeyMustExist(t, db, schema, "employees", "fk_employee_department", withOnDeleteCascade())
			},
		},
		{
			name: "setting a column to not null retains any default defined on the column",
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
									Nullable: ptr(true),
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
							Nullable: ptr(false),
							Up:       "SELECT CASE WHEN name IS NULL THEN 'anonymous' ELSE name END",
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
			name: "setting a column to not null retains any check constraints defined on the column",
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
									Nullable: ptr(true),
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
							Nullable: ptr(false),
							Up:       "SELECT CASE WHEN name IS NULL THEN 'anonymous' ELSE name END",
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
			name: "setting a column to not null retains any unique constraints defined on the column",
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
									Nullable: ptr(true),
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
							Nullable: ptr(false),
							Up:       "SELECT CASE WHEN name IS NULL THEN 'anonymous' ELSE name END",
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
			name: "setting a column to not null retains any comment defined on the column",
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
									Nullable: ptr(true),
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
							Nullable: ptr(false),
							Up:       "SELECT CASE WHEN name IS NULL THEN 'anonymous' ELSE name END",
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

func TestSetNotNullValidation(t *testing.T) {
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
						Nullable: ptr(true),
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
					Name: "02_set_nullable",
					Operations: migrations.Operations{
						&migrations.OpAlterColumn{
							Table:    "reviews",
							Column:   "review",
							Nullable: ptr(false),
							Down:     "review",
						},
					},
				},
			},
			wantStartErr: migrations.FieldRequiredError{Name: "up"},
		},
		{
			name: "column is nullable",
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
						&migrations.OpAlterColumn{
							Table:    "reviews",
							Column:   "review",
							Nullable: ptr(false),
							Up:       "SELECT CASE WHEN review IS NULL THEN product || ' is good' ELSE review END",
							Down:     "review",
						},
					},
				},
			},
			wantStartErr: migrations.ColumnIsNotNullableError{Table: "reviews", Name: "review"},
		},
	})
}
