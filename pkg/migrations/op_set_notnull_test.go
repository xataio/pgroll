// SPDX-License-Identifier: Apache-2.0

package migrations_test

import (
	"database/sql"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/xataio/pgroll/pkg/migrations"
)

func TestSetNotNull(t *testing.T) {
	t.Parallel()

	ExecuteTests(t, TestCases{
		{
			name: "set nullable with default down sql",
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
							Table:    "reviews",
							Column:   "review",
							Nullable: ptr(false),
							Up:       "(SELECT CASE WHEN review IS NULL THEN product || ' is good' ELSE review END)",
						},
					},
				},
			},
			afterStart: func(t *testing.T, db *sql.DB) {
				// The new (temporary) `review` column should exist on the underlying table.
				ColumnMustExist(t, db, "public", "reviews", migrations.TemporaryName("review"))

				// Inserting a NULL into the new `review` column should fail
				MustNotInsert(t, db, "public", "02_set_nullable", "reviews", map[string]string{
					"username": "alice",
					"product":  "apple",
				})

				// Inserting a non-NULL value into the new `review` column should succeed
				MustInsert(t, db, "public", "02_set_nullable", "reviews", map[string]string{
					"username": "alice",
					"product":  "apple",
					"review":   "amazing",
				})

				// The value inserted into the new `review` column has been backfilled into the
				// old `review` column.
				rows := MustSelect(t, db, "public", "01_add_table", "reviews")
				assert.Equal(t, []map[string]any{
					{"id": 2, "username": "alice", "product": "apple", "review": "amazing"},
				}, rows)

				// Inserting a NULL value into the old `review` column should succeed
				MustInsert(t, db, "public", "01_add_table", "reviews", map[string]string{
					"username": "bob",
					"product":  "banana",
				})

				// The NULL value inserted into the old `review` column has been written into
				// the new `review` column using the `up` SQL.
				rows = MustSelect(t, db, "public", "02_set_nullable", "reviews")
				assert.Equal(t, []map[string]any{
					{"id": 2, "username": "alice", "product": "apple", "review": "amazing"},
					{"id": 3, "username": "bob", "product": "banana", "review": "banana is good"},
				}, rows)

				// Inserting a non-NULL value into the old `review` column should succeed
				MustInsert(t, db, "public", "01_add_table", "reviews", map[string]string{
					"username": "carl",
					"product":  "carrot",
					"review":   "crunchy",
				})

				// The non-NULL value inserted into the old `review` column has been copied
				// unchanged into the new `review` column.
				rows = MustSelect(t, db, "public", "02_set_nullable", "reviews")
				assert.Equal(t, []map[string]any{
					{"id": 2, "username": "alice", "product": "apple", "review": "amazing"},
					{"id": 3, "username": "bob", "product": "banana", "review": "banana is good"},
					{"id": 4, "username": "carl", "product": "carrot", "review": "crunchy"},
				}, rows)
			},
			afterRollback: func(t *testing.T, db *sql.DB) {
				// The new (temporary) `review` column should not exist on the underlying table.
				ColumnMustNotExist(t, db, "public", "reviews", migrations.TemporaryName("review"))

				// The up function no longer exists.
				FunctionMustNotExist(t, db, "public", migrations.TriggerFunctionName("reviews", "review"))
				// The down function no longer exists.
				FunctionMustNotExist(t, db, "public", migrations.TriggerFunctionName("reviews", migrations.TemporaryName("review")))

				// The up trigger no longer exists.
				TriggerMustNotExist(t, db, "public", "reviews", migrations.TriggerName("reviews", "review"))
				// The down trigger no longer exists.
				TriggerMustNotExist(t, db, "public", "reviews", migrations.TriggerName("reviews", migrations.TemporaryName("review")))
			},
			afterComplete: func(t *testing.T, db *sql.DB) {
				// The new (temporary) `review` column should not exist on the underlying table.
				ColumnMustNotExist(t, db, "public", "reviews", migrations.TemporaryName("review"))

				// Selecting from the `reviews` view should succeed.
				rows := MustSelect(t, db, "public", "02_set_nullable", "reviews")
				assert.Equal(t, []map[string]any{
					{"id": 2, "username": "alice", "product": "apple", "review": "amazing"},
					{"id": 3, "username": "bob", "product": "banana", "review": "banana is good"},
					{"id": 4, "username": "carl", "product": "carrot", "review": "crunchy"},
				}, rows)

				// Writing NULL reviews into the `review` column should fail.
				MustNotInsert(t, db, "public", "02_set_nullable", "reviews", map[string]string{
					"username": "daisy",
					"product":  "durian",
				})

				// The up function no longer exists.
				FunctionMustNotExist(t, db, "public", migrations.TriggerFunctionName("reviews", "review"))
				// The down function no longer exists.
				FunctionMustNotExist(t, db, "public", migrations.TriggerFunctionName("reviews", migrations.TemporaryName("review")))

				// The up trigger no longer exists.
				TriggerMustNotExist(t, db, "public", "reviews", migrations.TriggerName("reviews", "review"))
				// The down trigger no longer exists.
				TriggerMustNotExist(t, db, "public", "reviews", migrations.TriggerName("reviews", migrations.TemporaryName("review")))
			},
		},
		{
			name: "set nullable with user-supplied down sql",
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
							Table:    "reviews",
							Column:   "review",
							Nullable: ptr(false),
							Up:       "(SELECT CASE WHEN review IS NULL THEN product || ' is good' ELSE review END)",
							Down:     "review || ' (from new column)'",
						},
					},
				},
			},
			afterStart: func(t *testing.T, db *sql.DB) {
				// Inserting a non-NULL value into the new `review` column should succeed
				MustInsert(t, db, "public", "02_set_nullable", "reviews", map[string]string{
					"username": "alice",
					"product":  "apple",
					"review":   "amazing",
				})

				// The value inserted into the new `review` column has been backfilled into the
				// old `review` column using the user-supplied `down` SQL.
				rows := MustSelect(t, db, "public", "01_add_table", "reviews")
				assert.Equal(t, []map[string]any{
					{"id": 1, "username": "alice", "product": "apple", "review": "amazing (from new column)"},
				}, rows)
			},
			afterRollback: func(t *testing.T, db *sql.DB) {
			},
			afterComplete: func(t *testing.T, db *sql.DB) {
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
									Nullable: true,
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
							Nullable: ptr(false),
							Up:       "(SELECT CASE WHEN department_id IS NULL THEN 1 ELSE department_id END)",
							Down:     "department_id",
						},
					},
				},
			},
			afterStart: func(t *testing.T, db *sql.DB) {
				// A temporary FK constraint has been created on the temporary column
				ValidatedForeignKeyMustExist(t, db, "public", "employees", migrations.TemporaryName("fk_employee_department"))
			},
			afterRollback: func(t *testing.T, db *sql.DB) {
			},
			afterComplete: func(t *testing.T, db *sql.DB) {
				// The foreign key constraint still exists on the column
				ValidatedForeignKeyMustExist(t, db, "public", "employees", "fk_employee_department")
			},
		},
		{
			name: "setting a nullable column to not null retains any default defined on the column",
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
									Nullable: true,
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
							Up:       "(SELECT CASE WHEN name IS NULL THEN 'anonymous' ELSE name END)",
						},
					},
				},
			},
			afterStart: func(t *testing.T, db *sql.DB) {
				// A row can be inserted into the new version of the table.
				MustInsert(t, db, "public", "02_set_not_null", "users", map[string]string{
					"id": "1",
				})

				// The newly inserted row respects the default value of the column.
				rows := MustSelect(t, db, "public", "02_set_not_null", "users")
				assert.Equal(t, []map[string]any{
					{"id": 1, "name": "anonymous"},
				}, rows)
			},
			afterRollback: func(t *testing.T, db *sql.DB) {
			},
			afterComplete: func(t *testing.T, db *sql.DB) {
				// A row can be inserted into the new version of the table.
				MustInsert(t, db, "public", "02_set_not_null", "users", map[string]string{
					"id": "2",
				})

				// The newly inserted row respects the default value of the column.
				rows := MustSelect(t, db, "public", "02_set_not_null", "users")
				assert.Equal(t, []map[string]any{
					{"id": 1, "name": "anonymous"},
					{"id": 2, "name": "anonymous"},
				}, rows)
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
						Nullable: true,
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
							Nullable: ptr(false),
							Up:       "(SELECT CASE WHEN review IS NULL THEN product || ' is good' ELSE review END)",
							Down:     "review",
						},
					},
				},
			},
			wantStartErr: migrations.ColumnIsNotNullableError{Table: "reviews", Name: "review"},
		},
	})
}
