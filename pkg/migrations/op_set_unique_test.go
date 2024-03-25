// SPDX-License-Identifier: Apache-2.0

package migrations_test

import (
	"database/sql"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/xataio/pgroll/pkg/migrations"
	"github.com/xataio/pgroll/pkg/testutils"
)

func TestSetColumnUnique(t *testing.T) {
	t.Parallel()

	ExecuteTests(t, TestCases{
		{
			name: "set unique with default down sql",
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
					Name: "02_set_unique",
					Operations: migrations.Operations{
						&migrations.OpSetUnique{
							Table:  "reviews",
							Column: "review",
							Unique: migrations.UniqueConstraint{Name: "reviews_review_unique"},
							Up:     "review || '-' || (random()*1000000)::integer",
						},
					},
				},
			},
			afterStart: func(t *testing.T, db *sql.DB, schema string) {
				// Inserting values into the old schema that violate uniqueness should succeed.
				MustInsert(t, db, schema, "01_add_table", "reviews", map[string]string{
					"username": "alice", "product": "apple", "review": "good",
				})
				MustInsert(t, db, schema, "01_add_table", "reviews", map[string]string{
					"username": "bob", "product": "banana", "review": "good",
				})

				// Inserting values into the new schema that violate uniqueness should fail.
				MustInsert(t, db, schema, "02_set_unique", "reviews", map[string]string{
					"username": "carl", "product": "carrot", "review": "bad",
				})
				MustNotInsert(t, db, schema, "02_set_unique", "reviews", map[string]string{
					"username": "dana", "product": "durian", "review": "bad",
				}, testutils.UniqueViolationErrorCode)
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

				// The up function no longer exists.
				FunctionMustNotExist(t, db, schema, migrations.TriggerFunctionName("reviews", "review"))
				// The down function no longer exists.
				FunctionMustNotExist(t, db, schema, migrations.TriggerFunctionName("reviews", migrations.TemporaryName("review")))

				// The up trigger no longer exists.
				TriggerMustNotExist(t, db, schema, "reviews", migrations.TriggerName("reviews", "review"))
				// The down trigger no longer exists.
				TriggerMustNotExist(t, db, schema, "reviews", migrations.TriggerName("reviews", migrations.TemporaryName("review")))

				// Inserting values into the new schema that violate uniqueness should fail.
				MustInsert(t, db, schema, "02_set_unique", "reviews", map[string]string{
					"username": "earl", "product": "elderberry", "review": "ok",
				})
				MustNotInsert(t, db, schema, "02_set_unique", "reviews", map[string]string{
					"username": "flora", "product": "fig", "review": "ok",
				}, testutils.UniqueViolationErrorCode)
			},
		},
		{
			name: "set unique with user supplied down sql",
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
					Name: "02_set_unique",
					Operations: migrations.Operations{
						&migrations.OpSetUnique{
							Table:  "reviews",
							Column: "review",
							Unique: migrations.UniqueConstraint{Name: "reviews_review_unique"},
							Up:     "review || '-' || (random()*1000000)::integer",
							Down:   "review || '!'",
						},
					},
				},
			},
			afterStart: func(t *testing.T, db *sql.DB, schema string) {
				// Inserting values into the new schema backfills the old column using the `down` SQL.
				MustInsert(t, db, schema, "02_set_unique", "reviews", map[string]string{
					"username": "carl", "product": "carrot", "review": "bad",
				})

				rows := MustSelect(t, db, schema, "01_add_table", "reviews")
				assert.Equal(t, []map[string]any{
					{"id": 1, "username": "carl", "product": "carrot", "review": "bad!"},
				}, rows)
			},
			afterRollback: func(t *testing.T, db *sql.DB, schema string) {
			},
			afterComplete: func(t *testing.T, db *sql.DB, schema string) {
			},
		},
		{
			name: "column defaults are preserved when adding a unique constraint",
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
									Name:    "username",
									Type:    "text",
									Default: ptr("'anonymous'"),
								},
								{
									Name: "product",
									Type: "text",
								},
								{
									Name: "review",
									Type: "text",
								},
							},
						},
					},
				},
				{
					Name: "02_set_unique",
					Operations: migrations.Operations{
						&migrations.OpSetUnique{
							Table:  "reviews",
							Column: "username",
							Unique: migrations.UniqueConstraint{Name: "reviews_username_unique"},
							Up:     "username",
							Down:   "username",
						},
					},
				},
			},
			afterStart: func(t *testing.T, db *sql.DB, schema string) {
				// A row can be inserted into the new version of the table.
				MustInsert(t, db, schema, "02_set_unique", "reviews", map[string]string{
					"product": "apple", "review": "awesome",
				})

				// The newly inserted row respects the default value of the column.
				rows := MustSelect(t, db, schema, "02_set_unique", "reviews")
				assert.Equal(t, []map[string]any{
					{"id": 1, "username": "anonymous", "product": "apple", "review": "awesome"},
				}, rows)
			},
			afterRollback: func(t *testing.T, db *sql.DB, schema string) {
			},
			afterComplete: func(t *testing.T, db *sql.DB, schema string) {
				// Delete the row that was inserted in the `afterStart` hook to
				// ensure that another row with a default 'username' can be inserted
				// without violating the UNIQUE constraint on the column.
				MustDelete(t, db, schema, "02_set_unique", "reviews", map[string]string{
					"id": "1",
				})

				// A row can be inserted into the new version of the table.
				MustInsert(t, db, schema, "02_set_unique", "reviews", map[string]string{
					"product": "banana", "review": "bent",
				})

				// The newly inserted row respects the default value of the column.
				rows := MustSelect(t, db, schema, "02_set_unique", "reviews")
				assert.Equal(t, []map[string]any{
					{"id": 2, "username": "anonymous", "product": "banana", "review": "bent"},
				}, rows)
			},
		},
		{
			name: "foreign keys defined on the column are preserved when adding a unique constraint",
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
									Nullable: ptr(true),
									References: &migrations.ForeignKeyReference{
										Name:     "fk_employee_department",
										Table:    "departments",
										Column:   "id",
										OnDelete: migrations.ForeignKeyReferenceOnDeleteSETNULL,
									},
								},
							},
						},
					},
				},
				{
					Name: "03_set_unique",
					Operations: migrations.Operations{
						&migrations.OpSetUnique{
							Table:  "employees",
							Column: "department_id",
							Unique: migrations.UniqueConstraint{Name: "employees_department_id_unique"},
							Up:     "department_id",
							Down:   "department_id",
						},
					},
				},
			},
			afterStart: func(t *testing.T, db *sql.DB, schema string) {
				// A temporary FK constraint has been created on the temporary column
				ValidatedForeignKeyMustExist(t, db, schema, "employees", migrations.DuplicationName("fk_employee_department"), withOnDeleteSetNull())
			},
			afterRollback: func(t *testing.T, db *sql.DB, schema string) {
			},
			afterComplete: func(t *testing.T, db *sql.DB, schema string) {
				// The foreign key constraint still exists on the column
				ValidatedForeignKeyMustExist(t, db, schema, "employees", "fk_employee_department", withOnDeleteSetNull())
			},
		},
		{
			name: "check constraints are preserved when adding a unique constraint",
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
									Name: "username",
									Type: "text",
								},
								{
									Name: "review",
									Type: "text",
									Check: &migrations.CheckConstraint{
										Name:       "reviews_review_check",
										Constraint: "length(review) > 3",
									},
								},
							},
						},
					},
				},
				{
					Name: "02_set_unique",
					Operations: migrations.Operations{
						&migrations.OpSetUnique{
							Table:  "reviews",
							Column: "username",
							Unique: migrations.UniqueConstraint{Name: "reviews_username_unique"},
							Up:     "username",
							Down:   "username",
						},
					},
				},
			},
			afterStart: func(t *testing.T, db *sql.DB, schema string) {
				// Inserting a row that violates the check constraint should fail.
				MustNotInsert(t, db, schema, "02_set_unique", "reviews", map[string]string{
					"username": "alice",
					"review":   "x",
				}, testutils.CheckViolationErrorCode)
			},
			afterRollback: func(t *testing.T, db *sql.DB, schema string) {
			},
			afterComplete: func(t *testing.T, db *sql.DB, schema string) {
				// Inserting a row that violates the check constraint should fail.
				MustNotInsert(t, db, schema, "02_set_unique", "reviews", map[string]string{
					"username": "bob",
					"review":   "y",
				}, testutils.CheckViolationErrorCode)
			},
		},
		{
			name: "not null is preserved when adding a unique constraint",
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
									Name: "product",
									Type: "text",
								},
								{
									Name: "review",
									Type: "text",
								},
							},
						},
					},
				},
				{
					Name: "02_set_unique",
					Operations: migrations.Operations{
						&migrations.OpSetUnique{
							Table:  "reviews",
							Column: "username",
							Unique: migrations.UniqueConstraint{Name: "reviews_username_unique"},
							Up:     "username",
							Down:   "username",
						},
					},
				},
			},
			afterStart: func(t *testing.T, db *sql.DB, schema string) {
				// Inserting a row that violates the NOT NULL constraint on `username` should fail.
				MustNotInsert(t, db, schema, "02_set_unique", "reviews", map[string]string{
					"product": "apple", "review": "awesome",
				}, testutils.NotNullViolationErrorCode)
			},
			afterRollback: func(t *testing.T, db *sql.DB, schema string) {
			},
			afterComplete: func(t *testing.T, db *sql.DB, schema string) {
				// Inserting a row that violates the NOT NULL constraint on `username` should fail.
				MustNotInsert(t, db, schema, "02_set_unique", "reviews", map[string]string{
					"product": "apple", "review": "awesome",
				}, testutils.NotNullViolationErrorCode)
			},
		},
		{
			name: "comments are preserved when adding a unique constraint",
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
									Comment:  ptr("the name of the user"),
								},
								{
									Name: "product",
									Type: "text",
								},
								{
									Name: "review",
									Type: "text",
								},
							},
						},
					},
				},
				{
					Name: "02_set_unique",
					Operations: migrations.Operations{
						&migrations.OpAlterColumn{
							Table:  "reviews",
							Column: "username",
							Unique: &migrations.UniqueConstraint{
								Name: "reviews_username_unique",
							},
							Up:   ptr("username"),
							Down: ptr("username"),
						},
					},
				},
			},
			afterStart: func(t *testing.T, db *sql.DB, schema string) {
				// The duplicated column has a comment defined on it
				ColumnMustHaveComment(t, db, schema, "reviews", migrations.TemporaryName("username"), "the name of the user")
			},
			afterRollback: func(t *testing.T, db *sql.DB, schema string) {
			},
			afterComplete: func(t *testing.T, db *sql.DB, schema string) {
				// The final column has a comment defined on it
				ColumnMustHaveComment(t, db, schema, "reviews", "username", "the name of the user")
			},
		},
		// It should be possible to add multiple unique constraints to a column
		// once unique constraints covering multiple columns are supported.
		//
		// In that case it should be possible to test that existing unique constraints are
		// preserved when adding a new unique constraint covering the same column.
	})
}
