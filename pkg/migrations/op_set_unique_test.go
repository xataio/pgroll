// SPDX-License-Identifier: Apache-2.0

package migrations_test

import (
	"database/sql"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/xataio/pgroll/pkg/migrations"
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
					Name: "02_set_unique",
					Operations: migrations.Operations{
						&migrations.OpAlterColumn{
							Table:  "reviews",
							Column: "review",
							Unique: &migrations.UniqueConstraint{
								Name: "reviews_review_unique",
							},
							Up: "review || '-' || (random()*1000000)::integer",
						},
					},
				},
			},
			afterStart: func(t *testing.T, db *sql.DB) {
				// Inserting values into the old schema that violate uniqueness should succeed.
				MustInsert(t, db, "public", "01_add_table", "reviews", map[string]string{
					"username": "alice", "product": "apple", "review": "good",
				})
				MustInsert(t, db, "public", "01_add_table", "reviews", map[string]string{
					"username": "bob", "product": "banana", "review": "good",
				})

				// Inserting values into the new schema that violate uniqueness should fail.
				MustInsert(t, db, "public", "02_set_unique", "reviews", map[string]string{
					"username": "carl", "product": "carrot", "review": "bad",
				})
				MustNotInsert(t, db, "public", "02_set_unique", "reviews", map[string]string{
					"username": "dana", "product": "durian", "review": "bad",
				})
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

				// The up function no longer exists.
				FunctionMustNotExist(t, db, "public", migrations.TriggerFunctionName("reviews", "review"))
				// The down function no longer exists.
				FunctionMustNotExist(t, db, "public", migrations.TriggerFunctionName("reviews", migrations.TemporaryName("review")))

				// The up trigger no longer exists.
				TriggerMustNotExist(t, db, "public", "reviews", migrations.TriggerName("reviews", "review"))
				// The down trigger no longer exists.
				TriggerMustNotExist(t, db, "public", "reviews", migrations.TriggerName("reviews", migrations.TemporaryName("review")))

				// Inserting values into the new schema that violate uniqueness should fail.
				MustInsert(t, db, "public", "02_set_unique", "reviews", map[string]string{
					"username": "earl", "product": "elderberry", "review": "ok",
				})
				MustNotInsert(t, db, "public", "02_set_unique", "reviews", map[string]string{
					"username": "flora", "product": "fig", "review": "ok",
				})
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
					Name: "02_set_unique",
					Operations: migrations.Operations{
						&migrations.OpAlterColumn{
							Table:  "reviews",
							Column: "review",
							Unique: &migrations.UniqueConstraint{
								Name: "reviews_review_unique",
							},
							Up:   "review || '-' || (random()*1000000)::integer",
							Down: "review || '!'",
						},
					},
				},
			},
			afterStart: func(t *testing.T, db *sql.DB) {
				// Inserting values into the new schema backfills the old column using the `down` SQL.
				MustInsert(t, db, "public", "02_set_unique", "reviews", map[string]string{
					"username": "carl", "product": "carrot", "review": "bad",
				})

				rows := MustSelect(t, db, "public", "01_add_table", "reviews")
				assert.Equal(t, []map[string]any{
					{"id": 1, "username": "carl", "product": "carrot", "review": "bad!"},
				}, rows)
			},
			afterRollback: func(t *testing.T, db *sql.DB) {
			},
			afterComplete: func(t *testing.T, db *sql.DB) {
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
									Pk:   true,
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
						&migrations.OpAlterColumn{
							Table:  "reviews",
							Column: "username",
							Unique: &migrations.UniqueConstraint{
								Name: "reviews_username_unique",
							},
							Up:   "username",
							Down: "username",
						},
					},
				},
			},
			afterStart: func(t *testing.T, db *sql.DB) {
				// A row can be inserted into the new version of the table.
				MustInsert(t, db, "public", "02_set_unique", "reviews", map[string]string{
					"product": "apple", "review": "awesome",
				})

				// The newly inserted row respects the default value of the column.
				rows := MustSelect(t, db, "public", "02_set_unique", "reviews")
				assert.Equal(t, []map[string]any{
					{"id": 1, "username": "anonymous", "product": "apple", "review": "awesome"},
				}, rows)
			},
			afterRollback: func(t *testing.T, db *sql.DB) {
			},
			afterComplete: func(t *testing.T, db *sql.DB) {
				// Delete the row that was inserted in the `afterStart` hook to
				// ensure that another row with a default 'username' can be inserted
				// without violating the UNIQUE constraint on the column.
				MustDelete(t, db, "public", "02_set_unique", "reviews", map[string]string{
					"id": "1",
				})

				// A row can be inserted into the new version of the table.
				MustInsert(t, db, "public", "02_set_unique", "reviews", map[string]string{
					"product": "banana", "review": "bent",
				})

				// The newly inserted row respects the default value of the column.
				rows := MustSelect(t, db, "public", "02_set_unique", "reviews")
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
					Name: "03_set_unique",
					Operations: migrations.Operations{
						&migrations.OpAlterColumn{
							Table:  "employees",
							Column: "department_id",
							Unique: &migrations.UniqueConstraint{
								Name: "employees_department_id_unique",
							},
							Up:   "department_id",
							Down: "department_id",
						},
					},
				},
			},
			afterStart: func(t *testing.T, db *sql.DB) {
				// A temporary FK constraint has been created on the temporary column
				ConstraintMustExist(t, db, "public", "employees", migrations.TemporaryName("fk_employee_department"))
			},
			afterRollback: func(t *testing.T, db *sql.DB) {
			},
			afterComplete: func(t *testing.T, db *sql.DB) {
				// The foreign key constraint still exists on the column
				ConstraintMustExist(t, db, "public", "employees", "fk_employee_department")
			},
		},
	})
}
