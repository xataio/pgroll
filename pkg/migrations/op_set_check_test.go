// SPDX-License-Identifier: Apache-2.0

package migrations_test

import (
	"database/sql"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/xataio/pgroll/pkg/migrations"
	"github.com/xataio/pgroll/pkg/testutils"
)

func TestSetCheckConstraint(t *testing.T) {
	t.Parallel()

	ExecuteTests(t, TestCases{
		{
			name: "add check constraint",
			migrations: []migrations.Migration{
				{
					Name: "01_add_table",
					Operations: migrations.Operations{
						&migrations.OpCreateTable{
							Name: "posts",
							Columns: []migrations.Column{
								{
									Name: "id",
									Type: "serial",
									Pk:   true,
								},
								{
									Name: "title",
									Type: "text",
								},
							},
						},
					},
				},
				{
					Name: "02_add_check_constraint",
					Operations: migrations.Operations{
						&migrations.OpAlterColumn{
							Table:  "posts",
							Column: "title",
							Check: &migrations.CheckConstraint{
								Name:       "check_title_length",
								Constraint: "length(title) > 3",
							},
							Up:   ptr("(SELECT CASE WHEN length(title) <= 3 THEN LPAD(title, 4, '-') ELSE title END)"),
							Down: ptr("title"),
						},
					},
				},
			},
			afterStart: func(t *testing.T, db *sql.DB) {
				// The new (temporary) `title` column should exist on the underlying table.
				ColumnMustExist(t, db, "public", "posts", migrations.TemporaryName("title"))

				// A check constraint has been added to the temporary column
				CheckConstraintMustExist(t, db, "public", "posts", "check_title_length")

				// Inserting a row that meets the check constraint into the old view works.
				MustInsert(t, db, "public", "01_add_table", "posts", map[string]string{
					"title": "post by alice",
				})

				// Inserting a row that does not meet the check constraint into the old view also works.
				MustInsert(t, db, "public", "01_add_table", "posts", map[string]string{
					"title": "b",
				})

				// Both rows have been backfilled into the new view; the short title has
				// been rewritten using `up` SQL to meet the length constraint.
				rows := MustSelect(t, db, "public", "02_add_check_constraint", "posts")
				assert.Equal(t, []map[string]any{
					{"id": 1, "title": "post by alice"},
					{"id": 2, "title": "---b"},
				}, rows)

				// Inserting a row that meets the check constraint into the new view works.
				MustInsert(t, db, "public", "02_add_check_constraint", "posts", map[string]string{
					"title": "post by carl",
				})

				// Inserting a row that does not meet the check constraint into the new view fails.
				MustNotInsert(t, db, "public", "02_add_check_constraint", "posts", map[string]string{
					"title": "d",
				}, testutils.CheckViolationErrorCode)

				// The row that was inserted into the new view has been backfilled into the old view.
				rows = MustSelect(t, db, "public", "01_add_table", "posts")
				assert.Equal(t, []map[string]any{
					{"id": 1, "title": "post by alice"},
					{"id": 2, "title": "b"},
					{"id": 3, "title": "post by carl"},
				}, rows)
			},
			afterRollback: func(t *testing.T, db *sql.DB) {
				// The new (temporary) `title` column should not exist on the underlying table.
				ColumnMustNotExist(t, db, "public", "posts", migrations.TemporaryName("title"))

				// The check constraint no longer exists.
				CheckConstraintMustNotExist(t, db, "public", "posts", "check_title_length")

				// The up function no longer exists.
				FunctionMustNotExist(t, db, "public", migrations.TriggerFunctionName("posts", "title"))
				// The down function no longer exists.
				FunctionMustNotExist(t, db, "public", migrations.TriggerFunctionName("posts", migrations.TemporaryName("title")))

				// The up trigger no longer exists.
				TriggerMustNotExist(t, db, "public", "posts", migrations.TriggerName("posts", "title"))
				// The down trigger no longer exists.
				TriggerMustNotExist(t, db, "public", "posts", migrations.TriggerName("posts", migrations.TemporaryName("title")))
			},
			afterComplete: func(t *testing.T, db *sql.DB) {
				// The check constraint exists on the new table.
				CheckConstraintMustExist(t, db, "public", "posts", "check_title_length")

				// Inserting a row that meets the check constraint into the new view works.
				MustInsert(t, db, "public", "02_add_check_constraint", "posts", map[string]string{
					"title": "post by dana",
				})

				// Inserting a row that does not meet the check constraint into the new view fails.
				MustNotInsert(t, db, "public", "02_add_check_constraint", "posts", map[string]string{
					"title": "e",
				}, testutils.CheckViolationErrorCode)

				// The data in the new `posts` view is as expected.
				rows := MustSelect(t, db, "public", "02_add_check_constraint", "posts")
				assert.Equal(t, []map[string]any{
					{"id": 1, "title": "post by alice"},
					{"id": 2, "title": "---b"},
					{"id": 3, "title": "post by carl"},
					{"id": 5, "title": "post by dana"},
				}, rows)

				// The up function no longer exists.
				FunctionMustNotExist(t, db, "public", migrations.TriggerFunctionName("posts", "title"))
				// The down function no longer exists.
				FunctionMustNotExist(t, db, "public", migrations.TriggerFunctionName("posts", migrations.TemporaryName("title")))

				// The up trigger no longer exists.
				TriggerMustNotExist(t, db, "public", "posts", migrations.TriggerName("posts", "title"))
				// The down trigger no longer exists.
				TriggerMustNotExist(t, db, "public", "posts", migrations.TriggerName("posts", migrations.TemporaryName("title")))
			},
		},
		{
			name: "column defaults are preserved when adding a check constraint",
			migrations: []migrations.Migration{
				{
					Name: "01_add_table",
					Operations: migrations.Operations{
						&migrations.OpCreateTable{
							Name: "posts",
							Columns: []migrations.Column{
								{
									Name: "id",
									Type: "serial",
									Pk:   true,
								},
								{
									Name:    "title",
									Type:    "text",
									Default: ptr("'untitled'"),
								},
							},
						},
					},
				},
				{
					Name: "02_add_check_constraint",
					Operations: migrations.Operations{
						&migrations.OpAlterColumn{
							Table:  "posts",
							Column: "title",
							Check: &migrations.CheckConstraint{
								Name:       "check_title_length",
								Constraint: "length(title) > 3",
							},
							Up:   ptr("(SELECT CASE WHEN length(title) <= 3 THEN LPAD(title, 4, '-') ELSE title END)"),
							Down: ptr("title"),
						},
					},
				},
			},
			afterStart: func(t *testing.T, db *sql.DB) {
				// A row can be inserted into the new version of the table.
				MustInsert(t, db, "public", "02_add_check_constraint", "posts", map[string]string{
					"id": "1",
				})

				// The newly inserted row respects the default value of the column.
				rows := MustSelect(t, db, "public", "02_add_check_constraint", "posts")
				assert.Equal(t, []map[string]any{
					{"id": 1, "title": "untitled"},
				}, rows)
			},
			afterRollback: func(t *testing.T, db *sql.DB) {
			},
			afterComplete: func(t *testing.T, db *sql.DB) {
				// A row can be inserted into the new version of the table.
				MustInsert(t, db, "public", "02_add_check_constraint", "posts", map[string]string{
					"id": "2",
				})

				// The newly inserted row respects the default value of the column.
				rows := MustSelect(t, db, "public", "02_add_check_constraint", "posts")
				assert.Equal(t, []map[string]any{
					{"id": 1, "title": "untitled"},
					{"id": 2, "title": "untitled"},
				}, rows)
			},
		},
		{
			name: "foreign keys are preserved when adding a check constraint",
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
					Name: "03_add_check_constraint",
					Operations: migrations.Operations{
						&migrations.OpAlterColumn{
							Table:  "employees",
							Column: "department_id",
							Check: &migrations.CheckConstraint{
								Name:       "check_valid_department_id",
								Constraint: "department_id > 1",
							},
							Up:   ptr("(SELECT CASE WHEN department_id <= 1 THEN 2 ELSE department_id END)"),
							Down: ptr("department_id"),
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
			name: "existing check constraints are preserved when adding a check constraint",
			migrations: []migrations.Migration{
				{
					Name: "01_add_table",
					Operations: migrations.Operations{
						&migrations.OpCreateTable{
							Name: "posts",
							Columns: []migrations.Column{
								{
									Name: "id",
									Type: "serial",
									Pk:   true,
								},
								{
									Name: "title",
									Type: "text",
									Check: &migrations.CheckConstraint{
										Name:       "check_title_length",
										Constraint: "length(title) > 3",
									},
								},
								{
									Name: "body",
									Type: "text",
								},
							},
						},
					},
				},
				{
					Name: "02_add_check_constraint",
					Operations: migrations.Operations{
						&migrations.OpAlterColumn{
							Table:  "posts",
							Column: "body",
							Check: &migrations.CheckConstraint{
								Name:       "check_body_length",
								Constraint: "length(body) > 3",
							},
							Up:   ptr("(SELECT CASE WHEN length(body) <= 3 THEN LPAD(body, 4, '-') ELSE body END)"),
							Down: ptr("body"),
						},
					},
				},
			},
			afterStart: func(t *testing.T, db *sql.DB) {
				// The check constraint on the `title` column still exists.
				MustNotInsert(t, db, "public", "02_add_check_constraint", "posts", map[string]string{
					"id":    "1",
					"title": "a",
					"body":  "this is the post body",
				}, testutils.CheckViolationErrorCode)
			},
			afterRollback: func(t *testing.T, db *sql.DB) {
			},
			afterComplete: func(t *testing.T, db *sql.DB) {
				// The check constraint on the `title` column still exists.
				MustNotInsert(t, db, "public", "02_add_check_constraint", "posts", map[string]string{
					"id":    "2",
					"title": "b",
					"body":  "this is another post body",
				}, testutils.CheckViolationErrorCode)
			},
		},
		{
			name: "not null is preserved when adding a check constraint",
			migrations: []migrations.Migration{
				{
					Name: "01_add_table",
					Operations: migrations.Operations{
						&migrations.OpCreateTable{
							Name: "posts",
							Columns: []migrations.Column{
								{
									Name: "id",
									Type: "serial",
									Pk:   true,
								},
								{
									Name:     "title",
									Type:     "text",
									Nullable: false,
								},
							},
						},
					},
				},
				{
					Name: "02_add_check_constraint",
					Operations: migrations.Operations{
						&migrations.OpAlterColumn{
							Table:  "posts",
							Column: "title",
							Check: &migrations.CheckConstraint{
								Name:       "check_title_length",
								Constraint: "length(title) > 3",
							},
							Up:   ptr("(SELECT CASE WHEN length(title) <= 3 THEN LPAD(title, 4, '-') ELSE title END)"),
							Down: ptr("title"),
						},
					},
				},
			},
			afterStart: func(t *testing.T, db *sql.DB) {
				// Inserting a row that violates the NOT NULL constraint on `title` fails.
				MustNotInsert(t, db, "public", "02_add_check_constraint", "posts", map[string]string{
					"id": "1",
				}, testutils.NotNullViolationErrorCode)
			},
			afterRollback: func(t *testing.T, db *sql.DB) {
			},
			afterComplete: func(t *testing.T, db *sql.DB) {
				// Inserting a row that violates the NOT NULL constraint on `title` fails.
				MustNotInsert(t, db, "public", "02_add_check_constraint", "posts", map[string]string{
					"id": "1",
				}, testutils.NotNullViolationErrorCode)
			},
		},
		{
			name: "unique constraints are preserved when adding a check constraint",
			migrations: []migrations.Migration{
				{
					Name: "01_add_table",
					Operations: migrations.Operations{
						&migrations.OpCreateTable{
							Name: "posts",
							Columns: []migrations.Column{
								{
									Name: "id",
									Type: "serial",
									Pk:   true,
								},
								{
									Name:   "title",
									Type:   "text",
									Unique: true,
								},
							},
						},
					},
				},
				{
					Name: "02_add_check_constraint",
					Operations: migrations.Operations{
						&migrations.OpAlterColumn{
							Table:  "posts",
							Column: "title",
							Check: &migrations.CheckConstraint{
								Name:       "check_title_length",
								Constraint: "length(title) > 3",
							},
							Up:   ptr("(SELECT CASE WHEN length(title) <= 3 THEN LPAD(title, 4, '-') ELSE title END)"),
							Down: ptr("title"),
						},
					},
				},
			},
			afterStart: func(t *testing.T, db *sql.DB) {
				// Inserting an initial row succeeds
				MustInsert(t, db, "public", "02_add_check_constraint", "posts", map[string]string{
					"title": "post by alice",
				})

				// Inserting a row with a duplicate `title` value fails
				MustNotInsert(t, db, "public", "02_add_check_constraint", "posts", map[string]string{
					"title": "post by alice",
				}, testutils.UniqueViolationErrorCode)
			},
			afterRollback: func(t *testing.T, db *sql.DB) {
			},
			afterComplete: func(t *testing.T, db *sql.DB) {
				// Inserting a row with a duplicate `title` value fails
				MustNotInsert(t, db, "public", "02_add_check_constraint", "posts", map[string]string{
					"title": "post by alice",
				}, testutils.UniqueViolationErrorCode)

				// Inserting a row with a different `title` value succeeds
				MustInsert(t, db, "public", "02_add_check_constraint", "posts", map[string]string{
					"title": "post by bob",
				})
			},
		},
	})
}

func TestSetCheckConstraintValidation(t *testing.T) {
	t.Parallel()

	createTableMigration := migrations.Migration{
		Name: "01_add_table",
		Operations: migrations.Operations{
			&migrations.OpCreateTable{
				Name: "posts",
				Columns: []migrations.Column{
					{
						Name: "id",
						Type: "serial",
						Pk:   true,
					},
					{
						Name: "title",
						Type: "text",
					},
				},
			},
		},
	}

	ExecuteTests(t, TestCases{
		{
			name: "name of the check constraint is mandatory",
			migrations: []migrations.Migration{
				createTableMigration,
				{
					Name: "02_add_check_constraint",
					Operations: migrations.Operations{
						&migrations.OpAlterColumn{
							Table:  "posts",
							Column: "title",
							Check: &migrations.CheckConstraint{
								Constraint: "length(title) > 3",
							},
							Up:   ptr("(SELECT CASE WHEN length(title) <= 3 THEN LPAD(title, 4, '-') ELSE title END)"),
							Down: ptr("title"),
						},
					},
				},
			},
			wantStartErr: migrations.FieldRequiredError{Name: "name"},
		},
		{
			name: "up SQL is mandatory",
			migrations: []migrations.Migration{
				createTableMigration,
				{
					Name: "02_add_check_constraint",
					Operations: migrations.Operations{
						&migrations.OpAlterColumn{
							Table:  "posts",
							Column: "title",
							Check: &migrations.CheckConstraint{
								Name:       "check_title_length",
								Constraint: "length(title) > 3",
							},
							Down: ptr("title"),
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
					Name: "02_add_check_constraint",
					Operations: migrations.Operations{
						&migrations.OpAlterColumn{
							Table:  "posts",
							Column: "title",
							Check: &migrations.CheckConstraint{
								Name:       "check_title_length",
								Constraint: "length(title) > 3",
							},
							Up: ptr("(SELECT CASE WHEN length(title) <= 3 THEN LPAD(title, 4, '-') ELSE title END)"),
						},
					},
				},
			},
			wantStartErr: migrations.FieldRequiredError{Name: "down"},
		},
	})
}
