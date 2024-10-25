// SPDX-License-Identifier: Apache-2.0

package migrations_test

import (
	"database/sql"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/xataio/pgroll/internal/testutils"
	"github.com/xataio/pgroll/pkg/migrations"
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
									Pk:   ptr(true),
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
							Up:   "(SELECT CASE WHEN length(title) <= 3 THEN LPAD(title, 4, '-') ELSE title END)",
							Down: "title",
						},
					},
				},
			},
			afterStart: func(t *testing.T, db *sql.DB, schema string) {
				// The new (temporary) `title` column should exist on the underlying table.
				ColumnMustExist(t, db, schema, "posts", migrations.TemporaryName("title"))

				// A check constraint has been added to the temporary column
				CheckConstraintMustExist(t, db, schema, "posts", "check_title_length")

				// Inserting a row that meets the check constraint into the old view works.
				MustInsert(t, db, schema, "01_add_table", "posts", map[string]string{
					"title": "post by alice",
				})

				// Inserting a row that does not meet the check constraint into the old view also works.
				MustInsert(t, db, schema, "01_add_table", "posts", map[string]string{
					"title": "b",
				})

				// Both rows have been backfilled into the new view; the short title has
				// been rewritten using `up` SQL to meet the length constraint.
				rows := MustSelect(t, db, schema, "02_add_check_constraint", "posts")
				assert.Equal(t, []map[string]any{
					{"id": 1, "title": "post by alice"},
					{"id": 2, "title": "---b"},
				}, rows)

				// Inserting a row that meets the check constraint into the new view works.
				MustInsert(t, db, schema, "02_add_check_constraint", "posts", map[string]string{
					"title": "post by carl",
				})

				// Inserting a row that does not meet the check constraint into the new view fails.
				MustNotInsert(t, db, schema, "02_add_check_constraint", "posts", map[string]string{
					"title": "d",
				}, testutils.CheckViolationErrorCode)

				// The row that was inserted into the new view has been backfilled into the old view.
				rows = MustSelect(t, db, schema, "01_add_table", "posts")
				assert.Equal(t, []map[string]any{
					{"id": 1, "title": "post by alice"},
					{"id": 2, "title": "b"},
					{"id": 3, "title": "post by carl"},
				}, rows)
			},
			afterRollback: func(t *testing.T, db *sql.DB, schema string) {
				// The new (temporary) `title` column should not exist on the underlying table.
				ColumnMustNotExist(t, db, schema, "posts", migrations.TemporaryName("title"))

				// The check constraint no longer exists.
				CheckConstraintMustNotExist(t, db, schema, "posts", "check_title_length")

				// The up function no longer exists.
				FunctionMustNotExist(t, db, schema, migrations.TriggerFunctionName("posts", "title"))
				// The down function no longer exists.
				FunctionMustNotExist(t, db, schema, migrations.TriggerFunctionName("posts", migrations.TemporaryName("title")))

				// The up trigger no longer exists.
				TriggerMustNotExist(t, db, schema, "posts", migrations.TriggerName("posts", "title"))
				// The down trigger no longer exists.
				TriggerMustNotExist(t, db, schema, "posts", migrations.TriggerName("posts", migrations.TemporaryName("title")))
			},
			afterComplete: func(t *testing.T, db *sql.DB, schema string) {
				// The check constraint exists on the new table.
				CheckConstraintMustExist(t, db, schema, "posts", "check_title_length")

				// Inserting a row that meets the check constraint into the new view works.
				MustInsert(t, db, schema, "02_add_check_constraint", "posts", map[string]string{
					"title": "post by dana",
				})

				// Inserting a row that does not meet the check constraint into the new view fails.
				MustNotInsert(t, db, schema, "02_add_check_constraint", "posts", map[string]string{
					"title": "e",
				}, testutils.CheckViolationErrorCode)

				// The data in the new `posts` view is as expected.
				rows := MustSelect(t, db, schema, "02_add_check_constraint", "posts")
				assert.Equal(t, []map[string]any{
					{"id": 1, "title": "post by alice"},
					{"id": 2, "title": "---b"},
					{"id": 3, "title": "post by carl"},
					{"id": 5, "title": "post by dana"},
				}, rows)

				// The up function no longer exists.
				FunctionMustNotExist(t, db, schema, migrations.TriggerFunctionName("posts", "title"))
				// The down function no longer exists.
				FunctionMustNotExist(t, db, schema, migrations.TriggerFunctionName("posts", migrations.TemporaryName("title")))

				// The up trigger no longer exists.
				TriggerMustNotExist(t, db, schema, "posts", migrations.TriggerName("posts", "title"))
				// The down trigger no longer exists.
				TriggerMustNotExist(t, db, schema, "posts", migrations.TriggerName("posts", migrations.TemporaryName("title")))
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
									Pk:   ptr(true),
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
							Up:   "(SELECT CASE WHEN length(title) <= 3 THEN LPAD(title, 4, '-') ELSE title END)",
							Down: "title",
						},
					},
				},
			},
			afterStart: func(t *testing.T, db *sql.DB, schema string) {
				// A row can be inserted into the new version of the table.
				MustInsert(t, db, schema, "02_add_check_constraint", "posts", map[string]string{
					"id": "1",
				})

				// The newly inserted row respects the default value of the column.
				rows := MustSelect(t, db, schema, "02_add_check_constraint", "posts")
				assert.Equal(t, []map[string]any{
					{"id": 1, "title": "untitled"},
				}, rows)
			},
			afterRollback: func(t *testing.T, db *sql.DB, schema string) {
			},
			afterComplete: func(t *testing.T, db *sql.DB, schema string) {
				// A row can be inserted into the new version of the table.
				MustInsert(t, db, schema, "02_add_check_constraint", "posts", map[string]string{
					"id": "2",
				})

				// The newly inserted row respects the default value of the column.
				rows := MustSelect(t, db, schema, "02_add_check_constraint", "posts")
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
										Column:   ptr("id"),
										OnDelete: migrations.ForeignKeyReferenceOnDeleteCASCADE,
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
							Up:   "(SELECT CASE WHEN department_id <= 1 THEN 2 ELSE department_id END)",
							Down: "department_id",
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
									Pk:   ptr(true),
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
							Up:   "(SELECT CASE WHEN length(body) <= 3 THEN LPAD(body, 4, '-') ELSE body END)",
							Down: "body",
						},
					},
				},
			},
			afterStart: func(t *testing.T, db *sql.DB, schema string) {
				// The check constraint on the `title` column still exists.
				MustNotInsert(t, db, schema, "02_add_check_constraint", "posts", map[string]string{
					"id":    "1",
					"title": "a",
					"body":  "this is the post body",
				}, testutils.CheckViolationErrorCode)
			},
			afterRollback: func(t *testing.T, db *sql.DB, schema string) {
			},
			afterComplete: func(t *testing.T, db *sql.DB, schema string) {
				// The check constraint on the `title` column still exists.
				MustNotInsert(t, db, schema, "02_add_check_constraint", "posts", map[string]string{
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
									Pk:   ptr(true),
								},
								{
									Name:     "title",
									Type:     "text",
									Nullable: ptr(false),
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
							Up:   "(SELECT CASE WHEN length(title) <= 3 THEN LPAD(title, 4, '-') ELSE title END)",
							Down: "title",
						},
					},
				},
			},
			afterStart: func(t *testing.T, db *sql.DB, schema string) {
				// Inserting a row that violates the NOT NULL constraint on `title` fails.
				MustNotInsert(t, db, schema, "02_add_check_constraint", "posts", map[string]string{
					"id": "1",
				}, testutils.NotNullViolationErrorCode)
			},
			afterRollback: func(t *testing.T, db *sql.DB, schema string) {
			},
			afterComplete: func(t *testing.T, db *sql.DB, schema string) {
				// Inserting a row that violates the NOT NULL constraint on `title` fails.
				MustNotInsert(t, db, schema, "02_add_check_constraint", "posts", map[string]string{
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
									Pk:   ptr(true),
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
					Name: "02_set_unique",
					Operations: migrations.Operations{
						&migrations.OpAlterColumn{
							Table:  "posts",
							Column: "title",
							Unique: &migrations.UniqueConstraint{Name: "unique_title"},
							Up:     "title",
							Down:   "title",
						},
					},
				},
				{
					Name: "03_add_check_constraint",
					Operations: migrations.Operations{
						&migrations.OpAlterColumn{
							Table:  "posts",
							Column: "title",
							Check: &migrations.CheckConstraint{
								Name:       "check_title_length",
								Constraint: "length(title) > 3",
							},
							Up:   "(SELECT CASE WHEN length(title) <= 3 THEN LPAD(title, 4, '-') ELSE title END)",
							Down: "title",
						},
					},
				},
			},
			afterStart: func(t *testing.T, db *sql.DB, schema string) {
				// Inserting an initial row succeeds
				MustInsert(t, db, schema, "03_add_check_constraint", "posts", map[string]string{
					"title": "post by alice",
				})

				// Inserting a row with a duplicate `title` value fails
				MustNotInsert(t, db, schema, "03_add_check_constraint", "posts", map[string]string{
					"title": "post by alice",
				}, testutils.UniqueViolationErrorCode)
			},
			afterRollback: func(t *testing.T, db *sql.DB, schema string) {
			},
			afterComplete: func(t *testing.T, db *sql.DB, schema string) {
				// The table has a unique constraint defined on it
				UniqueConstraintMustExist(t, db, schema, "posts", "unique_title")

				// Inserting a row with a duplicate `title` value fails
				MustNotInsert(t, db, schema, "03_add_check_constraint", "posts", map[string]string{
					"title": "post by alice",
				}, testutils.UniqueViolationErrorCode)

				// Inserting a row with a different `title` value succeeds
				MustInsert(t, db, schema, "03_add_check_constraint", "posts", map[string]string{
					"title": "post by bob",
				})
			},
		},
		{
			name: "comments are preserved when adding a check constraint",
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
									Pk:   ptr(true),
								},
								{
									Name:    "title",
									Type:    "text",
									Comment: ptr("the title of the post"),
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
							Up:   "(SELECT CASE WHEN length(title) <= 3 THEN LPAD(title, 4, '-') ELSE title END)",
							Down: "title",
						},
					},
				},
			},
			afterStart: func(t *testing.T, db *sql.DB, schema string) {
				// The duplicated column has a comment defined on it
				ColumnMustHaveComment(t, db, schema, "posts", migrations.TemporaryName("title"), "the title of the post")
			},
			afterRollback: func(t *testing.T, db *sql.DB, schema string) {
			},
			afterComplete: func(t *testing.T, db *sql.DB, schema string) {
				// The final column has a comment defined on it
				ColumnMustHaveComment(t, db, schema, "posts", "title", "the title of the post")
			},
		},
		{
			name: "validate that check constraint name is unique",
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
									Pk:   ptr(true),
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
							Up:   "(SELECT CASE WHEN length(title) <= 3 THEN LPAD(title, 4, '-') ELSE title END)",
							Down: "title",
						},
					},
				},
				{
					Name: "03_add_check_constraint_again",
					Operations: migrations.Operations{
						&migrations.OpAlterColumn{
							Table:  "posts",
							Column: "title",
							Check: &migrations.CheckConstraint{
								Name:       "check_title_length",
								Constraint: "length(title) > 3",
							},
							Up:   "(SELECT CASE WHEN length(title) <= 3 THEN LPAD(title, 4, '-') ELSE title END)",
							Down: "title",
						},
					},
				},
			},
			wantStartErr: migrations.ConstraintAlreadyExistsError{
				Table:      "posts",
				Constraint: "check_title_length",
			},
			afterStart:    func(t *testing.T, db *sql.DB, schema string) {},
			afterRollback: func(t *testing.T, db *sql.DB, schema string) {},
			afterComplete: func(t *testing.T, db *sql.DB, schema string) {},
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
						Pk:   ptr(true),
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
							Up:   "(SELECT CASE WHEN length(title) <= 3 THEN LPAD(title, 4, '-') ELSE title END)",
							Down: "title",
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
							Down: "title",
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
							Up: "(SELECT CASE WHEN length(title) <= 3 THEN LPAD(title, 4, '-') ELSE title END)",
						},
					},
				},
			},
			wantStartErr: migrations.FieldRequiredError{Name: "down"},
		},
	})
}
