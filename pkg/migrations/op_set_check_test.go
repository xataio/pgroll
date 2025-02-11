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
							Up:   "SELECT CASE WHEN length(title) <= 3 THEN LPAD(title, 4, '-') ELSE title END",
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
				// The table is cleaned up; temporary columns, trigger functions and triggers no longer exist.
				TableMustBeCleanedUp(t, db, schema, "posts", "title")
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
							Up:   "SELECT CASE WHEN length(title) <= 3 THEN LPAD(title, 4, '-') ELSE title END",
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
										Name:     "fk_employee_department",
										Table:    "departments",
										Column:   "id",
										OnDelete: migrations.ForeignKeyActionCASCADE,
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
							Up:   "SELECT CASE WHEN department_id <= 1 THEN 2 ELSE department_id END",
							Down: "department_id",
						},
					},
				},
			},
			afterStart: func(t *testing.T, db *sql.DB, schema string) {
				// A temporary FK constraint has been created on the temporary column
				ValidatedForeignKeyMustExistWithReferentialAction(
					t,
					db,
					schema,
					"employees",
					migrations.DuplicationName("fk_employee_department"),
					migrations.ForeignKeyActionCASCADE,
					migrations.ForeignKeyActionNOACTION)
			},
			afterRollback: func(t *testing.T, db *sql.DB, schema string) {
			},
			afterComplete: func(t *testing.T, db *sql.DB, schema string) {
				// The foreign key constraint still exists on the column
				ValidatedForeignKeyMustExistWithReferentialAction(
					t,
					db,
					schema,
					"employees",
					"fk_employee_department",
					migrations.ForeignKeyActionCASCADE,
					migrations.ForeignKeyActionNOACTION)
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
							Up:   "SELECT CASE WHEN length(body) <= 3 THEN LPAD(body, 4, '-') ELSE body END",
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
							Up:   "SELECT CASE WHEN length(title) <= 3 THEN LPAD(title, 4, '-') ELSE title END",
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
							Up:   "SELECT CASE WHEN length(title) <= 3 THEN LPAD(title, 4, '-') ELSE title END",
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
									Pk:   true,
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
							Up:   "SELECT CASE WHEN length(title) <= 3 THEN LPAD(title, 4, '-') ELSE title END",
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
							Up:   "SELECT CASE WHEN length(title) <= 3 THEN LPAD(title, 4, '-') ELSE title END",
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
							Up:   "SELECT CASE WHEN length(title) <= 3 THEN LPAD(title, 4, '-') ELSE title END",
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

func TestSetCheckInMultiOperationMigrations(t *testing.T) {
	t.Parallel()

	ExecuteTests(t, TestCases{
		{
			name: "rename table, set not null",
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
									Name:     "name",
									Type:     "varchar(255)",
									Nullable: true,
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
							Table:  "products",
							Column: "name",
							Check: &migrations.CheckConstraint{
								Name:       "check_name_length",
								Constraint: "LENGTH(name) > 2",
							},
							Up:   "SELECT CASE WHEN length(name) > 2 THEN name ELSE name || '---' END",
							Down: "name",
						},
					},
				},
			},
			afterStart: func(t *testing.T, db *sql.DB, schema string) {
				// Can insert a row into the new view that meets the check constraint
				MustInsert(t, db, schema, "02_multi_operation", "products", map[string]string{
					"id":   "1",
					"name": "abc",
				})

				// Can't insert a row into the new view that violates the check constraint
				MustNotInsert(t, db, schema, "02_multi_operation", "products", map[string]string{
					"id":   "2",
					"name": "x",
				}, testutils.CheckViolationErrorCode)

				// Can insert a row into the old view that violates the check constraint
				MustInsert(t, db, schema, "01_create_table", "items", map[string]string{
					"id":   "3",
					"name": "x",
				})

				// The new view has the expected rows
				rows := MustSelect(t, db, schema, "02_multi_operation", "products")
				assert.Equal(t, []map[string]any{
					{"id": 1, "name": "abc"},
					{"id": 3, "name": "x---"},
				}, rows)

				// The old view has the expected rows
				rows = MustSelect(t, db, schema, "01_create_table", "items")
				assert.Equal(t, []map[string]any{
					{"id": 1, "name": "abc"},
					{"id": 3, "name": "x"},
				}, rows)
			},
			afterRollback: func(t *testing.T, db *sql.DB, schema string) {
				// The table has been cleaned up
				TableMustBeCleanedUp(t, db, schema, "items", "name")
			},
			afterComplete: func(t *testing.T, db *sql.DB, schema string) {
				// Can insert a row into the new view that meets the check constraint
				MustInsert(t, db, schema, "02_multi_operation", "products", map[string]string{
					"id":   "4",
					"name": "def",
				})

				// Can't insert a row into the new view that violates the check constraint
				MustNotInsert(t, db, schema, "02_multi_operation", "products", map[string]string{
					"id":   "5",
					"name": "x",
				}, testutils.CheckViolationErrorCode)

				// The new view has the expected rows
				rows := MustSelect(t, db, schema, "02_multi_operation", "products")
				assert.Equal(t, []map[string]any{
					{"id": 1, "name": "abc"},
					{"id": 3, "name": "x---"},
					{"id": 4, "name": "def"},
				}, rows)
			},
		},
		{
			name: "rename table, rename column set not null",
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
									Name:     "name",
									Type:     "varchar(255)",
									Nullable: true,
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
							Table:  "products",
							Column: "item_name",
							Check: &migrations.CheckConstraint{
								Name:       "check_name_length",
								Constraint: "LENGTH(item_name) > 2",
							},
							Up:   "SELECT CASE WHEN length(item_name) > 2 THEN item_name ELSE item_name || '---' END",
							Down: "item_name",
						},
					},
				},
			},
			afterStart: func(t *testing.T, db *sql.DB, schema string) {
				// Can insert a row into the new view that meets the check constraint
				MustInsert(t, db, schema, "02_multi_operation", "products", map[string]string{
					"id":        "1",
					"item_name": "abc",
				})

				// Can't insert a row into the new view that violates the check constraint
				MustNotInsert(t, db, schema, "02_multi_operation", "products", map[string]string{
					"id":        "2",
					"item_name": "x",
				}, testutils.CheckViolationErrorCode)

				// Can insert a row into the old view that violates the check constraint
				MustInsert(t, db, schema, "01_create_table", "items", map[string]string{
					"id":   "3",
					"name": "x",
				})

				// The new view has the expected rows
				rows := MustSelect(t, db, schema, "02_multi_operation", "products")
				assert.Equal(t, []map[string]any{
					{"id": 1, "item_name": "abc"},
					{"id": 3, "item_name": "x---"},
				}, rows)

				// The old view has the expected rows
				rows = MustSelect(t, db, schema, "01_create_table", "items")
				assert.Equal(t, []map[string]any{
					{"id": 1, "name": "abc"},
					{"id": 3, "name": "x"},
				}, rows)
			},
			afterRollback: func(t *testing.T, db *sql.DB, schema string) {
				// The table has been cleaned up
				TableMustBeCleanedUp(t, db, schema, "items", "name")
			},
			afterComplete: func(t *testing.T, db *sql.DB, schema string) {
				// Can insert a row into the new view that meets the check constraint
				MustInsert(t, db, schema, "02_multi_operation", "products", map[string]string{
					"id":        "4",
					"item_name": "def",
				})

				// Can't insert a row into the new view that violates the check constraint
				MustNotInsert(t, db, schema, "02_multi_operation", "products", map[string]string{
					"id":        "5",
					"item_name": "x",
				}, testutils.CheckViolationErrorCode)

				// The new view has the expected rows
				rows := MustSelect(t, db, schema, "02_multi_operation", "products")
				assert.Equal(t, []map[string]any{
					{"id": 1, "item_name": "abc"},
					{"id": 3, "item_name": "x---"},
					{"id": 4, "item_name": "def"},
				}, rows)
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
							Up:   "SELECT CASE WHEN length(title) <= 3 THEN LPAD(title, 4, '-') ELSE title END",
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
							Up: "SELECT CASE WHEN length(title) <= 3 THEN LPAD(title, 4, '-') ELSE title END",
						},
					},
				},
			},
			wantStartErr: migrations.FieldRequiredError{Name: "down"},
		},
	})
}
