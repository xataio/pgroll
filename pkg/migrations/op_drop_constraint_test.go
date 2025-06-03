// SPDX-License-Identifier: Apache-2.0

package migrations_test

import (
	"database/sql"
	"testing"

	"github.com/xataio/pgroll/internal/testutils"

	"github.com/stretchr/testify/assert"

	"github.com/xataio/pgroll/pkg/migrations"
)

func TestDropConstraint(t *testing.T) {
	t.Parallel()

	ExecuteTests(t, TestCases{
		{
			name: "drop check constraint with default up sql",
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
					Name: "03_drop_check_constraint",
					Operations: migrations.Operations{
						&migrations.OpDropConstraint{
							Table: "posts",
							Name:  "check_title_length",
							Down:  "SELECT CASE WHEN length(title) <= 3 THEN LPAD(title, 4, '-') ELSE title END",
						},
					},
				},
			},
			afterStart: func(t *testing.T, db *sql.DB, schema string) {
				// The new (temporary) `title` column should exist on the underlying table.
				ColumnMustExist(t, db, schema, "posts", migrations.TemporaryName("title"))

				// Inserting a row that meets the check constraint into the old view works.
				MustInsert(t, db, schema, "02_add_check_constraint", "posts", map[string]string{
					"title": "post by alice",
				})

				// Inserting a row that does not meet the check constraint into the old view fails.
				MustNotInsert(t, db, schema, "02_add_check_constraint", "posts", map[string]string{
					"title": "b",
				}, testutils.CheckViolationErrorCode)

				// The inserted row has been backfilled into the new view.
				rows := MustSelect(t, db, schema, "03_drop_check_constraint", "posts")
				assert.Equal(t, []map[string]any{
					{"id": 1, "title": "post by alice"},
				}, rows)

				// Inserting a row that meets the check constraint into the new view works.
				MustInsert(t, db, schema, "03_drop_check_constraint", "posts", map[string]string{
					"title": "post by carl",
				})

				// Inserting a row that does not meet the check constraint into the new view also works.
				MustInsert(t, db, schema, "03_drop_check_constraint", "posts", map[string]string{
					"title": "d",
				})

				// Both rows that were inserted into the new view have been backfilled
				// into the old view. The short `title` value has been rewritten to meet the
				// check constraint present on the old view.
				rows = MustSelect(t, db, schema, "02_add_check_constraint", "posts")
				assert.Equal(t, []map[string]any{
					{"id": 1, "title": "post by alice"},
					{"id": 3, "title": "post by carl"},
					{"id": 4, "title": "---d"},
				}, rows)
			},
			afterRollback: func(t *testing.T, db *sql.DB, schema string) {
				// The table is cleaned up; temporary columns, trigger functions and triggers no longer exist.
				TableMustBeCleanedUp(t, db, schema, "posts", "title")
			},
			afterComplete: func(t *testing.T, db *sql.DB, schema string) {
				// Inserting a row that does not meet the check constraint into the new view works.
				MustInsert(t, db, schema, "03_drop_check_constraint", "posts", map[string]string{
					"title": "e",
				})

				// The data in the new `posts` view is as expected.
				rows := MustSelect(t, db, schema, "03_drop_check_constraint", "posts")
				assert.Equal(t, []map[string]any{
					{"id": 1, "title": "post by alice"},
					{"id": 3, "title": "post by carl"},
					{"id": 4, "title": "---d"},
					{"id": 5, "title": "e"},
				}, rows)

				// The table is cleaned up; temporary columns, trigger functions and triggers no longer exist.
				TableMustBeCleanedUp(t, db, schema, "posts", "title")
			},
		},
		{
			name: "drop check constraint with user-supplied up sql",
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
					Name: "03_drop_check_constraint",
					Operations: migrations.Operations{
						&migrations.OpDropConstraint{
							Table: "posts",
							Name:  "check_title_length",
							Up:    "title || '!'",
							Down:  "SELECT CASE WHEN length(title) <= 3 THEN LPAD(title, 4, '-') ELSE title END",
						},
					},
				},
			},
			afterStart: func(t *testing.T, db *sql.DB, schema string) {
				// Inserting a row that meets the check constraint into the old view works.
				MustInsert(t, db, schema, "02_add_check_constraint", "posts", map[string]string{
					"title": "post by alice",
				})
				// The inserted row has been backfilled into the new view, using the user-supplied `up` SQL.
				rows := MustSelect(t, db, schema, "03_drop_check_constraint", "posts")
				assert.Equal(t, []map[string]any{
					{"id": 1, "title": "post by alice!"},
				}, rows)
			},
			afterRollback: func(t *testing.T, db *sql.DB, schema string) {
			},
			afterComplete: func(t *testing.T, db *sql.DB, schema string) {
			},
		},
		{
			name: "drop foreign key constraint",
			migrations: []migrations.Migration{
				{
					Name: "01_add_tables",
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
									Name: "name",
									Type: "text",
								},
							},
						},
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
								{
									Name:     "user_id",
									Type:     "integer",
									Nullable: true,
								},
							},
						},
					},
				},
				{
					Name: "02_add_fk_constraint",
					Operations: migrations.Operations{
						&migrations.OpAlterColumn{
							Table:  "posts",
							Column: "user_id",
							References: &migrations.ForeignKeyReference{
								Name:   "fk_users_id",
								Table:  "users",
								Column: "id",
							},
							Up:   "SELECT CASE WHEN EXISTS (SELECT 1 FROM users WHERE users.id = user_id) THEN user_id ELSE NULL END",
							Down: "user_id",
						},
					},
				},
				{
					Name: "03_drop_fk_constraint",
					Operations: migrations.Operations{
						&migrations.OpDropConstraint{
							Table: "posts",
							Name:  "fk_users_id",
							Up:    "user_id",
							Down:  "SELECT CASE WHEN EXISTS (SELECT 1 FROM users WHERE users.id = user_id) THEN user_id ELSE NULL END",
						},
					},
				},
			},
			afterStart: func(t *testing.T, db *sql.DB, schema string) {
				// The new (temporary) `user_id` column should exist on the underlying table.
				ColumnMustExist(t, db, schema, "posts", migrations.TemporaryName("user_id"))

				// Inserting some data into the `users` table works.
				MustInsert(t, db, schema, "03_drop_fk_constraint", "users", map[string]string{
					"name": "alice",
				})
				MustInsert(t, db, schema, "03_drop_fk_constraint", "users", map[string]string{
					"name": "bob",
				})

				// Inserting data into the new `posts` view with a valid user reference works.
				MustInsert(t, db, schema, "03_drop_fk_constraint", "posts", map[string]string{
					"title":   "post by alice",
					"user_id": "1",
				})

				// Inserting data into the new `posts` view with an invalid user reference also works.
				MustInsert(t, db, schema, "03_drop_fk_constraint", "posts", map[string]string{
					"title":   "post by unknown user",
					"user_id": "3",
				})

				// The inserted rows have been backfilled into the old view.
				// The invalid user reference has been rewritten to NULL.
				rows := MustSelect(t, db, schema, "02_add_fk_constraint", "posts")
				assert.Equal(t, []map[string]any{
					{"id": 1, "title": "post by alice", "user_id": 1},
					{"id": 2, "title": "post by unknown user", "user_id": nil},
				}, rows)

				// Inserting data into the old `posts` view with a valid user reference works.
				MustInsert(t, db, schema, "02_add_fk_constraint", "posts", map[string]string{
					"title":   "post by bob",
					"user_id": "2",
				})

				// Inserting data into the old `posts` view with an invalid user reference fails.
				MustNotInsert(t, db, schema, "02_add_fk_constraint", "posts", map[string]string{
					"title":   "post by unknown user",
					"user_id": "3",
				}, testutils.FKViolationErrorCode)

				// The post that was inserted successfully has been backfilled into the new view.
				rows = MustSelect(t, db, schema, "03_drop_fk_constraint", "posts")
				assert.Equal(t, []map[string]any{
					{"id": 1, "title": "post by alice", "user_id": 1},
					{"id": 2, "title": "post by unknown user", "user_id": 3},
					{"id": 3, "title": "post by bob", "user_id": 2},
				}, rows)
			},
			afterRollback: func(t *testing.T, db *sql.DB, schema string) {
				// The table is cleaned up; temporary columns, trigger functions and triggers no longer exist.
				TableMustBeCleanedUp(t, db, schema, "posts", "user_id")
			},
			afterComplete: func(t *testing.T, db *sql.DB, schema string) {
				// The new (temporary) `user_id` column should not exist on the underlying table.
				ColumnMustNotExist(t, db, schema, "posts", migrations.TemporaryName("user_id"))

				// Inserting a row that does not meet the check constraint into the new view works.
				MustInsert(t, db, schema, "03_drop_fk_constraint", "posts", map[string]string{
					"title":   "another post by an unknown user",
					"user_id": "4",
				})

				// The data in the new `posts` view is as expected.
				rows := MustSelect(t, db, schema, "03_drop_fk_constraint", "posts")
				assert.Equal(t, []map[string]any{
					{"id": 1, "title": "post by alice", "user_id": 1},
					{"id": 2, "title": "post by unknown user", "user_id": nil},
					{"id": 3, "title": "post by bob", "user_id": 2},
					{"id": 5, "title": "another post by an unknown user", "user_id": 4},
				}, rows)

				// The table is cleaned up; temporary columns, trigger functions and triggers no longer exist.
				TableMustBeCleanedUp(t, db, schema, "posts", "user_id")
			},
		},
		{
			name: "drop unique constraint",
			migrations: []migrations.Migration{
				{
					Name: "01_add_tables",
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
									Name:   "name",
									Type:   "text",
									Unique: true,
								},
							},
						},
					},
				},
				{
					Name: "02_drop_unique_constraint",
					Operations: migrations.Operations{
						&migrations.OpDropConstraint{
							Table: "users",
							Name:  "users_name_key",
							Up:    "name",
							Down:  "name || '-' || (random()*1000000)::integer",
						},
					},
				},
			},
			afterStart: func(t *testing.T, db *sql.DB, schema string) {
				// The new (temporary) `name` column should exist on the underlying table.
				ColumnMustExist(t, db, schema, "users", migrations.TemporaryName("name"))

				// Inserting a row that meets the unique constraint into the old view works.
				MustInsert(t, db, schema, "01_add_tables", "users", map[string]string{
					"name": "alice",
				})

				// Inserting a row that does not meet the unique constraint into the old view fails.
				MustNotInsert(t, db, schema, "01_add_tables", "users", map[string]string{
					"name": "alice",
				}, testutils.UniqueViolationErrorCode)

				// Inserting a row that does not meet the unique constraint into the new view works.
				MustInsert(t, db, schema, "02_drop_unique_constraint", "users", map[string]string{
					"name": "alice",
				})
			},
			afterRollback: func(t *testing.T, db *sql.DB, schema string) {
				// The table is cleaned up; temporary columns, trigger functions and triggers no longer exist.
				TableMustBeCleanedUp(t, db, schema, "users", "name")
			},
			afterComplete: func(t *testing.T, db *sql.DB, schema string) {
				// The table is cleaned up; temporary columns, trigger functions and triggers no longer exist.
				TableMustBeCleanedUp(t, db, schema, "users", "name")

				// Inserting a row that does not meet the unique constraint into the new view works.
				MustInsert(t, db, schema, "02_drop_unique_constraint", "users", map[string]string{
					"name": "alice",
				})
			},
		},
		{
			name: "drop unique constraint from serial column",
			migrations: []migrations.Migration{
				{
					Name: "01_add_tables",
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
									Name:   "secondary_id",
									Type:   "serial",
									Unique: true,
								},
							},
						},
					},
				},
				{
					Name: "02_drop_unique_constraint",
					Operations: migrations.Operations{
						&migrations.OpDropConstraint{
							Table: "users",
							Name:  "users_secondary_id_key",
							Up:    "secondary_id",
							Down:  "secondary_id",
						},
					},
				},
			},
			afterStart: func(t *testing.T, db *sql.DB, schema string) {
				// The new (temporary) `secondary_id` column should exist on the underlying table.
				ColumnMustExist(t, db, schema, "users", migrations.TemporaryName("secondary_id"))

				// Inserting a row that meets the unique constraint into the old view works.
				MustInsert(t, db, schema, "01_add_tables", "users", map[string]string{
					"secondary_id": "1",
				})

				// Inserting a row that does not meet the unique constraint into the old view fails.
				MustNotInsert(t, db, schema, "01_add_tables", "users", map[string]string{
					"secondary_id": "1",
				}, testutils.UniqueViolationErrorCode)
			},
			afterRollback: func(t *testing.T, db *sql.DB, schema string) {
				// The table is cleaned up; temporary columns, trigger functions and triggers no longer exist.
				TableMustBeCleanedUp(t, db, schema, "users", "secondary_id")
			},
			afterComplete: func(t *testing.T, db *sql.DB, schema string) {
				// The table is cleaned up; temporary columns, trigger functions and triggers no longer exist.
				TableMustBeCleanedUp(t, db, schema, "users", "secondary_id")

				// Inserting a row that does not meet the unique constraint into the new view works.
				MustInsert(t, db, schema, "02_drop_unique_constraint", "users", map[string]string{
					"secondary_id": "1",
				})
			},
		},
		{
			name: "dropping a unique constraint preserves the column's default value",
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
									Name:    "name",
									Type:    "text",
									Unique:  true,
									Default: ptr("'anonymous'"),
								},
							},
						},
					},
				},
				{
					Name: "02_drop_unique_constraint",
					Operations: migrations.Operations{
						&migrations.OpDropConstraint{
							Table: "users",
							Name:  "users_name_key",
							Up:    "name",
							Down:  "name || '-' || (random()*1000000)::integer",
						},
					},
				},
			},
			afterStart: func(t *testing.T, db *sql.DB, schema string) {
				// A row can be inserted into the new version of the table.
				MustInsert(t, db, schema, "02_drop_unique_constraint", "users", map[string]string{
					"id": "1",
				})

				// The newly inserted row respects the default value of the column.
				rows := MustSelect(t, db, schema, "02_drop_unique_constraint", "users")
				assert.Equal(t, []map[string]any{
					{"id": 1, "name": "anonymous"},
				}, rows)
			},
			afterRollback: func(t *testing.T, db *sql.DB, schema string) {
			},
			afterComplete: func(t *testing.T, db *sql.DB, schema string) {
				// Delete the row that was inserted in the `afterStart` hook to
				// ensure that another row with a default 'name` can be inserted
				// without violating the UNIQUE constraint on the column.
				MustDelete(t, db, schema, "02_drop_unique_constraint", "users", map[string]string{
					"id": "1",
				})

				// A row can be inserted into the new version of the table.
				MustInsert(t, db, schema, "02_drop_unique_constraint", "users", map[string]string{
					"id": "2",
				})

				// The newly inserted row respects the default value of the column.
				rows := MustSelect(t, db, schema, "02_drop_unique_constraint", "users")
				assert.Equal(t, []map[string]any{
					{"id": 2, "name": "anonymous"},
				}, rows)
			},
		},
		{
			name: "dropping a unique constraint preserves a foreign key constraint on the column",
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
									Name:   "department_id",
									Type:   "integer",
									Unique: true,
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
					Name: "03_drop_unique_constraint",
					Operations: migrations.Operations{
						&migrations.OpDropConstraint{
							Table: "employees",
							Name:  "employees_department_id_key",
							Up:    "department_id",
							Down:  "department_id",
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
			name: "dropping a unique constraint preserves a check constraint on the column",
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
									Nullable: true,
									Unique:   true,
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
					Name: "03_drop_unique_constraint",
					Operations: migrations.Operations{
						&migrations.OpDropConstraint{
							Table: "posts",
							Name:  "posts_title_key",
							Up:    "title",
							Down:  "title",
						},
					},
				},
			},
			afterStart: func(t *testing.T, db *sql.DB, schema string) {
				// Inserting a row that violates the check constraint should fail
				MustNotInsert(t, db, schema, "03_drop_unique_constraint", "posts", map[string]string{
					"id":    "1",
					"title": "a",
				}, testutils.CheckViolationErrorCode)
			},
			afterRollback: func(t *testing.T, db *sql.DB, schema string) {
			},
			afterComplete: func(t *testing.T, db *sql.DB, schema string) {
				// Inserting a row that violates the check constraint should fail.
				MustNotInsert(t, db, schema, "03_drop_unique_constraint", "posts", map[string]string{
					"id":    "2",
					"title": "b",
				}, testutils.CheckViolationErrorCode)
			},
		},
		{
			name: "dropping a check constraint preserves a unique constraint on the column",
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
									Nullable: true,
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
				{
					Name: "04_drop_check_constraint",
					Operations: migrations.Operations{
						&migrations.OpDropConstraint{
							Table: "posts",
							Name:  "check_title_length",
							Up:    "title",
							Down:  "title",
						},
					},
				},
			},
			afterStart: func(t *testing.T, db *sql.DB, schema string) {
				// Inserting an initial row into the `posts` table succeeds
				MustInsert(t, db, schema, "04_drop_check_constraint", "posts", map[string]string{
					"title": "post by alice",
				})

				// Inserting another row with a duplicate `title` value fails
				MustNotInsert(t, db, schema, "04_drop_check_constraint", "posts", map[string]string{
					"title": "post by alice",
				}, testutils.UniqueViolationErrorCode)
			},
			afterRollback: func(t *testing.T, db *sql.DB, schema string) {
			},
			afterComplete: func(t *testing.T, db *sql.DB, schema string) {
				// The table has a unique constraint defined on it
				UniqueConstraintMustExist(t, db, schema, "posts", "unique_title")

				// Inserting a row with a duplicate `title` value fails
				MustNotInsert(t, db, schema, "04_drop_check_constraint", "posts", map[string]string{
					"title": "post by alice",
				}, testutils.UniqueViolationErrorCode)

				// Inserting a row with a different `title` value succeeds
				MustInsert(t, db, schema, "04_drop_check_constraint", "posts", map[string]string{
					"title": "post by bob",
				})
			},
		},
		{
			name: "dropping a unique constraint preserves column not null",
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
									Unique:   true,
									Nullable: false,
								},
							},
						},
					},
				},
				{
					Name: "02_drop_unique_constraint",
					Operations: migrations.Operations{
						&migrations.OpDropConstraint{
							Table: "posts",
							Name:  "posts_title_key",
							Up:    "title",
							Down:  "title",
						},
					},
				},
			},
			afterStart: func(t *testing.T, db *sql.DB, schema string) {
				// Inserting a row that violates the NOT NULL constraint fails.
				MustNotInsert(t, db, schema, "02_drop_unique_constraint", "posts", map[string]string{
					"id": "1",
				}, testutils.NotNullViolationErrorCode)
			},
			afterRollback: func(t *testing.T, db *sql.DB, schema string) {
			},
			afterComplete: func(t *testing.T, db *sql.DB, schema string) {
				// Inserting a row that violates the NOT NULL constraint fails.
				MustNotInsert(t, db, schema, "02_drop_unique_constraint", "posts", map[string]string{
					"id": "2",
				}, testutils.NotNullViolationErrorCode)
			},
		},
		{
			name: "dropping a unique constraint preserves any comment on the column",
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
									Unique:   true,
									Nullable: false,
									Comment:  ptr("the title of the post"),
								},
							},
						},
					},
				},
				{
					Name: "02_drop_unique_constraint",
					Operations: migrations.Operations{
						&migrations.OpDropConstraint{
							Table: "posts",
							Name:  "posts_title_key",
							Up:    "title",
							Down:  "title",
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
	})
}

func TestDropConstraintValidation(t *testing.T) {
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
	addCheckMigration := migrations.Migration{
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
	}

	ExecuteTests(t, TestCases{
		{
			name: "table must exist",
			migrations: []migrations.Migration{
				createTableMigration,
				addCheckMigration,
				{
					Name: "03_drop_check_constraint",
					Operations: migrations.Operations{
						&migrations.OpDropConstraint{
							Table: "doesntexist",
							Name:  "check_title_length",
							Up:    "title",
							Down:  "SELECT CASE WHEN length(title) <= 3 THEN LPAD(title, 4, '-') ELSE title END",
						},
					},
				},
			},
			wantStartErr: migrations.TableDoesNotExistError{Name: "doesntexist"},
		},
		{
			name: "constraint must exist",
			migrations: []migrations.Migration{
				createTableMigration,
				addCheckMigration,
				{
					Name: "03_drop_check_constraint",
					Operations: migrations.Operations{
						&migrations.OpDropConstraint{
							Table: "posts",
							Name:  "doesntexist",
							Up:    "title",
							Down:  "SELECT CASE WHEN length(title) <= 3 THEN LPAD(title, 4, '-') ELSE title END",
						},
					},
				},
			},
			wantStartErr: migrations.ConstraintDoesNotExistError{Table: "posts", Constraint: "doesntexist"},
		},
		{
			name: "name is mandatory",
			migrations: []migrations.Migration{
				createTableMigration,
				addCheckMigration,
				{
					Name: "03_drop_check_constraint",
					Operations: migrations.Operations{
						&migrations.OpDropConstraint{
							Table: "posts",
							Up:    "title",
							Down:  "SELECT CASE WHEN length(title) <= 3 THEN LPAD(title, 4, '-') ELSE title END",
						},
					},
				},
			},
			wantStartErr: migrations.FieldRequiredError{Name: "name"},
		},
		{
			name: "down SQL is mandatory",
			migrations: []migrations.Migration{
				createTableMigration,
				addCheckMigration,
				{
					Name: "03_drop_check_constraint",
					Operations: migrations.Operations{
						&migrations.OpDropConstraint{
							Table: "posts",
							Name:  "check_title_length",
							Up:    "title",
						},
					},
				},
			},
			wantStartErr: migrations.FieldRequiredError{Name: "down"},
		},
	})
}
