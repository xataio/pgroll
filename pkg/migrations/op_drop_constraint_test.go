// SPDX-License-Identifier: Apache-2.0

package migrations_test

import (
	"database/sql"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/xataio/pgroll/pkg/migrations"
	"github.com/xataio/pgroll/pkg/testutils"
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
							Up:   "(SELECT CASE WHEN length(title) <= 3 THEN LPAD(title, 4, '-') ELSE title END)",
							Down: "title",
						},
					},
				},
				{
					Name: "03_drop_check_constraint",
					Operations: migrations.Operations{
						&migrations.OpDropConstraint{
							Table:  "posts",
							Column: "title",
							Name:   "check_title_length",
							Down:   "(SELECT CASE WHEN length(title) <= 3 THEN LPAD(title, 4, '-') ELSE title END)",
						},
					},
				},
			},
			afterStart: func(t *testing.T, db *sql.DB) {
				// The new (temporary) `title` column should exist on the underlying table.
				ColumnMustExist(t, db, "public", "posts", migrations.TemporaryName("title"))

				// Inserting a row that meets the check constraint into the old view works.
				MustInsert(t, db, "public", "02_add_check_constraint", "posts", map[string]string{
					"title": "post by alice",
				})

				// Inserting a row that does not meet the check constraint into the old view fails.
				MustNotInsert(t, db, "public", "02_add_check_constraint", "posts", map[string]string{
					"title": "b",
				}, testutils.CheckViolationErrorCode)

				// The inserted row has been backfilled into the new view.
				rows := MustSelect(t, db, "public", "03_drop_check_constraint", "posts")
				assert.Equal(t, []map[string]any{
					{"id": 1, "title": "post by alice"},
				}, rows)

				// Inserting a row that meets the check constraint into the new view works.
				MustInsert(t, db, "public", "03_drop_check_constraint", "posts", map[string]string{
					"title": "post by carl",
				})

				// Inserting a row that does not meet the check constraint into the new view also works.
				MustInsert(t, db, "public", "03_drop_check_constraint", "posts", map[string]string{
					"title": "d",
				})

				// Both rows that were inserted into the new view have been backfilled
				// into the old view. The short `title` value has been rewritten to meet the
				// check constraint present on the old view.
				rows = MustSelect(t, db, "public", "02_add_check_constraint", "posts")
				assert.Equal(t, []map[string]any{
					{"id": 1, "title": "post by alice"},
					{"id": 3, "title": "post by carl"},
					{"id": 4, "title": "---d"},
				}, rows)
			},
			afterRollback: func(t *testing.T, db *sql.DB) {
				// The new (temporary) `title` column should not exist on the underlying table.
				ColumnMustNotExist(t, db, "public", "posts", migrations.TemporaryName("title"))

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
				// Inserting a row that does not meet the check constraint into the new view works.
				MustInsert(t, db, "public", "03_drop_check_constraint", "posts", map[string]string{
					"title": "e",
				})

				// The data in the new `posts` view is as expected.
				rows := MustSelect(t, db, "public", "03_drop_check_constraint", "posts")
				assert.Equal(t, []map[string]any{
					{"id": 1, "title": "post by alice"},
					{"id": 3, "title": "post by carl"},
					{"id": 4, "title": "---d"},
					{"id": 5, "title": "e"},
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
							Up:   "(SELECT CASE WHEN length(title) <= 3 THEN LPAD(title, 4, '-') ELSE title END)",
							Down: "title",
						},
					},
				},
				{
					Name: "03_drop_check_constraint",
					Operations: migrations.Operations{
						&migrations.OpDropConstraint{
							Table:  "posts",
							Column: "title",
							Name:   "check_title_length",
							Up:     "title || '!'",
							Down:   "(SELECT CASE WHEN length(title) <= 3 THEN LPAD(title, 4, '-') ELSE title END)",
						},
					},
				},
			},
			afterStart: func(t *testing.T, db *sql.DB) {
				// Inserting a row that meets the check constraint into the old view works.
				MustInsert(t, db, "public", "02_add_check_constraint", "posts", map[string]string{
					"title": "post by alice",
				})
				// The inserted row has been backfilled into the new view, using the user-supplied `up` SQL.
				rows := MustSelect(t, db, "public", "03_drop_check_constraint", "posts")
				assert.Equal(t, []map[string]any{
					{"id": 1, "title": "post by alice!"},
				}, rows)
			},
			afterRollback: func(t *testing.T, db *sql.DB) {
			},
			afterComplete: func(t *testing.T, db *sql.DB) {
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
							Up:   "(SELECT CASE WHEN EXISTS (SELECT 1 FROM users WHERE users.id = user_id) THEN user_id ELSE NULL END)",
							Down: "user_id",
						},
					},
				},
				{
					Name: "03_drop_fk_constraint",
					Operations: migrations.Operations{
						&migrations.OpDropConstraint{
							Table:  "posts",
							Column: "user_id",
							Name:   "fk_users_id",
							Up:     "user_id",
							Down:   "(SELECT CASE WHEN EXISTS (SELECT 1 FROM users WHERE users.id = user_id) THEN user_id ELSE NULL END)",
						},
					},
				},
			},
			afterStart: func(t *testing.T, db *sql.DB) {
				// The new (temporary) `user_id` column should exist on the underlying table.
				ColumnMustExist(t, db, "public", "posts", migrations.TemporaryName("user_id"))

				// Inserting some data into the `users` table works.
				MustInsert(t, db, "public", "03_drop_fk_constraint", "users", map[string]string{
					"name": "alice",
				})
				MustInsert(t, db, "public", "03_drop_fk_constraint", "users", map[string]string{
					"name": "bob",
				})

				// Inserting data into the new `posts` view with a valid user reference works.
				MustInsert(t, db, "public", "03_drop_fk_constraint", "posts", map[string]string{
					"title":   "post by alice",
					"user_id": "1",
				})

				// Inserting data into the new `posts` view with an invalid user reference also works.
				MustInsert(t, db, "public", "03_drop_fk_constraint", "posts", map[string]string{
					"title":   "post by unknown user",
					"user_id": "3",
				})

				// The inserted rows have been backfilled into the old view.
				// The invalid user reference has been rewritten to NULL.
				rows := MustSelect(t, db, "public", "02_add_fk_constraint", "posts")
				assert.Equal(t, []map[string]any{
					{"id": 1, "title": "post by alice", "user_id": 1},
					{"id": 2, "title": "post by unknown user", "user_id": nil},
				}, rows)

				// Inserting data into the old `posts` view with a valid user reference works.
				MustInsert(t, db, "public", "02_add_fk_constraint", "posts", map[string]string{
					"title":   "post by bob",
					"user_id": "2",
				})

				// Inserting data into the old `posts` view with an invalid user reference fails.
				MustNotInsert(t, db, "public", "02_add_fk_constraint", "posts", map[string]string{
					"title":   "post by unknown user",
					"user_id": "3",
				}, testutils.FKViolationErrorCode)

				// The post that was inserted successfully has been backfilled into the new view.
				rows = MustSelect(t, db, "public", "03_drop_fk_constraint", "posts")
				assert.Equal(t, []map[string]any{
					{"id": 1, "title": "post by alice", "user_id": 1},
					{"id": 2, "title": "post by unknown user", "user_id": 3},
					{"id": 3, "title": "post by bob", "user_id": 2},
				}, rows)
			},
			afterRollback: func(t *testing.T, db *sql.DB) {
				// The new (temporary) `user_id` column should not exist on the underlying table.
				ColumnMustNotExist(t, db, "public", "posts", migrations.TemporaryName("user_id"))

				// The up function no longer exists.
				FunctionMustNotExist(t, db, "public", migrations.TriggerFunctionName("posts", "user_id"))
				// The down function no longer exists.
				FunctionMustNotExist(t, db, "public", migrations.TriggerFunctionName("posts", migrations.TemporaryName("user_id")))

				// The up trigger no longer exists.
				TriggerMustNotExist(t, db, "public", "posts", migrations.TriggerName("posts", "user_id"))
				// The down trigger no longer exists.
				TriggerMustNotExist(t, db, "public", "posts", migrations.TriggerName("posts", migrations.TemporaryName("user_id")))
			},
			afterComplete: func(t *testing.T, db *sql.DB) {
				// The new (temporary) `user_id` column should not exist on the underlying table.
				ColumnMustNotExist(t, db, "public", "posts", migrations.TemporaryName("user_id"))

				// Inserting a row that does not meet the check constraint into the new view works.
				MustInsert(t, db, "public", "03_drop_fk_constraint", "posts", map[string]string{
					"title":   "another post by an unknown user",
					"user_id": "4",
				})

				// The data in the new `posts` view is as expected.
				rows := MustSelect(t, db, "public", "03_drop_fk_constraint", "posts")
				assert.Equal(t, []map[string]any{
					{"id": 1, "title": "post by alice", "user_id": 1},
					{"id": 2, "title": "post by unknown user", "user_id": nil},
					{"id": 3, "title": "post by bob", "user_id": 2},
					{"id": 5, "title": "another post by an unknown user", "user_id": 4},
				}, rows)

				// The up function no longer exists.
				FunctionMustNotExist(t, db, "public", migrations.TriggerFunctionName("posts", "user_id"))
				// The down function no longer exists.
				FunctionMustNotExist(t, db, "public", migrations.TriggerFunctionName("posts", migrations.TemporaryName("user_id")))

				// The up trigger no longer exists.
				TriggerMustNotExist(t, db, "public", "posts", migrations.TriggerName("posts", "user_id"))
				// The down trigger no longer exists.
				TriggerMustNotExist(t, db, "public", "posts", migrations.TriggerName("posts", migrations.TemporaryName("user_id")))
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
							Table:  "users",
							Column: "name",
							Name:   "_pgroll_new_users_name_key",
							Up:     "name",
							Down:   "name || '-' || (random()*1000000)::integer",
						},
					},
				},
			},
			afterStart: func(t *testing.T, db *sql.DB) {
				// The new (temporary) `name` column should exist on the underlying table.
				ColumnMustExist(t, db, "public", "users", migrations.TemporaryName("name"))

				// Inserting a row that meets the unique constraint into the old view works.
				MustInsert(t, db, "public", "01_add_tables", "users", map[string]string{
					"name": "alice",
				})

				// Inserting a row that does not meet the unique constraint into the old view fails.
				MustNotInsert(t, db, "public", "01_add_tables", "users", map[string]string{
					"name": "alice",
				}, testutils.UniqueViolationErrorCode)

				// Inserting a row that does not meet the unique constraint into the new view works.
				MustInsert(t, db, "public", "02_drop_unique_constraint", "users", map[string]string{
					"name": "alice",
				})
			},
			afterRollback: func(t *testing.T, db *sql.DB) {
				// The new (temporary) `name` column should not exist on the underlying table.
				ColumnMustNotExist(t, db, "public", "users", migrations.TemporaryName("name"))

				// The up function no longer exists.
				FunctionMustNotExist(t, db, "public", migrations.TriggerFunctionName("users", "name"))
				// The down function no longer exists.
				FunctionMustNotExist(t, db, "public", migrations.TriggerFunctionName("users", migrations.TemporaryName("name")))

				// The up trigger no longer exists.
				TriggerMustNotExist(t, db, "public", "users", migrations.TriggerName("users", "name"))
				// The down trigger no longer exists.
				TriggerMustNotExist(t, db, "public", "users", migrations.TriggerName("users", migrations.TemporaryName("name")))
			},
			afterComplete: func(t *testing.T, db *sql.DB) {
				// The new (temporary) `name` column should not exist on the underlying table.
				ColumnMustNotExist(t, db, "public", "users", migrations.TemporaryName("name"))

				// Inserting a row that does not meet the unique constraint into the new view works.
				MustInsert(t, db, "public", "02_drop_unique_constraint", "users", map[string]string{
					"name": "alice",
				})
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
				Up:   "(SELECT CASE WHEN length(title) <= 3 THEN LPAD(title, 4, '-') ELSE title END)",
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
							Table:  "doesntexist",
							Column: "title",
							Name:   "check_title_length",
							Up:     "title",
							Down:   "(SELECT CASE WHEN length(title) <= 3 THEN LPAD(title, 4, '-') ELSE title END)",
						},
					},
				},
			},
			wantStartErr: migrations.TableDoesNotExistError{Name: "doesntexist"},
		},
		{
			name: "column must exist",
			migrations: []migrations.Migration{
				createTableMigration,
				addCheckMigration,
				{
					Name: "03_drop_check_constraint",
					Operations: migrations.Operations{
						&migrations.OpDropConstraint{
							Table:  "posts",
							Column: "doesntexist",
							Name:   "check_title_length",
							Up:     "title",
							Down:   "(SELECT CASE WHEN length(title) <= 3 THEN LPAD(title, 4, '-') ELSE title END)",
						},
					},
				},
			},
			wantStartErr: migrations.ColumnDoesNotExistError{Table: "posts", Name: "doesntexist"},
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
							Table:  "posts",
							Column: "title",
							Name:   "doesntexist",
							Up:     "title",
							Down:   "(SELECT CASE WHEN length(title) <= 3 THEN LPAD(title, 4, '-') ELSE title END)",
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
							Table:  "posts",
							Column: "title",
							Up:     "title",
							Down:   "(SELECT CASE WHEN length(title) <= 3 THEN LPAD(title, 4, '-') ELSE title END)",
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
							Table:  "posts",
							Column: "title",
							Name:   "check_title_length",
							Up:     "title",
						},
					},
				},
			},
			wantStartErr: migrations.FieldRequiredError{Name: "down"},
		},
	})
}
