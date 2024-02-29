// SPDX-License-Identifier: Apache-2.0

package migrations_test

import (
	"database/sql"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/xataio/pgroll/pkg/migrations"
	"github.com/xataio/pgroll/pkg/testutils"
)

func TestSetForeignKey(t *testing.T) {
	t.Parallel()

	ExecuteTests(t, TestCases{
		{
			name: "add foreign key constraint",
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
									Pk:   ptr(true),
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
									Pk:   ptr(true),
								},
								{
									Name: "title",
									Type: "text",
								},
								{
									Name:     "user_id",
									Type:     "integer",
									Nullable: ptr(true),
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
							Up:   ptr("(SELECT CASE WHEN EXISTS (SELECT 1 FROM users WHERE users.id = user_id) THEN user_id ELSE NULL END)"),
							Down: ptr("user_id"),
						},
					},
				},
			},
			afterStart: func(t *testing.T, db *sql.DB, schema string) {
				// The new (temporary) `user_id` column should exist on the underlying table.
				ColumnMustExist(t, db, schema, "posts", migrations.TemporaryName("user_id"))

				// A temporary FK constraint has been created on the temporary column
				NotValidatedForeignKeyMustExist(t, db, schema, "posts", "fk_users_id")

				// Inserting some data into the `users` table works.
				MustInsert(t, db, schema, "02_add_fk_constraint", "users", map[string]string{
					"name": "alice",
				})
				MustInsert(t, db, schema, "02_add_fk_constraint", "users", map[string]string{
					"name": "bob",
				})

				// Inserting data into the new `posts` view with a valid user reference works.
				MustInsert(t, db, schema, "02_add_fk_constraint", "posts", map[string]string{
					"title":   "post by alice",
					"user_id": "1",
				})

				// Inserting data into the new `posts` view with an invalid user reference fails.
				MustNotInsert(t, db, schema, "02_add_fk_constraint", "posts", map[string]string{
					"title":   "post by unknown user",
					"user_id": "3",
				}, testutils.FKViolationErrorCode)

				// The post that was inserted successfully has been backfilled into the old view.
				rows := MustSelect(t, db, schema, "01_add_tables", "posts")
				assert.Equal(t, []map[string]any{
					{"id": 1, "title": "post by alice", "user_id": 1},
				}, rows)

				// Inserting data into the old `posts` view with a valid user reference works.
				MustInsert(t, db, schema, "01_add_tables", "posts", map[string]string{
					"title":   "post by bob",
					"user_id": "2",
				})

				// Inserting data into the old `posts` view with an invalid user reference also works.
				MustInsert(t, db, schema, "01_add_tables", "posts", map[string]string{
					"title":   "post by unknown user",
					"user_id": "3",
				})

				// The post that was inserted successfully has been backfilled into the new view.
				// The post by an unknown user has been backfilled with a NULL user_id.
				rows = MustSelect(t, db, schema, "02_add_fk_constraint", "posts")
				assert.Equal(t, []map[string]any{
					{"id": 1, "title": "post by alice", "user_id": 1},
					{"id": 3, "title": "post by bob", "user_id": 2},
					{"id": 4, "title": "post by unknown user", "user_id": nil},
				}, rows)
			},
			afterRollback: func(t *testing.T, db *sql.DB, schema string) {
				// The new (temporary) `user_id` column should not exist on the underlying table.
				ColumnMustNotExist(t, db, schema, "posts", migrations.TemporaryName("user_id"))

				// The up function no longer exists.
				FunctionMustNotExist(t, db, schema, migrations.TriggerFunctionName("posts", "user_id"))
				// The down function no longer exists.
				FunctionMustNotExist(t, db, schema, migrations.TriggerFunctionName("posts", migrations.TemporaryName("user_id")))

				// The up trigger no longer exists.
				TriggerMustNotExist(t, db, schema, "posts", migrations.TriggerName("posts", "user_id"))
				// The down trigger no longer exists.
				TriggerMustNotExist(t, db, schema, "posts", migrations.TriggerName("posts", migrations.TemporaryName("user_id")))
			},
			afterComplete: func(t *testing.T, db *sql.DB, schema string) {
				// The new (temporary) `user_id` column should not exist on the underlying table.
				ColumnMustNotExist(t, db, schema, "posts", migrations.TemporaryName("user_id"))

				// A validated foreign key constraint exists on the underlying table.
				ValidatedForeignKeyMustExist(t, db, schema, "posts", "fk_users_id")

				// Inserting data into the new `posts` view with a valid user reference works.
				MustInsert(t, db, schema, "02_add_fk_constraint", "posts", map[string]string{
					"title":   "another post by alice",
					"user_id": "1",
				})

				// Inserting data into the new `posts` view with an invalid user reference fails.
				MustNotInsert(t, db, schema, "02_add_fk_constraint", "posts", map[string]string{
					"title":   "post by unknown user",
					"user_id": "3",
				}, testutils.FKViolationErrorCode)

				// The data in the new `posts` view is as expected.
				rows := MustSelect(t, db, schema, "02_add_fk_constraint", "posts")
				assert.Equal(t, []map[string]any{
					{"id": 1, "title": "post by alice", "user_id": 1},
					{"id": 3, "title": "post by bob", "user_id": 2},
					{"id": 4, "title": "post by unknown user", "user_id": nil},
					{"id": 5, "title": "another post by alice", "user_id": 1},
				}, rows)

				// The up function no longer exists.
				FunctionMustNotExist(t, db, schema, migrations.TriggerFunctionName("posts", "user_id"))
				// The down function no longer exists.
				FunctionMustNotExist(t, db, schema, migrations.TriggerFunctionName("posts", migrations.TemporaryName("user_id")))

				// The up trigger no longer exists.
				TriggerMustNotExist(t, db, schema, "posts", migrations.TriggerName("posts", "user_id"))
				// The down trigger no longer exists.
				TriggerMustNotExist(t, db, schema, "posts", migrations.TriggerName("posts", migrations.TemporaryName("user_id")))
			},
		},
		{
			name: "on delete NO ACTION is the default behavior when removing referenced rows",
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
									Pk:   ptr(true),
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
									Pk:   ptr(true),
								},
								{
									Name: "title",
									Type: "text",
								},
								{
									Name:     "user_id",
									Type:     "integer",
									Nullable: ptr(true),
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
							Up:   ptr("(SELECT CASE WHEN EXISTS (SELECT 1 FROM users WHERE users.id = user_id) THEN user_id ELSE NULL END)"),
							Down: ptr("user_id"),
						},
					},
				},
			},
			afterStart: func(t *testing.T, db *sql.DB, schema string) {
				// Inserting some data into the `users` table works.
				MustInsert(t, db, schema, "02_add_fk_constraint", "users", map[string]string{
					"name": "alice",
				})
				MustInsert(t, db, schema, "02_add_fk_constraint", "users", map[string]string{
					"name": "bob",
				})

				// Inserting data into the new `posts` view with a valid user reference works.
				MustInsert(t, db, schema, "02_add_fk_constraint", "posts", map[string]string{
					"title":   "post by alice",
					"user_id": "1",
				})

				// Attempting to delete a row from the `users` table that is referenced
				// by a row in the `posts` table fails.
				MustNotDelete(t, db, schema, "02_add_fk_constraint", "users", map[string]string{
					"name": "alice",
				}, testutils.FKViolationErrorCode)
			},
			afterRollback: func(t *testing.T, db *sql.DB, schema string) {
			},
			afterComplete: func(t *testing.T, db *sql.DB, schema string) {
				// Attempting to delete a row from the `users` table that is referenced
				// by a row in the `posts` table fails.
				MustNotDelete(t, db, schema, "02_add_fk_constraint", "users", map[string]string{
					"name": "alice",
				}, testutils.FKViolationErrorCode)
			},
		},
		{
			name: "on delete CASCADE allows referenced rows to be removed",
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
									Pk:   ptr(true),
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
									Pk:   ptr(true),
								},
								{
									Name: "title",
									Type: "text",
								},
								{
									Name:     "user_id",
									Type:     "integer",
									Nullable: ptr(true),
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
								Name:     "fk_users_id",
								Table:    "users",
								Column:   "id",
								OnDelete: migrations.ForeignKeyReferenceOnDeleteCASCADE,
							},
							Up:   ptr("(SELECT CASE WHEN EXISTS (SELECT 1 FROM users WHERE users.id = user_id) THEN user_id ELSE NULL END)"),
							Down: ptr("user_id"),
						},
					},
				},
			},
			afterStart: func(t *testing.T, db *sql.DB, schema string) {
				// Inserting some data into the `users` table works.
				MustInsert(t, db, schema, "02_add_fk_constraint", "users", map[string]string{
					"name": "alice",
				})
				MustInsert(t, db, schema, "02_add_fk_constraint", "users", map[string]string{
					"name": "bob",
				})

				// Inserting data into the new `posts` view with a valid user reference works.
				MustInsert(t, db, schema, "02_add_fk_constraint", "posts", map[string]string{
					"title":   "post by alice",
					"user_id": "1",
				})

				// Attempting to delete a row from the `users` table that is referenced
				// by a row in the `posts` table succeeds.
				MustDelete(t, db, schema, "02_add_fk_constraint", "users", map[string]string{
					"name": "alice",
				})

				// The row in the `posts` table that referenced the deleted row in the
				// `users` table has been removed.
				rows := MustSelect(t, db, schema, "02_add_fk_constraint", "posts")
				assert.Empty(t, rows)
			},
			afterRollback: func(t *testing.T, db *sql.DB, schema string) {
			},
			afterComplete: func(t *testing.T, db *sql.DB, schema string) {
				// Inserting data into the new `posts` view with a valid user reference works.
				MustInsert(t, db, schema, "02_add_fk_constraint", "posts", map[string]string{
					"title":   "post by bob",
					"user_id": "2",
				})

				// Attempting to delete a row from the `users` table that is referenced
				// by a row in the `posts` table succeeds.
				MustDelete(t, db, schema, "02_add_fk_constraint", "users", map[string]string{
					"name": "bob",
				})

				// The row in the `posts` table that referenced the deleted row in the
				// `users` table has been removed.
				rows := MustSelect(t, db, schema, "02_add_fk_constraint", "posts")
				assert.Empty(t, rows)
			},
		},
		{
			name: "on delete SET NULL allows referenced rows to be removed",
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
									Pk:   ptr(true),
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
									Pk:   ptr(true),
								},
								{
									Name: "title",
									Type: "text",
								},
								{
									Name:     "user_id",
									Type:     "integer",
									Nullable: ptr(true),
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
								Name:     "fk_users_id",
								Table:    "users",
								Column:   "id",
								OnDelete: migrations.ForeignKeyReferenceOnDeleteSETNULL,
							},
							Up:   ptr("(SELECT CASE WHEN EXISTS (SELECT 1 FROM users WHERE users.id = user_id) THEN user_id ELSE NULL END)"),
							Down: ptr("user_id"),
						},
					},
				},
			},
			afterStart: func(t *testing.T, db *sql.DB, schema string) {
				// Inserting some data into the `users` table works.
				MustInsert(t, db, schema, "02_add_fk_constraint", "users", map[string]string{
					"name": "alice",
				})
				MustInsert(t, db, schema, "02_add_fk_constraint", "users", map[string]string{
					"name": "bob",
				})

				// Inserting data into the new `posts` view with a valid user reference works.
				MustInsert(t, db, schema, "02_add_fk_constraint", "posts", map[string]string{
					"title":   "post by alice",
					"user_id": "1",
				})

				// Attempting to delete a row from the `users` table that is referenced
				// by a row in the `posts` table succeeds.
				MustDelete(t, db, schema, "02_add_fk_constraint", "users", map[string]string{
					"name": "alice",
				})

				// The user_id of the deleted row in the `posts` table is set to NULL.
				rows := MustSelect(t, db, schema, "02_add_fk_constraint", "posts")
				assert.Equal(t, []map[string]any{
					{"id": 1, "title": "post by alice", "user_id": nil},
				}, rows)
			},
			afterRollback: func(t *testing.T, db *sql.DB, schema string) {
			},
			afterComplete: func(t *testing.T, db *sql.DB, schema string) {
				// Inserting data into the new `posts` view with a valid user reference works.
				MustInsert(t, db, schema, "02_add_fk_constraint", "posts", map[string]string{
					"title":   "post by bob",
					"user_id": "2",
				})

				// Attempting to delete a row from the `users` table that is referenced
				// by a row in the `posts` table succeeds.
				MustDelete(t, db, schema, "02_add_fk_constraint", "users", map[string]string{
					"name": "bob",
				})

				// The user_id of the deleted row in the `posts` table is set to NULL.
				rows := MustSelect(t, db, schema, "02_add_fk_constraint", "posts")
				assert.Equal(t, []map[string]any{
					{"id": 1, "title": "post by alice", "user_id": nil},
					{"id": 2, "title": "post by bob", "user_id": nil},
				}, rows)
			},
		},
		{
			name: "on delete SET DEFAULT allows referenced rows to be removed",
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
									Pk:   ptr(true),
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
									Pk:   ptr(true),
								},
								{
									Name: "title",
									Type: "text",
								},
								{
									Name:     "user_id",
									Type:     "integer",
									Nullable: ptr(true),
									Default:  ptr("3"),
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
								Name:     "fk_users_id",
								Table:    "users",
								Column:   "id",
								OnDelete: migrations.ForeignKeyReferenceOnDeleteSETDEFAULT,
							},
							Up:   ptr("(SELECT CASE WHEN EXISTS (SELECT 1 FROM users WHERE users.id = user_id) THEN user_id ELSE NULL END)"),
							Down: ptr("user_id"),
						},
					},
				},
			},
			afterStart: func(t *testing.T, db *sql.DB, schema string) {
				// Inserting some data into the `users` table works.
				MustInsert(t, db, schema, "02_add_fk_constraint", "users", map[string]string{
					"name": "alice",
				})
				MustInsert(t, db, schema, "02_add_fk_constraint", "users", map[string]string{
					"name": "bob",
				})
				MustInsert(t, db, schema, "02_add_fk_constraint", "users", map[string]string{
					"name": "carl",
				})

				// Inserting data into the new `posts` view with a valid user reference works.
				MustInsert(t, db, schema, "02_add_fk_constraint", "posts", map[string]string{
					"title":   "post by alice",
					"user_id": "1",
				})

				// Attempting to delete a row from the `users` table that is referenced
				// by a row in the `posts` table succeeds.
				MustDelete(t, db, schema, "02_add_fk_constraint", "users", map[string]string{
					"name": "alice",
				})

				// The user_id of the deleted row in the `posts` table is set to its default value.
				rows := MustSelect(t, db, schema, "02_add_fk_constraint", "posts")
				assert.Equal(t, []map[string]any{
					{"id": 1, "title": "post by alice", "user_id": 3},
				}, rows)
			},
			afterRollback: func(t *testing.T, db *sql.DB, schema string) {
			},
			afterComplete: func(t *testing.T, db *sql.DB, schema string) {
				// Inserting data into the new `posts` view with a valid user reference works.
				MustInsert(t, db, schema, "02_add_fk_constraint", "posts", map[string]string{
					"title":   "post by bob",
					"user_id": "2",
				})

				// Attempting to delete a row from the `users` table that is referenced
				// by a row in the `posts` table succeeds.
				MustDelete(t, db, schema, "02_add_fk_constraint", "users", map[string]string{
					"name": "bob",
				})

				// The user_id of the deleted row in the `posts` table is set to its default value.
				rows := MustSelect(t, db, schema, "02_add_fk_constraint", "posts")
				assert.Equal(t, []map[string]any{
					{"id": 1, "title": "post by alice", "user_id": 3},
					{"id": 2, "title": "post by bob", "user_id": 3},
				}, rows)
			},
		},
		{
			name: "column defaults are preserved when adding a foreign key constraint",
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
									Pk:   ptr(true),
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
									Pk:   ptr(true),
								},
								{
									Name: "title",
									Type: "text",
								},
								{
									Name:    "user_id",
									Type:    "integer",
									Default: ptr("1"),
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
							Up:   ptr("(SELECT CASE WHEN EXISTS (SELECT 1 FROM users WHERE users.id = user_id) THEN user_id ELSE NULL END)"),
							Down: ptr("user_id"),
						},
					},
				},
			},
			afterStart: func(t *testing.T, db *sql.DB, schema string) {
				// Set up the users table with a reference row
				MustInsert(t, db, schema, "02_add_fk_constraint", "users", map[string]string{
					"name": "alice",
				})

				// A row can be inserted into the new version of the table.
				// The new row does not specify `user_id`, so the default value should be used.
				MustInsert(t, db, schema, "02_add_fk_constraint", "posts", map[string]string{
					"title": "post by alice",
				})

				// The newly inserted row respects the default value of the `user_id` column.
				rows := MustSelect(t, db, schema, "02_add_fk_constraint", "posts")
				assert.Equal(t, []map[string]any{
					{"id": 1, "title": "post by alice", "user_id": 1},
				}, rows)
			},
			afterRollback: func(t *testing.T, db *sql.DB, schema string) {
			},
			afterComplete: func(t *testing.T, db *sql.DB, schema string) {
				// A row can be inserted into the new version of the table.
				// The new row does not specify `user_id`, so the default value should be used.
				MustInsert(t, db, schema, "02_add_fk_constraint", "posts", map[string]string{
					"title": "another post by alice",
				})

				// The newly inserted row respects the default value of the `user_id` column.
				rows := MustSelect(t, db, schema, "02_add_fk_constraint", "posts")
				assert.Equal(t, []map[string]any{
					{"id": 1, "title": "post by alice", "user_id": 1},
					{"id": 2, "title": "another post by alice", "user_id": 1},
				}, rows)
			},
		},
		{
			name: "existing FK constraints on a column are preserved when adding a foreign key constraint",
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
									Pk:   ptr(true),
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
									Pk:   ptr(true),
								},
								{
									Name: "title",
									Type: "text",
								},
								{
									Name:    "user_id",
									Type:    "integer",
									Default: ptr("1"),
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
								Name:   "fk_users_id_1",
								Table:  "users",
								Column: "id",
							},
							Up:   ptr("(SELECT CASE WHEN EXISTS (SELECT 1 FROM users WHERE users.id = user_id) THEN user_id ELSE NULL END)"),
							Down: ptr("user_id"),
						},
					},
				},
				{
					Name: "03_add_fk_constraint",
					Operations: migrations.Operations{
						&migrations.OpAlterColumn{
							Table:  "posts",
							Column: "user_id",
							References: &migrations.ForeignKeyReference{
								Name:   "fk_users_id_2",
								Table:  "users",
								Column: "id",
							},
							Up:   ptr("(SELECT CASE WHEN EXISTS (SELECT 1 FROM users WHERE users.id = user_id) THEN user_id ELSE NULL END)"),
							Down: ptr("user_id"),
						},
					},
				},
			},
			afterStart: func(t *testing.T, db *sql.DB, schema string) {
				// A temporary FK constraint has been created on the temporary column
				ValidatedForeignKeyMustExist(t, db, schema, "posts", migrations.DuplicationName("fk_users_id_1"))
			},
			afterRollback: func(t *testing.T, db *sql.DB, schema string) {
			},
			afterComplete: func(t *testing.T, db *sql.DB, schema string) {
				// The foreign key constraint still exists on the column
				ValidatedForeignKeyMustExist(t, db, schema, "posts", "fk_users_id_1")
			},
		},
		{
			name: "check constraints on a column are preserved when adding a foreign key constraint",
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
									Pk:   ptr(true),
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
									Pk:   ptr(true),
								},
								{
									Name: "title",
									Type: "text",
									Check: &migrations.CheckConstraint{
										Name:       "title_length",
										Constraint: "length(title) > 3",
									},
								},
								{
									Name: "user_id",
									Type: "integer",
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
							Up:   ptr("(SELECT CASE WHEN EXISTS (SELECT 1 FROM users WHERE users.id = user_id) THEN user_id ELSE NULL END)"),
							Down: ptr("user_id"),
						},
					},
				},
			},
			afterStart: func(t *testing.T, db *sql.DB, schema string) {
				// Set up the users table with a reference row
				MustInsert(t, db, schema, "02_add_fk_constraint", "users", map[string]string{
					"name": "alice",
				})

				// Inserting a row that violates the check constraint should fail.
				MustNotInsert(t, db, schema, "02_add_fk_constraint", "posts", map[string]string{
					"id":      "1",
					"user_id": "1",
					"title":   "a",
				}, testutils.CheckViolationErrorCode)
			},
			afterRollback: func(t *testing.T, db *sql.DB, schema string) {
			},
			afterComplete: func(t *testing.T, db *sql.DB, schema string) {
				// Inserting a row that violates the check constraint should fail.
				MustNotInsert(t, db, schema, "02_add_fk_constraint", "posts", map[string]string{
					"id":      "2",
					"user_id": "1",
					"title":   "b",
				}, testutils.CheckViolationErrorCode)
			},
		},
		{
			name: "not null is preserved when adding a foreign key constraint",
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
									Pk:   ptr(true),
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
									Pk:   ptr(true),
								},
								{
									Name:     "title",
									Type:     "text",
									Nullable: ptr(true),
								},
								{
									Name:     "user_id",
									Type:     "integer",
									Nullable: ptr(false),
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
							Up:   ptr("(SELECT CASE WHEN EXISTS (SELECT 1 FROM users WHERE users.id = user_id) THEN user_id ELSE NULL END)"),
							Down: ptr("user_id"),
						},
					},
				},
			},
			afterStart: func(t *testing.T, db *sql.DB, schema string) {
				// Inserting a row that violates the NOT NULL constraint on `user_id` fails.
				MustNotInsert(t, db, schema, "02_add_fk_constraint", "posts", map[string]string{
					"id":    "1",
					"title": "post by alice",
				}, testutils.NotNullViolationErrorCode)
			},
			afterRollback: func(t *testing.T, db *sql.DB, schema string) {
			},
			afterComplete: func(t *testing.T, db *sql.DB, schema string) {
				// Inserting a row that violates the NOT NULL constraint on `user_id` fails.
				MustNotInsert(t, db, schema, "02_add_fk_constraint", "posts", map[string]string{
					"id":    "1",
					"title": "post by alice",
				}, testutils.NotNullViolationErrorCode)
			},
		},
		{
			name: "unique constraints are preserved when adding a foreign key constraint",
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
									Pk:   ptr(true),
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
									Pk:   ptr(true),
								},
								{
									Name: "title",
									Type: "text",
								},
								{
									Name: "user_id",
									Type: "integer",
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
							Column: "user_id",
							Unique: &migrations.UniqueConstraint{Name: "unique_user_id"},
							Up:     ptr("user_id"),
							Down:   ptr("user_id"),
						},
					},
				},
				{
					Name: "03_add_fk_constraint",
					Operations: migrations.Operations{
						&migrations.OpAlterColumn{
							Table:  "posts",
							Column: "user_id",
							References: &migrations.ForeignKeyReference{
								Name:   "fk_users_id",
								Table:  "users",
								Column: "id",
							},
							Up:   ptr("(SELECT CASE WHEN EXISTS (SELECT 1 FROM users WHERE users.id = user_id) THEN user_id ELSE NULL END)"),
							Down: ptr("user_id"),
						},
					},
				},
			},
			afterStart: func(t *testing.T, db *sql.DB, schema string) {
				// Set up the users table with a reference row
				MustInsert(t, db, schema, "03_add_fk_constraint", "users", map[string]string{
					"name": "alice",
					"id":   "1",
				})

				// Inserting an initial row succeeds
				MustInsert(t, db, schema, "03_add_fk_constraint", "posts", map[string]string{
					"title":   "post by alice",
					"user_id": "1",
				})

				// Inserting a row with a duplicate `user_id` fails.
				MustNotInsert(t, db, schema, "03_add_fk_constraint", "posts", map[string]string{
					"title":   "post by alice 2",
					"user_id": "1",
				}, testutils.UniqueViolationErrorCode)
			},
			afterRollback: func(t *testing.T, db *sql.DB, schema string) {
			},
			afterComplete: func(t *testing.T, db *sql.DB, schema string) {
				// The 'posts' table has a unique constraint defined on it
				UniqueConstraintMustExist(t, db, schema, "posts", "unique_user_id")

				// Inserting a row with a duplicate `user_id` fails
				MustNotInsert(t, db, schema, "03_add_fk_constraint", "posts", map[string]string{
					"title":   "post by alice 3",
					"user_id": "1",
				}, testutils.UniqueViolationErrorCode)

				// Set up the users table with another reference row
				MustInsert(t, db, schema, "03_add_fk_constraint", "users", map[string]string{
					"name": "bob",
					"id":   "2",
				})

				// Inserting a row with a different `user_id` succeeds
				MustInsert(t, db, schema, "03_add_fk_constraint", "posts", map[string]string{
					"title":   "post by bob",
					"user_id": "2",
				})
			},
		},
	})
}

func TestSetForeignKeyValidation(t *testing.T) {
	t.Parallel()

	createTablesMigration := migrations.Migration{
		Name: "01_add_tables",
		Operations: migrations.Operations{
			&migrations.OpCreateTable{
				Name: "users",
				Columns: []migrations.Column{
					{
						Name: "id",
						Type: "serial",
						Pk:   ptr(true),
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
						Pk:   ptr(true),
					},
					{
						Name: "title",
						Type: "text",
					},
					{
						Name: "user_id",
						Type: "integer",
					},
				},
			},
		},
	}

	ExecuteTests(t, TestCases{
		{
			name: "foreign key must have a name",
			migrations: []migrations.Migration{
				createTablesMigration,
				{
					Name: "02_add_fk_constraint",
					Operations: migrations.Operations{
						&migrations.OpAlterColumn{
							Table:  "posts",
							Column: "user_id",
							References: &migrations.ForeignKeyReference{
								Table:  "users",
								Column: "id",
							},
							Up:   ptr("(SELECT CASE WHEN EXISTS (SELECT 1 FROM users WHERE users.id = user_id) THEN user_id ELSE NULL END)"),
							Down: ptr("user_id"),
						},
					},
				},
			},
			wantStartErr: migrations.ColumnReferenceError{
				Table:  "posts",
				Column: "user_id",
				Err:    migrations.FieldRequiredError{Name: "name"},
			},
		},
		{
			name: "referenced table must exist",
			migrations: []migrations.Migration{
				createTablesMigration,
				{
					Name: "02_add_fk_constraint",
					Operations: migrations.Operations{
						&migrations.OpAlterColumn{
							Table:  "posts",
							Column: "user_id",
							References: &migrations.ForeignKeyReference{
								Name:   "fk_doesntexist_id",
								Table:  "doesntexist",
								Column: "id",
							},
							Up:   ptr("(SELECT CASE WHEN EXISTS (SELECT 1 FROM users WHERE users.id = user_id) THEN user_id ELSE NULL END)"),
							Down: ptr("user_id"),
						},
					},
				},
			},
			wantStartErr: migrations.ColumnReferenceError{
				Table:  "posts",
				Column: "user_id",
				Err:    migrations.TableDoesNotExistError{Name: "doesntexist"},
			},
		},
		{
			name: "referenced column must exist",
			migrations: []migrations.Migration{
				createTablesMigration,
				{
					Name: "02_add_fk_constraint",
					Operations: migrations.Operations{
						&migrations.OpAlterColumn{
							Table:  "posts",
							Column: "user_id",
							References: &migrations.ForeignKeyReference{
								Name:   "fk_users_doesntexist",
								Table:  "users",
								Column: "doesntexist",
							},
							Up:   ptr("(SELECT CASE WHEN EXISTS (SELECT 1 FROM users WHERE users.id = user_id) THEN user_id ELSE NULL END)"),
							Down: ptr("user_id"),
						},
					},
				},
			},
			wantStartErr: migrations.ColumnReferenceError{
				Table:  "posts",
				Column: "user_id",
				Err:    migrations.ColumnDoesNotExistError{Table: "users", Name: "doesntexist"},
			},
		},
	})
}
