// SPDX-License-Identifier: Apache-2.0

package migrations_test

import (
	"database/sql"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/xataio/pgroll/internal/testutils"
	"github.com/xataio/pgroll/pkg/migrations"
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
				// The table is cleaned up; temporary columns, trigger functions and triggers no longer exist.
				TableMustBeCleanedUp(t, db, schema, "posts", "user_id")
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

				// The table is cleaned up; temporary columns, trigger functions and triggers no longer exist.
				TableMustBeCleanedUp(t, db, schema, "posts", "user_id")
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
								Name:     "fk_users_id",
								Table:    "users",
								Column:   "id",
								OnDelete: migrations.ForeignKeyOnDeleteCASCADE,
							},
							Up:   "SELECT CASE WHEN EXISTS (SELECT 1 FROM users WHERE users.id = user_id) THEN user_id ELSE NULL END",
							Down: "user_id",
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
								Name:     "fk_users_id",
								Table:    "users",
								Column:   "id",
								OnDelete: migrations.ForeignKeyOnDeleteSETNULL,
							},
							Up:   "SELECT CASE WHEN EXISTS (SELECT 1 FROM users WHERE users.id = user_id) THEN user_id ELSE NULL END",
							Down: "user_id",
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
								OnDelete: migrations.ForeignKeyOnDeleteSETDEFAULT,
							},
							Up:   "SELECT CASE WHEN EXISTS (SELECT 1 FROM users WHERE users.id = user_id) THEN user_id ELSE NULL END",
							Down: "user_id",
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
							Up:   "SELECT CASE WHEN EXISTS (SELECT 1 FROM users WHERE users.id = user_id) THEN user_id ELSE NULL END",
							Down: "user_id",
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
								Name:     "fk_users_id_1",
								Table:    "users",
								Column:   "id",
								OnDelete: migrations.ForeignKeyOnDeleteCASCADE,
							},
							Up:   "SELECT CASE WHEN EXISTS (SELECT 1 FROM users WHERE users.id = user_id) THEN user_id ELSE NULL END",
							Down: "user_id",
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
							Up:   "SELECT CASE WHEN EXISTS (SELECT 1 FROM users WHERE users.id = user_id) THEN user_id ELSE NULL END",
							Down: "user_id",
						},
					},
				},
			},
			afterStart: func(t *testing.T, db *sql.DB, schema string) {
				// A temporary FK constraint has been created on the temporary column
				ValidatedForeignKeyMustExist(t, db, schema, "posts", migrations.DuplicationName("fk_users_id_1"), withOnDeleteCascade())
			},
			afterRollback: func(t *testing.T, db *sql.DB, schema string) {
			},
			afterComplete: func(t *testing.T, db *sql.DB, schema string) {
				// The foreign key constraint still exists on the column
				ValidatedForeignKeyMustExist(t, db, schema, "posts", "fk_users_id_1", withOnDeleteCascade())
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
							Up:   "SELECT CASE WHEN EXISTS (SELECT 1 FROM users WHERE users.id = user_id) THEN user_id ELSE NULL END",
							Down: "user_id",
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
									Name:     "title",
									Type:     "text",
									Nullable: true,
								},
								{
									Name:     "user_id",
									Type:     "integer",
									Nullable: false,
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
							Up:     "user_id",
							Down:   "user_id",
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
							Up:   "SELECT CASE WHEN EXISTS (SELECT 1 FROM users WHERE users.id = user_id) THEN user_id ELSE NULL END",
							Down: "user_id",
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
		{
			name: "comments are preserved when adding a foreign key constraint",
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
									Name:    "user_id",
									Type:    "integer",
									Comment: ptr("the id of the author"),
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
			},
			afterStart: func(t *testing.T, db *sql.DB, schema string) {
				// The duplicated column has a comment defined on it
				ColumnMustHaveComment(t, db, schema, "posts", migrations.TemporaryName("user_id"), "the id of the author")
			},
			afterRollback: func(t *testing.T, db *sql.DB, schema string) {
			},
			afterComplete: func(t *testing.T, db *sql.DB, schema string) {
				// The final column has a comment defined on it
				ColumnMustHaveComment(t, db, schema, "posts", "user_id", "the id of the author")
			},
		},
		{
			name: "validate that foreign key name is unique",
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
					Name: "03_add_fk_constraint_again",
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
			},
			wantStartErr: migrations.ConstraintAlreadyExistsError{
				Table:      "posts",
				Constraint: "fk_users_id",
			},
			afterStart:    func(t *testing.T, db *sql.DB, schema string) {},
			afterRollback: func(t *testing.T, db *sql.DB, schema string) {},
			afterComplete: func(t *testing.T, db *sql.DB, schema string) {},
		},
	})
}

func TestSetForeignKeyInMultiOperationMigrations(t *testing.T) {
	t.Parallel()

	ExecuteTests(t, TestCases{
		{
			name: "rename referencing table, set fk",
			migrations: []migrations.Migration{
				{
					Name: "01_create_tables",
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
									Name: "name",
									Type: "varchar(255)",
								},
								{
									Name:     "supplier_id",
									Type:     "int",
									Nullable: true,
								},
							},
						},
						&migrations.OpCreateTable{
							Name: "suppliers",
							Columns: []migrations.Column{
								{
									Name: "id",
									Type: "int",
									Pk:   true,
								},
								{
									Name: "name",
									Type: "varchar(255)",
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
							Column: "supplier_id",
							References: &migrations.ForeignKeyReference{
								Name:   "fk_products_suppliers",
								Table:  "suppliers",
								Column: "id",
							},
							Up:   "SELECT CASE WHEN EXISTS (SELECT 1 FROM suppliers WHERE suppliers.id = supplier_id) THEN supplier_id ELSE NULL END",
							Down: "supplier_id",
						},
					},
				},
			},
			afterStart: func(t *testing.T, db *sql.DB, schema string) {
				// Can insert a row into the suppliers table
				MustInsert(t, db, schema, "02_multi_operation", "suppliers", map[string]string{
					"id":   "1",
					"name": "supplier1",
				})

				// Can insert a row into the products table that meets the FK constraint
				MustInsert(t, db, schema, "02_multi_operation", "products", map[string]string{
					"id":          "1",
					"name":        "apples",
					"supplier_id": "1",
				})

				// Can't insert a row into the products table that violates the FK constraint
				MustNotInsert(t, db, schema, "02_multi_operation", "products", map[string]string{
					"id":          "2",
					"name":        "bananas",
					"supplier_id": "999",
				}, testutils.FKViolationErrorCode)

				// Can insert a row into the old schema that violates the FK constraint
				MustInsert(t, db, schema, "01_create_tables", "items", map[string]string{
					"id":          "2",
					"name":        "bananas",
					"supplier_id": "999",
				})

				// The new view has the expected rows
				rows := MustSelect(t, db, schema, "02_multi_operation", "products")
				assert.Equal(t, []map[string]any{
					{"id": 1, "name": "apples", "supplier_id": 1},
					{"id": 2, "name": "bananas", "supplier_id": nil},
				}, rows)

				// The old view has the expected rows
				rows = MustSelect(t, db, schema, "01_create_tables", "items")
				assert.Equal(t, []map[string]any{
					{"id": 1, "name": "apples", "supplier_id": 1},
					{"id": 2, "name": "bananas", "supplier_id": 999},
				}, rows)
			},
			afterRollback: func(t *testing.T, db *sql.DB, schema string) {
				// The table has been cleaned up
				TableMustBeCleanedUp(t, db, schema, "items", "name")
			},
			afterComplete: func(t *testing.T, db *sql.DB, schema string) {
				// Can insert a row into the products table that meets the FK constraint
				MustInsert(t, db, schema, "02_multi_operation", "products", map[string]string{
					"id":          "3",
					"name":        "carrots",
					"supplier_id": "1",
				})

				// Can't insert a row into the products table that violates the FK constraint
				MustNotInsert(t, db, schema, "02_multi_operation", "products", map[string]string{
					"id":          "4",
					"name":        "durian",
					"supplier_id": "999",
				}, testutils.FKViolationErrorCode)

				// The new view has the expected rows
				rows := MustSelect(t, db, schema, "02_multi_operation", "products")
				assert.Equal(t, []map[string]any{
					{"id": 1, "name": "apples", "supplier_id": 1},
					{"id": 2, "name": "bananas", "supplier_id": nil},
					{"id": 3, "name": "carrots", "supplier_id": 1},
				}, rows)
			},
		},
		{
			name: "rename referencing table, rename referencing column, set fk",
			migrations: []migrations.Migration{
				{
					Name: "01_create_tables",
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
									Name: "name",
									Type: "varchar(255)",
								},
								{
									Name:     "supplier_id",
									Type:     "int",
									Nullable: true,
								},
							},
						},
						&migrations.OpCreateTable{
							Name: "suppliers",
							Columns: []migrations.Column{
								{
									Name: "id",
									Type: "int",
									Pk:   true,
								},
								{
									Name: "name",
									Type: "varchar(255)",
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
							From:  "supplier_id",
							To:    "sup_id",
						},
						&migrations.OpAlterColumn{
							Table:  "products",
							Column: "sup_id",
							References: &migrations.ForeignKeyReference{
								Name:   "fk_products_suppliers",
								Table:  "suppliers",
								Column: "id",
							},
							Up:   "SELECT CASE WHEN EXISTS (SELECT 1 FROM suppliers WHERE suppliers.id = sup_id) THEN sup_id ELSE NULL END",
							Down: "sup_id",
						},
					},
				},
			},
			afterStart: func(t *testing.T, db *sql.DB, schema string) {
				// Can insert a row into the suppliers table
				MustInsert(t, db, schema, "02_multi_operation", "suppliers", map[string]string{
					"id":   "1",
					"name": "supplier1",
				})

				// Can insert a row into the products table that meets the FK constraint
				MustInsert(t, db, schema, "02_multi_operation", "products", map[string]string{
					"id":     "1",
					"name":   "apples",
					"sup_id": "1",
				})

				// Can't insert a row into the products table that violates the FK constraint
				MustNotInsert(t, db, schema, "02_multi_operation", "products", map[string]string{
					"id":     "2",
					"name":   "bananas",
					"sup_id": "999",
				}, testutils.FKViolationErrorCode)

				// Can insert a row into the old schema that violates the FK constraint
				MustInsert(t, db, schema, "01_create_tables", "items", map[string]string{
					"id":          "2",
					"name":        "bananas",
					"supplier_id": "999",
				})

				// The new view has the expected rows
				rows := MustSelect(t, db, schema, "02_multi_operation", "products")
				assert.Equal(t, []map[string]any{
					{"id": 1, "name": "apples", "sup_id": 1},
					{"id": 2, "name": "bananas", "sup_id": nil},
				}, rows)

				// The old view has the expected rows
				rows = MustSelect(t, db, schema, "01_create_tables", "items")
				assert.Equal(t, []map[string]any{
					{"id": 1, "name": "apples", "supplier_id": 1},
					{"id": 2, "name": "bananas", "supplier_id": 999},
				}, rows)
			},
			afterRollback: func(t *testing.T, db *sql.DB, schema string) {
				// The table has been cleaned up
				TableMustBeCleanedUp(t, db, schema, "items", "name")
			},
			afterComplete: func(t *testing.T, db *sql.DB, schema string) {
				// Can insert a row into the products table that meets the FK constraint
				MustInsert(t, db, schema, "02_multi_operation", "products", map[string]string{
					"id":     "3",
					"name":   "carrots",
					"sup_id": "1",
				})

				// Can't insert a row into the products table that violates the FK constraint
				MustNotInsert(t, db, schema, "02_multi_operation", "products", map[string]string{
					"id":     "4",
					"name":   "durian",
					"sup_id": "999",
				}, testutils.FKViolationErrorCode)

				// The new view has the expected rows
				rows := MustSelect(t, db, schema, "02_multi_operation", "products")
				assert.Equal(t, []map[string]any{
					{"id": 1, "name": "apples", "sup_id": 1},
					{"id": 2, "name": "bananas", "sup_id": nil},
					{"id": 3, "name": "carrots", "sup_id": 1},
				}, rows)
			},
		},
		{
			name: "rename referenced table, set fk",
			migrations: []migrations.Migration{
				{
					Name: "01_create_tables",
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
									Name: "name",
									Type: "varchar(255)",
								},
								{
									Name:     "supplier_id",
									Type:     "int",
									Nullable: true,
								},
							},
						},
						&migrations.OpCreateTable{
							Name: "suppliers",
							Columns: []migrations.Column{
								{
									Name: "id",
									Type: "int",
									Pk:   true,
								},
								{
									Name: "name",
									Type: "varchar(255)",
								},
							},
						},
					},
				},
				{
					Name: "02_multi_operation",
					Operations: migrations.Operations{
						&migrations.OpRenameTable{
							From: "suppliers",
							To:   "producers",
						},
						&migrations.OpAlterColumn{
							Table:  "items",
							Column: "supplier_id",
							References: &migrations.ForeignKeyReference{
								Name:   "fk_items_producers",
								Table:  "producers",
								Column: "id",
							},
							// Still have to refer to the renamed referenced table by its old name here which is not ideal
							Up:   "SELECT CASE WHEN EXISTS (SELECT 1 FROM suppliers WHERE suppliers.id = supplier_id) THEN supplier_id ELSE NULL END",
							Down: "supplier_id",
						},
					},
				},
			},
			afterStart: func(t *testing.T, db *sql.DB, schema string) {
				// Can insert a row into the suppliers table
				MustInsert(t, db, schema, "02_multi_operation", "producers", map[string]string{
					"id":   "1",
					"name": "producer1",
				})

				// Can insert a row into the products table that meets the FK constraint
				MustInsert(t, db, schema, "02_multi_operation", "items", map[string]string{
					"id":          "1",
					"name":        "apples",
					"supplier_id": "1",
				})

				// Can't insert a row into the products table that violates the FK constraint
				MustNotInsert(t, db, schema, "02_multi_operation", "items", map[string]string{
					"id":          "2",
					"name":        "bananas",
					"supplier_id": "999",
				}, testutils.FKViolationErrorCode)

				// Can insert a row into the old schema that violates the FK constraint
				MustInsert(t, db, schema, "01_create_tables", "items", map[string]string{
					"id":          "2",
					"name":        "bananas",
					"supplier_id": "999",
				})

				// The new view has the expected rows
				rows := MustSelect(t, db, schema, "02_multi_operation", "items")
				assert.Equal(t, []map[string]any{
					{"id": 1, "name": "apples", "supplier_id": 1},
					{"id": 2, "name": "bananas", "supplier_id": nil},
				}, rows)

				// The old view has the expected rows
				rows = MustSelect(t, db, schema, "01_create_tables", "items")
				assert.Equal(t, []map[string]any{
					{"id": 1, "name": "apples", "supplier_id": 1},
					{"id": 2, "name": "bananas", "supplier_id": 999},
				}, rows)
			},
			afterRollback: func(t *testing.T, db *sql.DB, schema string) {
				// The table has been cleaned up
				TableMustBeCleanedUp(t, db, schema, "items", "name")
			},
			afterComplete: func(t *testing.T, db *sql.DB, schema string) {
				// Can insert a row into the products table that meets the FK constraint
				MustInsert(t, db, schema, "02_multi_operation", "items", map[string]string{
					"id":          "3",
					"name":        "carrots",
					"supplier_id": "1",
				})

				// Can't insert a row into the products table that violates the FK constraint
				MustNotInsert(t, db, schema, "02_multi_operation", "items", map[string]string{
					"id":          "4",
					"name":        "durian",
					"supplier_id": "999",
				}, testutils.FKViolationErrorCode)

				// The new view has the expected rows
				rows := MustSelect(t, db, schema, "02_multi_operation", "items")
				assert.Equal(t, []map[string]any{
					{"id": 1, "name": "apples", "supplier_id": 1},
					{"id": 2, "name": "bananas", "supplier_id": nil},
					{"id": 3, "name": "carrots", "supplier_id": 1},
				}, rows)
			},
		},
		{
			name: "rename referenced column, set fk",
			migrations: []migrations.Migration{
				{
					Name: "01_create_tables",
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
									Name: "name",
									Type: "varchar(255)",
								},
								{
									Name:     "supplier_id",
									Type:     "int",
									Nullable: true,
								},
							},
						},
						&migrations.OpCreateTable{
							Name: "suppliers",
							Columns: []migrations.Column{
								{
									Name: "id",
									Type: "int",
									Pk:   true,
								},
								{
									Name: "name",
									Type: "varchar(255)",
								},
							},
						},
					},
				},
				{
					Name: "02_multi_operation",
					Operations: migrations.Operations{
						&migrations.OpRenameColumn{
							Table: "suppliers",
							From:  "id",
							To:    "supplier_id",
						},
						&migrations.OpAlterColumn{
							Table:  "items",
							Column: "supplier_id",
							References: &migrations.ForeignKeyReference{
								Name:   "fk_items_suppliers",
								Table:  "suppliers",
								Column: "supplier_id",
							},
							// Still have to refer to the renamed referenced column by its old name here which is not ideal
							Up:   "SELECT CASE WHEN EXISTS (SELECT 1 FROM suppliers WHERE suppliers.id = supplier_id) THEN supplier_id ELSE NULL END",
							Down: "supplier_id",
						},
					},
				},
			},
			afterStart: func(t *testing.T, db *sql.DB, schema string) {
				// Can insert a row into the suppliers table
				MustInsert(t, db, schema, "02_multi_operation", "suppliers", map[string]string{
					"supplier_id": "1",
					"name":        "producer1",
				})

				// Can insert a row into the products table that meets the FK constraint
				MustInsert(t, db, schema, "02_multi_operation", "items", map[string]string{
					"id":          "1",
					"name":        "apples",
					"supplier_id": "1",
				})

				// Can't insert a row into the products table that violates the FK constraint
				MustNotInsert(t, db, schema, "02_multi_operation", "items", map[string]string{
					"id":          "2",
					"name":        "bananas",
					"supplier_id": "999",
				}, testutils.FKViolationErrorCode)

				// Can insert a row into the old schema that violates the FK constraint
				MustInsert(t, db, schema, "01_create_tables", "items", map[string]string{
					"id":          "2",
					"name":        "bananas",
					"supplier_id": "999",
				})

				// The new view has the expected rows
				rows := MustSelect(t, db, schema, "02_multi_operation", "items")
				assert.Equal(t, []map[string]any{
					{"id": 1, "name": "apples", "supplier_id": 1},
					{"id": 2, "name": "bananas", "supplier_id": nil},
				}, rows)

				// The old view has the expected rows
				rows = MustSelect(t, db, schema, "01_create_tables", "items")
				assert.Equal(t, []map[string]any{
					{"id": 1, "name": "apples", "supplier_id": 1},
					{"id": 2, "name": "bananas", "supplier_id": 999},
				}, rows)
			},
			afterRollback: func(t *testing.T, db *sql.DB, schema string) {
				// The table has been cleaned up
				TableMustBeCleanedUp(t, db, schema, "items", "name")
			},
			afterComplete: func(t *testing.T, db *sql.DB, schema string) {
				// Can insert a row into the products table that meets the FK constraint
				MustInsert(t, db, schema, "02_multi_operation", "items", map[string]string{
					"id":          "3",
					"name":        "carrots",
					"supplier_id": "1",
				})

				// Can't insert a row into the products table that violates the FK constraint
				MustNotInsert(t, db, schema, "02_multi_operation", "items", map[string]string{
					"id":          "4",
					"name":        "durian",
					"supplier_id": "999",
				}, testutils.FKViolationErrorCode)

				// The new view has the expected rows
				rows := MustSelect(t, db, schema, "02_multi_operation", "items")
				assert.Equal(t, []map[string]any{
					{"id": 1, "name": "apples", "supplier_id": 1},
					{"id": 2, "name": "bananas", "supplier_id": nil},
					{"id": 3, "name": "carrots", "supplier_id": 1},
				}, rows)
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
							Up:   "SELECT CASE WHEN EXISTS (SELECT 1 FROM users WHERE users.id = user_id) THEN user_id ELSE NULL END",
							Down: "user_id",
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
							Up:   "SELECT CASE WHEN EXISTS (SELECT 1 FROM users WHERE users.id = user_id) THEN user_id ELSE NULL END",
							Down: "user_id",
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
							Up:   "SELECT CASE WHEN EXISTS (SELECT 1 FROM users WHERE users.id = user_id) THEN user_id ELSE NULL END",
							Down: "user_id",
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
		{
			name: "on_delete must be a valid value",
			migrations: []migrations.Migration{
				createTablesMigration,
				{
					Name: "02_add_fk_constraint",
					Operations: migrations.Operations{
						&migrations.OpAlterColumn{
							Table:  "posts",
							Column: "user_id",
							References: &migrations.ForeignKeyReference{
								Name:     "fk_users_doesntexist",
								Table:    "users",
								Column:   "id",
								OnDelete: "invalid",
							},
							Up:   "SELECT CASE WHEN EXISTS (SELECT 1 FROM users WHERE users.id = user_id) THEN user_id ELSE NULL END",
							Down: "user_id",
						},
					},
				},
			},
			wantStartErr: migrations.InvalidOnDeleteSettingError{
				Name:    "fk_users_doesntexist",
				Setting: "invalid",
			},
		},
		{
			name: "on_delete can be specified as lowercase",
			migrations: []migrations.Migration{
				createTablesMigration,
				{
					Name: "02_add_fk_constraint",
					Operations: migrations.Operations{
						&migrations.OpAlterColumn{
							Table:  "posts",
							Column: "user_id",
							References: &migrations.ForeignKeyReference{
								Name:     "fk_users_doesntexist",
								Table:    "users",
								Column:   "id",
								OnDelete: migrations.ForeignKeyOnDeleteNOACTION,
							},
							Up:   "SELECT CASE WHEN EXISTS (SELECT 1 FROM users WHERE users.id = user_id) THEN user_id ELSE NULL END",
							Down: "user_id",
						},
					},
				},
			},
			wantStartErr: nil,
		},
		{
			name: "on_delete can be specified as uppercase",
			migrations: []migrations.Migration{
				createTablesMigration,
				{
					Name: "02_add_fk_constraint",
					Operations: migrations.Operations{
						&migrations.OpAlterColumn{
							Table:  "posts",
							Column: "user_id",
							References: &migrations.ForeignKeyReference{
								Name:     "fk_users_doesntexist",
								Table:    "users",
								Column:   "id",
								OnDelete: migrations.ForeignKeyOnDeleteSETNULL,
							},
							Up:   "SELECT CASE WHEN EXISTS (SELECT 1 FROM users WHERE users.id = user_id) THEN user_id ELSE NULL END",
							Down: "user_id",
						},
					},
				},
			},
			wantStartErr: nil,
		},
	})
}
