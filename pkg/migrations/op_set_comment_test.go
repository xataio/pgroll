// SPDX-License-Identifier: Apache-2.0

package migrations_test

import (
	"database/sql"
	"testing"

	"github.com/oapi-codegen/nullable"
	"github.com/stretchr/testify/assert"
	"github.com/xataio/pgroll/pkg/migrations"
)

func TestSetComment(t *testing.T) {
	t.Parallel()

	ExecuteTests(t, TestCases{
		{
			name: "set column comment with default up and down SQL",
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
									Pk:   ptr(true),
								},
								{
									Name:    "name",
									Type:    "text",
									Comment: ptr("apples"),
								},
							},
						},
					},
				},
				{
					Name: "02_set_comment",
					Operations: migrations.Operations{
						&migrations.OpAlterColumn{
							Table:   "users",
							Column:  "name",
							Comment: nullable.NewNullableWithValue("name of the user"),
						},
					},
				},
			},
			afterStart: func(t *testing.T, db *sql.DB, schema string) {
				// Inserting a row into the new schema succeeds
				MustInsert(t, db, schema, "02_set_comment", "users", map[string]string{
					"name": "alice",
				})

				// Inserting a row into the old schema succeeds
				MustInsert(t, db, schema, "01_add_table", "users", map[string]string{
					"name": "bob",
				})

				// The old column should have the old comment.
				ColumnMustHaveComment(t, db, schema, "users", "name", "apples")

				// The new column should have the new comment.
				ColumnMustHaveComment(t, db, schema, "users", migrations.TemporaryName("name"), "name of the user")

				// The old schema view has the expected rows
				rows := MustSelect(t, db, schema, "01_add_table", "users")
				assert.Equal(t, []map[string]any{
					{"id": 1, "name": "alice"},
					{"id": 2, "name": "bob"},
				}, rows)

				// The new schema view has the expected rows
				rows = MustSelect(t, db, schema, "02_set_comment", "users")
				assert.Equal(t, []map[string]any{
					{"id": 1, "name": "alice"},
					{"id": 2, "name": "bob"},
				}, rows)
			},
			afterRollback: func(t *testing.T, db *sql.DB, schema string) {
				// The column should have the old comment.
				ColumnMustHaveComment(t, db, schema, "users", "name", "apples")
			},
			afterComplete: func(t *testing.T, db *sql.DB, schema string) {
				// The new column should have the new comment.
				ColumnMustHaveComment(t, db, schema, "users", "name", "name of the user")

				// The new schema view has the expected rows
				rows := MustSelect(t, db, schema, "02_set_comment", "users")
				assert.Equal(t, []map[string]any{
					{"id": 1, "name": "alice"},
					{"id": 2, "name": "bob"},
				}, rows)
			},
		},
		{
			name: "set column comment with user-supplied up and down SQL",
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
									Pk:   ptr(true),
								},
								{
									Name:    "name",
									Type:    "text",
									Comment: ptr("apples"),
								},
							},
						},
					},
				},
				{
					Name: "02_set_comment",
					Operations: migrations.Operations{
						&migrations.OpAlterColumn{
							Table:   "users",
							Column:  "name",
							Comment: nullable.NewNullableWithValue("name of the user"),
							Up:      "'rewritten by up SQL'",
							Down:    "'rewritten by down SQL'",
						},
					},
				},
			},
			afterStart: func(t *testing.T, db *sql.DB, schema string) {
				// Inserting a row into the new schema succeeds
				MustInsert(t, db, schema, "02_set_comment", "users", map[string]string{
					"name": "alice",
				})

				// Inserting a row into the old schema succeeds
				MustInsert(t, db, schema, "01_add_table", "users", map[string]string{
					"name": "bob",
				})

				// The old column should have the old comment.
				ColumnMustHaveComment(t, db, schema, "users", "name", "apples")

				// The new column should have the new comment.
				ColumnMustHaveComment(t, db, schema, "users", migrations.TemporaryName("name"), "name of the user")

				// The old schema view has the expected rows
				rows := MustSelect(t, db, schema, "01_add_table", "users")
				assert.Equal(t, []map[string]any{
					{"id": 1, "name": "rewritten by down SQL"},
					{"id": 2, "name": "bob"},
				}, rows)

				// The new schema view has the expected rows
				rows = MustSelect(t, db, schema, "02_set_comment", "users")
				assert.Equal(t, []map[string]any{
					{"id": 1, "name": "alice"},
					{"id": 2, "name": "rewritten by up SQL"},
				}, rows)
			},
			afterRollback: func(t *testing.T, db *sql.DB, schema string) {
				// The column should have the old comment.
				ColumnMustHaveComment(t, db, schema, "users", "name", "apples")
			},
			afterComplete: func(t *testing.T, db *sql.DB, schema string) {
				// The new column should have the new comment.
				ColumnMustHaveComment(t, db, schema, "users", "name", "name of the user")

				// The new schema view has the expected rows
				rows := MustSelect(t, db, schema, "02_set_comment", "users")
				assert.Equal(t, []map[string]any{
					{"id": 1, "name": "rewritten by up SQL"},
					{"id": 2, "name": "rewritten by up SQL"},
				}, rows)
			},
		},
		{
			name: "set column comment to NULL",
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
									Pk:   ptr(true),
								},
								{
									Name:    "name",
									Type:    "text",
									Comment: ptr("apples"),
								},
							},
						},
					},
				},
				{
					Name: "02_set_comment",
					Operations: migrations.Operations{
						&migrations.OpAlterColumn{
							Table:   "users",
							Column:  "name",
							Comment: nullable.NewNullNullable[string](),
						},
					},
				},
			},
			afterStart: func(t *testing.T, db *sql.DB, schema string) {
				// The old column should have the old comment.
				ColumnMustHaveComment(t, db, schema, "users", "name", "apples")

				// The new column should have no comment.
				ColumnMustNotHaveComment(t, db, schema, "users", migrations.TemporaryName("name"))
			},
			afterRollback: func(t *testing.T, db *sql.DB, schema string) {
				// The old column should have the old comment.
				ColumnMustHaveComment(t, db, schema, "users", "name", "apples")
			},
			afterComplete: func(t *testing.T, db *sql.DB, schema string) {
				// The new column should have no comment.
				ColumnMustNotHaveComment(t, db, schema, "users", "name")
			},
		},
	})
}
