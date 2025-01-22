// SPDX-License-Identifier: Apache-2.0

package migrations_test

import (
	"database/sql"
	"testing"

	"github.com/oapi-codegen/nullable"
	"github.com/stretchr/testify/assert"

	"github.com/xataio/pgroll/pkg/migrations"
)

func TestSetDefault(t *testing.T) {
	t.Parallel()

	ExecuteTests(t, TestCases{
		{
			name: "set column default with default up and down SQL",
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
									Name:     "name",
									Type:     "text",
									Nullable: true,
								},
							},
						},
					},
				},
				{
					Name: "02_set_default",
					Operations: migrations.Operations{
						&migrations.OpAlterColumn{
							Table:   "users",
							Column:  "name",
							Default: nullable.NewNullableWithValue("'unknown user'"),
						},
					},
				},
			},
			afterStart: func(t *testing.T, db *sql.DB, schema string) {
				// Inserting a row into the new schema succeeds
				MustInsert(t, db, schema, "02_set_default", "users", map[string]string{
					"id": "1",
				})

				// Inserting a row into the old schema succeeds
				MustInsert(t, db, schema, "01_add_table", "users", map[string]string{
					"id": "2",
				})

				// The new schema has the expected rows:
				// * The first row has a default value because it was inserted without
				//   a value into the new schema which has a default
				// * The second is NULL because it was backfilled from the old schema
				//   which does not have a default
				rows := MustSelect(t, db, schema, "02_set_default", "users")
				assert.Equal(t, []map[string]any{
					{"id": 1, "name": "unknown user"},
					{"id": 2, "name": nil},
				}, rows)

				// The old schema has the expected rows:
				// * The first row has a default value because it was backfilled from the
				//   new schema which has a default
				// * The second row is NULL because it was inserted without a value
				//   into the old schema which does not have a default
				rows = MustSelect(t, db, schema, "01_add_table", "users")
				assert.Equal(t, []map[string]any{
					{"id": 1, "name": "unknown user"},
					{"id": 2, "name": nil},
				}, rows)
			},
			afterRollback: func(t *testing.T, db *sql.DB, schema string) {
				// Inserting a row into the old schema succeeds
				MustInsert(t, db, schema, "01_add_table", "users", map[string]string{
					"id": "3",
				})

				// The old schema has the expected rows
				rows := MustSelect(t, db, schema, "01_add_table", "users")
				assert.Equal(t, []map[string]any{
					{"id": 1, "name": "unknown user"},
					{"id": 2, "name": nil},
					{"id": 3, "name": nil},
				}, rows)
			},
			afterComplete: func(t *testing.T, db *sql.DB, schema string) {
				// Inserting a row into the new schema succeeds
				MustInsert(t, db, schema, "02_set_default", "users", map[string]string{
					"id": "4",
				})

				// The new schema has the expected rows:
				// The new schema has the expected rows:
				// * The first and fourth rows have default values because they were
				//   inserted without values into the new schema which has a default
				// * The second and third are NULL because they were backfilled from
				//   the old schema which does not have a default
				rows := MustSelect(t, db, schema, "02_set_default", "users")
				assert.Equal(t, []map[string]any{
					{"id": 1, "name": "unknown user"},
					{"id": 2, "name": nil},
					{"id": 3, "name": nil},
					{"id": 4, "name": "unknown user"},
				}, rows)
			},
		},
		{
			name: "set column default with user-supplied up and down SQL",
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
									Name:     "name",
									Type:     "text",
									Nullable: true,
								},
							},
						},
					},
				},
				{
					Name: "02_set_default",
					Operations: migrations.Operations{
						&migrations.OpAlterColumn{
							Table:   "users",
							Column:  "name",
							Default: nullable.NewNullableWithValue("'unknown user'"),
							Up:      "'rewritten by up SQL'",
							Down:    "'rewritten by down SQL'",
						},
					},
				},
			},
			afterStart: func(t *testing.T, db *sql.DB, schema string) {
				// Inserting a row into the new schema succeeds
				MustInsert(t, db, schema, "02_set_default", "users", map[string]string{
					"id": "1",
				})

				// Inserting a row into the old schema succeeds
				MustInsert(t, db, schema, "01_add_table", "users", map[string]string{
					"id": "2",
				})

				// The new schema has the expected rows:
				// * The first row has a default value because it was inserted without
				//   a value into the new schema which has a default
				// * The second was rewritten because it was backfilled from the old schema
				rows := MustSelect(t, db, schema, "02_set_default", "users")
				assert.Equal(t, []map[string]any{
					{"id": 1, "name": "unknown user"},
					{"id": 2, "name": "rewritten by up SQL"},
				}, rows)

				// The old schema has the expected rows:
				// * The first row has a rewritten value because it was backfilled from the
				//   new schema.
				// * The second row is NULL because it was inserted without a value
				//   into the old schema which does not have a default
				rows = MustSelect(t, db, schema, "01_add_table", "users")
				assert.Equal(t, []map[string]any{
					{"id": 1, "name": "rewritten by down SQL"},
					{"id": 2, "name": nil},
				}, rows)
			},
			afterRollback: func(t *testing.T, db *sql.DB, schema string) {
				// Inserting a row into the old schema succeeds
				MustInsert(t, db, schema, "01_add_table", "users", map[string]string{
					"id": "3",
				})

				// The old schema has the expected rows
				rows := MustSelect(t, db, schema, "01_add_table", "users")
				assert.Equal(t, []map[string]any{
					{"id": 1, "name": "rewritten by down SQL"},
					{"id": 2, "name": nil},
					{"id": 3, "name": nil},
				}, rows)
			},
			afterComplete: func(t *testing.T, db *sql.DB, schema string) {
				// Inserting a row into the new schema succeeds
				MustInsert(t, db, schema, "02_set_default", "users", map[string]string{
					"id": "4",
				})

				// The new schema has the expected rows:
				// * The first three rows have rewritten values because they were
				//   backfilled from the old schema
				// * The fourth row has a default value because it was inserted into
				//   the new schema
				rows := MustSelect(t, db, schema, "02_set_default", "users")
				assert.Equal(t, []map[string]any{
					{"id": 1, "name": "rewritten by up SQL"},
					{"id": 2, "name": "rewritten by up SQL"},
					{"id": 3, "name": "rewritten by up SQL"},
					{"id": 4, "name": "unknown user"},
				}, rows)
			},
		},
		{
			name: "set column default: remove the default",
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
									Name:     "name",
									Type:     "text",
									Nullable: true,
									Default:  ptr("'unknown user'"),
								},
							},
						},
					},
				},
				{
					Name: "02_set_default",
					Operations: migrations.Operations{
						&migrations.OpAlterColumn{
							Table:   "users",
							Column:  "name",
							Default: nullable.NewNullableWithValue("NULL"),
						},
					},
				},
			},
			afterStart: func(t *testing.T, db *sql.DB, schema string) {
				// Inserting a row into the new schema succeeds
				MustInsert(t, db, schema, "02_set_default", "users", map[string]string{
					"id": "1",
				})

				// Inserting a row into the old schema succeeds
				MustInsert(t, db, schema, "01_add_table", "users", map[string]string{
					"id": "2",
				})

				// The new schema has the expected rows:
				// * The first row is NULL because it was inserted into the new schema which
				//   does not have a default
				// * The second has a default value because it was backfilled from the old
				//   schema which has a default
				rows := MustSelect(t, db, schema, "02_set_default", "users")
				assert.Equal(t, []map[string]any{
					{"id": 1, "name": nil},
					{"id": 2, "name": "unknown user"},
				}, rows)

				// The old schema has the expected rows:
				// * The first row is NULL because it was backfilled from the new schema
				//   which does not have a default
				// * The second row has a default value because it was inserted without
				//   a value into the old schema which has a default
				rows = MustSelect(t, db, schema, "01_add_table", "users")
				assert.Equal(t, []map[string]any{
					{"id": 1, "name": nil},
					{"id": 2, "name": "unknown user"},
				}, rows)
			},
			afterRollback: func(t *testing.T, db *sql.DB, schema string) {
				// Inserting a row into the old schema succeeds
				MustInsert(t, db, schema, "01_add_table", "users", map[string]string{
					"id": "3",
				})

				// The old schema has the expected rows
				rows := MustSelect(t, db, schema, "01_add_table", "users")
				assert.Equal(t, []map[string]any{
					{"id": 1, "name": nil},
					{"id": 2, "name": "unknown user"},
					{"id": 3, "name": "unknown user"},
				}, rows)
			},
			afterComplete: func(t *testing.T, db *sql.DB, schema string) {
				// Inserting a row into the new schema succeeds
				MustInsert(t, db, schema, "02_set_default", "users", map[string]string{
					"id": "4",
				})

				// The new schema has the expected rows:
				// * The first row is NULL because it was inserted into the new schema which
				//   does not have a default
				// * The second has a default value because it was backfilled from the old
				//   schema which has a default
				rows := MustSelect(t, db, schema, "02_set_default", "users")
				assert.Equal(t, []map[string]any{
					{"id": 1, "name": nil},
					{"id": 2, "name": "unknown user"},
					{"id": 3, "name": "unknown user"},
					{"id": 4, "name": nil},
				}, rows)
			},
		},
		{
			name: "set column default: remove the default by setting it null",
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
									Name:     "name",
									Type:     "text",
									Nullable: true,
									Default:  ptr("'unknown user'"),
								},
							},
						},
					},
				},
				{
					Name: "02_set_default",
					Operations: migrations.Operations{
						&migrations.OpAlterColumn{
							Table:   "users",
							Column:  "name",
							Default: nullable.NewNullNullable[string](),
						},
					},
				},
			},
			afterStart: func(t *testing.T, db *sql.DB, schema string) {
				// Inserting a row into the new schema succeeds
				MustInsert(t, db, schema, "02_set_default", "users", map[string]string{
					"id": "1",
				})

				// Inserting a row into the old schema succeeds
				MustInsert(t, db, schema, "01_add_table", "users", map[string]string{
					"id": "2",
				})

				// The new schema has the expected rows:
				// * The first row is NULL because it was inserted into the new schema which
				//   does not have a default
				// * The second has a default value because it was backfilled from the old
				//   schema which has a default
				rows := MustSelect(t, db, schema, "02_set_default", "users")
				assert.Equal(t, []map[string]any{
					{"id": 1, "name": nil},
					{"id": 2, "name": "unknown user"},
				}, rows)

				// The old schema has the expected rows:
				// * The first row is NULL because it was backfilled from the new schema
				//   which does not have a default
				// * The second row has a default value because it was inserted without
				//   a value into the old schema which has a default
				rows = MustSelect(t, db, schema, "01_add_table", "users")
				assert.Equal(t, []map[string]any{
					{"id": 1, "name": nil},
					{"id": 2, "name": "unknown user"},
				}, rows)
			},
			afterRollback: func(t *testing.T, db *sql.DB, schema string) {
				// Inserting a row into the old schema succeeds
				MustInsert(t, db, schema, "01_add_table", "users", map[string]string{
					"id": "3",
				})

				// The old schema has the expected rows
				rows := MustSelect(t, db, schema, "01_add_table", "users")
				assert.Equal(t, []map[string]any{
					{"id": 1, "name": nil},
					{"id": 2, "name": "unknown user"},
					{"id": 3, "name": "unknown user"},
				}, rows)
			},
			afterComplete: func(t *testing.T, db *sql.DB, schema string) {
				// Inserting a row into the new schema succeeds
				MustInsert(t, db, schema, "02_set_default", "users", map[string]string{
					"id": "4",
				})

				// The new schema has the expected rows:
				// * The first row is NULL because it was inserted into the new schema which
				//   does not have a default
				// * The second has a default value because it was backfilled from the old
				//   schema which has a default
				rows := MustSelect(t, db, schema, "02_set_default", "users")
				assert.Equal(t, []map[string]any{
					{"id": 1, "name": nil},
					{"id": 2, "name": "unknown user"},
					{"id": 3, "name": "unknown user"},
					{"id": 4, "name": nil},
				}, rows)
			},
		},
	})
}

func TestSetDefaultInMultiOperationMigrations(t *testing.T) {
	t.Parallel()

	ExecuteTests(t, TestCases{
		{
			name: "rename table, set default",
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
							Table:   "products",
							Column:  "name",
							Default: nullable.NewNullableWithValue("'unknown'"),
							Up:      "name",
							Down:    "name",
						},
					},
				},
			},
			afterStart: func(t *testing.T, db *sql.DB, schema string) {
				// Can insert a row into the new schema that uses the default
				MustInsert(t, db, schema, "02_multi_operation", "products", map[string]string{
					"id": "1",
				})

				// Can insert a row into the old schema
				MustInsert(t, db, schema, "01_create_table", "items", map[string]string{
					"id":   "2",
					"name": "apple",
				})

				// The new view has the expected rows
				rows := MustSelect(t, db, schema, "02_multi_operation", "products")
				assert.Equal(t, []map[string]any{
					{"id": 1, "name": "unknown"},
					{"id": 2, "name": "apple"},
				}, rows)

				// The old view has the expected rows
				rows = MustSelect(t, db, schema, "01_create_table", "items")
				assert.Equal(t, []map[string]any{
					{"id": 1, "name": "unknown"},
					{"id": 2, "name": "apple"},
				}, rows)
			},
			afterRollback: func(t *testing.T, db *sql.DB, schema string) {
				// The table has been cleaned up
				TableMustBeCleanedUp(t, db, schema, "items", "name")
			},
			afterComplete: func(t *testing.T, db *sql.DB, schema string) {
				// Can insert a row into the new schema that uses the default
				MustInsert(t, db, schema, "02_multi_operation", "products", map[string]string{
					"id": "3",
				})

				// The new view has the expected rows
				rows := MustSelect(t, db, schema, "02_multi_operation", "products")
				assert.Equal(t, []map[string]any{
					{"id": 1, "name": "unknown"},
					{"id": 2, "name": "apple"},
					{"id": 3, "name": "unknown"},
				}, rows)

				// The table has been cleaned up
				TableMustBeCleanedUp(t, db, schema, "products", "name")
			},
		},
	})
}
