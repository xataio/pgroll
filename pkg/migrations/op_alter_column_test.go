// SPDX-License-Identifier: Apache-2.0

package migrations_test

import (
	"database/sql"
	"testing"

	"github.com/oapi-codegen/nullable"
	"github.com/stretchr/testify/assert"

	"github.com/xataio/pgroll/internal/testutils"
	"github.com/xataio/pgroll/pkg/migrations"
)

func TestAlterColumnMultipleSubOperations(t *testing.T) {
	t.Parallel()

	ExecuteTests(t, TestCases{
		{
			name: "can alter a column: set not null, change type, change comment, and add check constraint",
			migrations: []migrations.Migration{
				{
					Name:          "01_create_table",
					VersionSchema: "create_table",
					Operations: migrations.Operations{
						&migrations.OpCreateTable{
							Name: "events",
							Columns: []migrations.Column{
								{
									Name: "id",
									Type: "serial",
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
					Name:          "02_alter_column",
					VersionSchema: "alter_column",
					Operations: migrations.Operations{
						&migrations.OpAlterColumn{
							Table:    "events",
							Column:   "name",
							Up:       "SELECT CASE WHEN name IS NULL OR LENGTH(name) <= 3 THEN 'placeholder' ELSE name END",
							Down:     "name",
							Type:     ptr("text"),
							Comment:  nullable.NewNullableWithValue("the name of the event"),
							Nullable: ptr(false),
							Check: &migrations.CheckConstraint{
								Name:       "name_length",
								Constraint: "length(name) > 3",
							},
						},
					},
				},
			},
			afterStart: func(t *testing.T, db *sql.DB, schema string) {
				// Inserting a NULL into the new column should fail
				MustNotInsert(t, db, schema, "alter_column", "events", map[string]string{
					"id": "1",
				}, testutils.CheckViolationErrorCode)

				// Inserting a non-NULL value into the new column should succeed
				MustInsert(t, db, schema, "alter_column", "events", map[string]string{
					"id":   "2",
					"name": "apples",
				})

				// The value inserted into the new column has been backfilled into the
				// old column.
				rows := MustSelect(t, db, schema, "create_table", "events")
				assert.Equal(t, []map[string]any{
					{"id": 2, "name": "apples"},
				}, rows)

				// Inserting a NULL value into the old column should succeed
				MustInsert(t, db, schema, "create_table", "events", map[string]string{
					"id": "3",
				})

				// The NULL value inserted into the old column has been written into
				// the new column using the `up` SQL.
				rows = MustSelect(t, db, schema, "alter_column", "events")
				assert.Equal(t, []map[string]any{
					{"id": 2, "name": "apples"},
					{"id": 3, "name": "placeholder"},
				}, rows)

				// Inserting a non-NULL value into the old column should succeed
				MustInsert(t, db, schema, "create_table", "events", map[string]string{
					"id":   "4",
					"name": "bananas",
				})

				// The non-NULL value inserted into the old column has been copied
				// unchanged into the new column.
				rows = MustSelect(t, db, schema, "alter_column", "events")
				assert.Equal(t, []map[string]any{
					{"id": 2, "name": "apples"},
					{"id": 3, "name": "placeholder"},
					{"id": 4, "name": "bananas"},
				}, rows)

				// Inserting a row into the new column that violates the
				// check constraint should fail
				MustNotInsert(t, db, schema, "alter_column", "events", map[string]string{
					"id":   "5",
					"name": "x",
				}, testutils.CheckViolationErrorCode)

				// Inserting a row into the old column that violates the
				// check constraint should succeed.
				MustInsert(t, db, schema, "create_table", "events", map[string]string{
					"id":   "5",
					"name": "x",
				})

				// The value that didn't meet the check constraint has been rewritten
				// into the new column using the `up` SQL.
				rows = MustSelect(t, db, schema, "alter_column", "events")
				assert.Equal(t, []map[string]any{
					{"id": 2, "name": "apples"},
					{"id": 3, "name": "placeholder"},
					{"id": 4, "name": "bananas"},
					{"id": 5, "name": "placeholder"},
				}, rows)

				// The type of the new column in the underlying table should be `text`
				ColumnMustHaveType(t, db, schema, "events", migrations.TemporaryName("name"), "text")

				// The new column should have the new comment.
				ColumnMustHaveComment(t, db, schema, "events", migrations.TemporaryName("name"), "the name of the event")
			},
			afterRollback: func(t *testing.T, db *sql.DB, schema string) {
				// The new (temporary) column should not exist on the underlying table.
				ColumnMustNotExist(t, db, schema, "events", migrations.TemporaryName("name"))
			},
			afterComplete: func(t *testing.T, db *sql.DB, schema string) {
				// Inserting a NULL into the new column should fail
				MustNotInsert(t, db, schema, "alter_column", "events", map[string]string{
					"id": "6",
				}, testutils.NotNullViolationErrorCode)

				// Inserting a row into the new column that violates the
				// check constraint should fail
				MustNotInsert(t, db, schema, "alter_column", "events", map[string]string{
					"id":   "6",
					"name": "x",
				}, testutils.CheckViolationErrorCode)

				// The type of the new column in the underlying table should be `text`
				ColumnMustHaveType(t, db, schema, "events", "name", "text")

				// The new column should have the new comment.
				ColumnMustHaveComment(t, db, schema, "events", "name", "the name of the event")

				// The table contains the expected rows
				rows := MustSelect(t, db, schema, "alter_column", "events")
				assert.Equal(t, []map[string]any{
					{"id": 2, "name": "apples"},
					{"id": 3, "name": "placeholder"},
					{"id": 4, "name": "bananas"},
					{"id": 5, "name": "placeholder"},
				}, rows)
			},
		},
		{
			name: "can alter a column: set not null, and add a default",
			migrations: []migrations.Migration{
				{
					Name: "01_create_table",
					Operations: migrations.Operations{
						&migrations.OpCreateTable{
							Name: "events",
							Columns: []migrations.Column{
								{
									Name: "id",
									Type: "serial",
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
					Name: "02_alter_column",
					Operations: migrations.Operations{
						&migrations.OpAlterColumn{
							Table:    "events",
							Column:   "name",
							Nullable: ptr(false),
							Default:  nullable.NewNullableWithValue("'default'"),
							Up:       "SELECT CASE WHEN name IS NULL THEN 'rewritten by up SQL' ELSE name END",
						},
					},
				},
			},
			afterStart: func(t *testing.T, db *sql.DB, schema string) {
				// Inserting with no `name` into the new schema should succeed
				MustInsert(t, db, schema, "02_alter_column", "events", map[string]string{
					"id": "1",
				})

				// Updating name to default value should succeed
				MustUpdate(t, db, schema, "02_alter_column", "events", "id", "1", map[string]string{
					"name": "DEFAULT",
				})

				// Inserting a NULL explicitly into the new schema should fail
				MustNotInsert(t, db, schema, "02_alter_column", "events", map[string]string{
					"id":   "100",
					"name": "NULL",
				}, testutils.CheckViolationErrorCode)

				// Inserting with no `name` into the old schema should succeed
				MustInsert(t, db, schema, "01_create_table", "events", map[string]string{
					"id": "2",
				})

				// The new schema has the expected rows:
				// * The first row has a default value because it was inserted with no
				//   value into the new schema which has a default
				// * The second row was rewritten by the `up` SQL because it was
				//   inserted with no value into the old schema which has no default
				rows := MustSelect(t, db, schema, "02_alter_column", "events")
				assert.Equal(t, []map[string]any{
					{"id": 1, "name": "default"},
					{"id": 2, "name": "rewritten by up SQL"},
				}, rows)

				// The old schema has the expected rows:
				// * The first row has a default value because it was inserted with no
				//   value into the new schema which has a default and then backfilled
				//   to the old schema
				// * The second row is NULL because it was inserted with no value into
				//   the old schema which has no default
				rows = MustSelect(t, db, schema, "01_create_table", "events")
				assert.Equal(t, []map[string]any{
					{"id": 1, "name": "default"},
					{"id": 2, "name": nil},
				}, rows)
			},
			afterRollback: func(t *testing.T, db *sql.DB, schema string) {
				// Inserting with no `name` into the old schema should succeed
				MustInsert(t, db, schema, "01_create_table", "events", map[string]string{
					"id": "3",
				})

				// The old schema has the expected rows
				rows := MustSelect(t, db, schema, "01_create_table", "events")
				assert.Equal(t, []map[string]any{
					{"id": 1, "name": "default"},
					{"id": 2, "name": nil},
					{"id": 3, "name": nil},
				}, rows)
			},
			afterComplete: func(t *testing.T, db *sql.DB, schema string) {
				// Inserting with no `name` into the new schema should succeed
				MustInsert(t, db, schema, "02_alter_column", "events", map[string]string{
					"id": "4",
				})

				// Updating name to default value should succeed
				MustUpdate(t, db, schema, "02_alter_column", "events", "id", "4", map[string]string{
					"name": "DEFAULT",
				})

				// Inserting a NULL explicitly into the new schema should fail
				MustNotInsert(t, db, schema, "02_alter_column", "events", map[string]string{
					"id":   "100",
					"name": "NULL",
				}, testutils.NotNullViolationErrorCode)

				// The new schema has the expected rows:
				// * The first and fourth rows have default values because they were
				//   inserted with no value into the new schema which has a default
				// * The second and third rows were rewritten by the `up` SQL because
				//   they were inserted with no value into the old schema which has no
				//   default
				rows := MustSelect(t, db, schema, "02_alter_column", "events")
				assert.Equal(t, []map[string]any{
					{"id": 1, "name": "default"},
					{"id": 2, "name": "rewritten by up SQL"},
					{"id": 3, "name": "rewritten by up SQL"},
					{"id": 4, "name": "default"},
				}, rows)
			},
		},
		{
			name: "can alter a column: rename and add a unique constraint",
			migrations: []migrations.Migration{
				{
					Name: "01_create_table",
					Operations: migrations.Operations{
						&migrations.OpCreateTable{
							Name: "events",
							Columns: []migrations.Column{
								{
									Name: "id",
									Type: "serial",
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
					Name: "02_alter_column",
					Operations: migrations.Operations{
						&migrations.OpAlterColumn{
							Table:  "events",
							Column: "name",
							Up:     "name || '-' || random()*999::int",
							Down:   "name",
							Unique: &migrations.UniqueConstraint{
								Name: "events_name_unique",
							},
						},
					},
				},
			},
			afterStart: func(t *testing.T, db *sql.DB, schema string) {
				// Can insert a row into the new column
				MustInsert(t, db, schema, "02_alter_column", "events", map[string]string{
					"id":   "1",
					"name": "apples",
				})

				// Inserting a row with the same value into the new column fails
				MustNotInsert(t, db, schema, "02_alter_column", "events", map[string]string{
					"id":   "2",
					"name": "apples",
				}, testutils.UniqueViolationErrorCode)

				// Inserting a row with a duplicate value into the old column succeeds
				MustInsert(t, db, schema, "01_create_table", "events", map[string]string{
					"id":   "2",
					"name": "apples",
				})
			},
			afterRollback: func(t *testing.T, db *sql.DB, schema string) {
			},
			afterComplete: func(t *testing.T, db *sql.DB, schema string) {
				// Inserting a row with the same value into the new column fails
				MustNotInsert(t, db, schema, "02_alter_column", "events", map[string]string{
					"id":   "2",
					"name": "apples",
				}, testutils.UniqueViolationErrorCode)
			},
		},
		{
			name: "can alter a column: add an FK constraint and set not null",
			migrations: []migrations.Migration{
				{
					Name: "01_create_table",
					Operations: migrations.Operations{
						&migrations.OpCreateTable{
							Name: "events",
							Columns: []migrations.Column{
								{
									Name: "id",
									Type: "serial",
									Pk:   true,
								},
								{
									Name:     "name",
									Type:     "varchar(255)",
									Nullable: true,
								},
							},
						},
						&migrations.OpCreateTable{
							Name: "people",
							Columns: []migrations.Column{
								{
									Name: "id",
									Type: "serial",
									Pk:   true,
								},
								{
									Name:     "name",
									Type:     "varchar(255)",
									Nullable: true,
								},
								{
									Name:     "manages",
									Type:     "integer",
									Nullable: true,
								},
							},
						},
					},
				},
				{
					Name: "02_alter_column",
					Operations: migrations.Operations{
						&migrations.OpAlterColumn{
							Table:    "people",
							Column:   "manages",
							Up:       "SELECT CASE WHEN manages IS NULL THEN 1 ELSE manages END",
							Down:     "manages",
							Nullable: ptr(false),
							References: &migrations.ForeignKeyReference{
								Table:  "events",
								Column: "id",
								Name:   "person_manages_event_fk",
							},
						},
					},
				},
			},
			afterStart: func(t *testing.T, db *sql.DB, schema string) {
				// Can insert an initial event into the `events` table
				MustInsert(t, db, schema, "02_alter_column", "events", map[string]string{
					"id":   "1",
					"name": "event1",
				})

				// Inserting a row into the `people` table with a NULL `manages` value fails
				MustNotInsert(t, db, schema, "02_alter_column", "people", map[string]string{
					"id":   "1",
					"name": "alice",
				}, testutils.CheckViolationErrorCode)

				// Inserting a row into the `people` table with a `manages` field that
				// violates the FK constraint fails
				MustNotInsert(t, db, schema, "02_alter_column", "people", map[string]string{
					"id":      "1",
					"name":    "alice",
					"manages": "2",
				}, testutils.FKViolationErrorCode)

				// Inserting a row into the `people` table with a valid `manages` field
				// succeeds
				MustInsert(t, db, schema, "02_alter_column", "people", map[string]string{
					"id":      "1",
					"name":    "alice",
					"manages": "1",
				})

				// Inserting a row into the old version of the `people` table with a
				// NULL `manages` value succeeds
				MustInsert(t, db, schema, "01_create_table", "people", map[string]string{
					"id":   "2",
					"name": "bob",
				})

				// The version of the `people` table in the new schema has the expected rows.
				// In particular, the `manages` field has been backfilled using the
				// `up` SQL for `bob`.
				rows := MustSelect(t, db, schema, "02_alter_column", "people")
				assert.Equal(t, []map[string]any{
					{"id": 1, "name": "alice", "manages": 1},
					{"id": 2, "name": "bob", "manages": 1},
				}, rows)
			},
			afterRollback: func(t *testing.T, db *sql.DB, schema string) {
			},
			afterComplete: func(t *testing.T, db *sql.DB, schema string) {
				// Inserting a row into the `people` table with a NULL `manages` value fails
				MustNotInsert(t, db, schema, "02_alter_column", "people", map[string]string{
					"id":   "1",
					"name": "carl",
				}, testutils.NotNullViolationErrorCode)

				// Inserting a row into the `people` table with a `manages` field that
				// violates the FK constraint fails
				MustNotInsert(t, db, schema, "02_alter_column", "people", map[string]string{
					"id":      "3",
					"name":    "carl",
					"manages": "2",
				}, testutils.FKViolationErrorCode)

				// Inserting a row into the `people` table with a valid `manages` field
				// succeeds
				MustInsert(t, db, schema, "02_alter_column", "people", map[string]string{
					"id":      "3",
					"name":    "carl",
					"manages": "1",
				})

				// The `people` table has the expected rows.
				rows := MustSelect(t, db, schema, "02_alter_column", "people")
				assert.Equal(t, []map[string]any{
					{"id": 1, "name": "alice", "manages": 1},
					{"id": 2, "name": "bob", "manages": 1},
					{"id": 3, "name": "carl", "manages": 1},
				}, rows)
			},
		},
	})
}

func TestAlterColumnMultipleTimes(t *testing.T) {
	t.Skip("This test is not supported by the current implementation of pgroll, as it does not support multiple alterations on the same column in a single migration.")
	t.Parallel()

	ExecuteTests(t, TestCases{
		{
			name: "can alter a column: set not null, change type, change comment, and add check constraint",
			migrations: []migrations.Migration{
				{
					Name:          "01_create_table",
					VersionSchema: "create_table",
					Operations: migrations.Operations{
						&migrations.OpCreateTable{
							Name: "events",
							Columns: []migrations.Column{
								{
									Name: "id",
									Type: "serial",
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
					Name:          "02_alter_column",
					VersionSchema: "alter_column",
					Operations: migrations.Operations{
						// The first alteration sets the column to not null
						&migrations.OpAlterColumn{
							Table:    "events",
							Column:   "name",
							Up:       "SELECT CASE WHEN name IS NULL THEN 'placeholder' ELSE name END",
							Down:     "name",
							Nullable: ptr(false),
						},
						// The second alteration adds a check constraint
						&migrations.OpAlterColumn{
							Table:  "events",
							Column: "name",
							Up:     "SELECT CASE WHEN LENGTH(name) <= 3 THEN 'placeholder' ELSE name END",
							Down:   "name",
							Check: &migrations.CheckConstraint{
								Name:       "name_length",
								Constraint: "length(name) > 3",
							},
						},
						// The third alteration adds a comment
						&migrations.OpAlterColumn{
							Table:   "events",
							Column:  "name",
							Up:      "name",
							Down:    "name",
							Comment: nullable.NewNullableWithValue("the name of the event"),
						},
					},
				},
			},
			afterStart: func(t *testing.T, db *sql.DB, schema string) {
				// Inserting a NULL into the new column should fail
				MustNotInsert(t, db, schema, "alter_column", "events", map[string]string{
					"id": "1",
				}, testutils.CheckViolationErrorCode)

				// Inserting a non-NULL value into the new column should succeed
				MustInsert(t, db, schema, "alter_column", "events", map[string]string{
					"id":   "2",
					"name": "apples",
				})

				// The value inserted into the new column has been backfilled into the
				// old column.
				rows := MustSelect(t, db, schema, "create_table", "events")
				assert.Equal(t, []map[string]any{
					{"id": 2, "name": "apples"},
				}, rows)

				// Inserting a NULL value into the old column should succeed
				MustInsert(t, db, schema, "create_table", "events", map[string]string{
					"id": "3",
				})

				// The NULL value inserted into the old column has been written into
				// the new column using the `up` SQL.
				rows = MustSelect(t, db, schema, "alter_column", "events")
				assert.Equal(t, []map[string]any{
					{"id": 2, "name": "apples"},
					{"id": 3, "name": "placeholder"},
				}, rows)

				// Inserting a non-NULL value into the old column should succeed
				MustInsert(t, db, schema, "create_table", "events", map[string]string{
					"id":   "4",
					"name": "bananas",
				})

				// The non-NULL value inserted into the old column has been copied
				// unchanged into the new column.
				rows = MustSelect(t, db, schema, "alter_column", "events")
				assert.Equal(t, []map[string]any{
					{"id": 2, "name": "apples"},
					{"id": 3, "name": "placeholder"},
					{"id": 4, "name": "bananas"},
				}, rows)

				// Inserting a row into the new column that violates the
				// check constraint should fail
				MustNotInsert(t, db, schema, "alter_column", "events", map[string]string{
					"id":   "5",
					"name": "x",
				}, testutils.CheckViolationErrorCode)

				// Inserting a row into the old column that violates the
				// check constraint should succeed.
				MustInsert(t, db, schema, "create_table", "events", map[string]string{
					"id":   "5",
					"name": "x",
				})

				// The value that didn't meet the check constraint has been rewritten
				// into the new column using the `up` SQL.
				rows = MustSelect(t, db, schema, "alter_column", "events")
				assert.Equal(t, []map[string]any{
					{"id": 2, "name": "apples"},
					{"id": 3, "name": "placeholder"},
					{"id": 4, "name": "bananas"},
					{"id": 5, "name": "placeholder"},
				}, rows)

				// The new column should have the new comment.
				ColumnMustHaveComment(t, db, schema, "events", migrations.TemporaryName("name"), "the name of the event")
			},
			afterRollback: func(t *testing.T, db *sql.DB, schema string) {
				// The new (temporary) column should not exist on the underlying table.
				ColumnMustNotExist(t, db, schema, "events", migrations.TemporaryName("name"))
			},
			afterComplete: func(t *testing.T, db *sql.DB, schema string) {
				// Inserting a NULL into the new column should fail
				MustNotInsert(t, db, schema, "alter_column", "events", map[string]string{
					"id": "6",
				}, testutils.NotNullViolationErrorCode)

				// Inserting a row into the new column that violates the
				// check constraint should fail
				MustNotInsert(t, db, schema, "alter_column", "events", map[string]string{
					"id":   "6",
					"name": "x",
				}, testutils.CheckViolationErrorCode)

				// The type of the new column in the underlying table should be `text`
				ColumnMustHaveType(t, db, schema, "events", "name", "text")

				// The new column should have the new comment.
				ColumnMustHaveComment(t, db, schema, "events", "name", "the name of the event")

				// The table contains the expected rows
				rows := MustSelect(t, db, schema, "alter_column", "events")
				assert.Equal(t, []map[string]any{
					{"id": 2, "name": "apples"},
					{"id": 3, "name": "placeholder"},
					{"id": 4, "name": "bananas"},
					{"id": 5, "name": "placeholder"},
				}, rows)
			},
		},
	})
}

func TestAlterColumnValidation(t *testing.T) {
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
			name: "table must exist",
			migrations: []migrations.Migration{
				createTablesMigration,
				{
					Name: "01_alter_column",
					Operations: migrations.Operations{
						&migrations.OpAlterColumn{
							Table:    "doesntexist",
							Column:   "title",
							Nullable: ptr(false),
						},
					},
				},
			},
			wantStartErr: migrations.TableDoesNotExistError{Name: "doesntexist"},
		},
		{
			name: "column must exist",
			migrations: []migrations.Migration{
				createTablesMigration,
				{
					Name: "01_alter_column",
					Operations: migrations.Operations{
						&migrations.OpAlterColumn{
							Table:    "posts",
							Column:   "doesntexist",
							Nullable: ptr(false),
						},
					},
				},
			},
			wantStartErr: migrations.ColumnDoesNotExistError{Table: "posts", Name: "doesntexist"},
		},
		{
			name: "cant make no changes",
			migrations: []migrations.Migration{
				createTablesMigration,
				{
					Name: "01_alter_column",
					Operations: migrations.Operations{
						&migrations.OpAlterColumn{
							Table:  "posts",
							Column: "title",
						},
					},
				},
			},
			wantStartErr: migrations.AlterColumnNoChangesError{Table: "posts", Column: "title"},
		},
		{
			name: "backfill with multiple primary keys",
			migrations: []migrations.Migration{
				{
					Name: "01_add_table",
					Operations: migrations.Operations{
						&migrations.OpRawSQL{
							Up:   "CREATE TABLE orders(id integer, order_id integer, quantity integer, primary key (id, order_id))",
							Down: "DROP TABLE orders",
						},
					},
				},
				{
					Name: "02_alter_column",
					Operations: migrations.Operations{
						&migrations.OpAlterColumn{
							Table:    "orders",
							Column:   "quantity",
							Nullable: ptr(false),
							Up:       "1",
						},
					},
				},
			},
			wantStartErr: nil,
		},
	})
}
