// SPDX-License-Identifier: Apache-2.0

package migrations_test

import (
	"database/sql"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/xataio/pgroll/pkg/migrations"
	"github.com/xataio/pgroll/pkg/testutils"
)

func TestAlterColumnMultipleSubOperations(t *testing.T) {
	t.Parallel()

	ExecuteTests(t, TestCases{
		{
			name: "can alter a column: set not null, change type, rename and add check constraint",
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
									Pk:   ptr(true),
								},
								{
									Name:     "name",
									Type:     "varchar(255)",
									Nullable: ptr(true),
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
							Up:       "(SELECT CASE WHEN name IS NULL OR LENGTH(name) <= 3 THEN 'placeholder' ELSE name END)",
							Down:     "name",
							Name:     ptr("event_name"),
							Type:     ptr("text"),
							Nullable: ptr(false),
							Check: &migrations.CheckConstraint{
								Name:       "event_name_length",
								Constraint: "length(name) > 3",
							},
						},
					},
				},
			},
			afterStart: func(t *testing.T, db *sql.DB, schema string) {
				// Inserting a NULL into the new `event_name` column should fail
				MustNotInsert(t, db, schema, "02_alter_column", "events", map[string]string{
					"id": "1",
				}, testutils.CheckViolationErrorCode)

				// Inserting a non-NULL value into the new `event_name` column should succeed
				MustInsert(t, db, schema, "02_alter_column", "events", map[string]string{
					"id":         "2",
					"event_name": "apples",
				})

				// The value inserted into the new `event_name` column has been backfilled into the
				// old `name` column.
				rows := MustSelect(t, db, schema, "01_create_table", "events")
				assert.Equal(t, []map[string]any{
					{"id": 2, "name": "apples"},
				}, rows)

				// Inserting a NULL value into the old `name` column should succeed
				MustInsert(t, db, schema, "01_create_table", "events", map[string]string{
					"id": "3",
				})

				// The NULL value inserted into the old `name` column has been written into
				// the new `event_name` column using the `up` SQL.
				rows = MustSelect(t, db, schema, "02_alter_column", "events")
				assert.Equal(t, []map[string]any{
					{"id": 2, "event_name": "apples"},
					{"id": 3, "event_name": "placeholder"},
				}, rows)

				// Inserting a non-NULL value into the old `name` column should succeed
				MustInsert(t, db, schema, "01_create_table", "events", map[string]string{
					"id":   "4",
					"name": "bananas",
				})

				// The non-NULL value inserted into the old `name` column has been copied
				// unchanged into the new `event_name` column.
				rows = MustSelect(t, db, schema, "02_alter_column", "events")
				assert.Equal(t, []map[string]any{
					{"id": 2, "event_name": "apples"},
					{"id": 3, "event_name": "placeholder"},
					{"id": 4, "event_name": "bananas"},
				}, rows)

				// Inserting a row into the new `event_name` column that violates the
				// check constraint should fail
				MustNotInsert(t, db, schema, "02_alter_column", "events", map[string]string{
					"id":         "5",
					"event_name": "x",
				}, testutils.CheckViolationErrorCode)

				// Inserting a row into the old `name` column that violates the
				// check constraint should succeed.
				MustInsert(t, db, schema, "01_create_table", "events", map[string]string{
					"id":   "5",
					"name": "x",
				})

				// The value that didn't meet the check constraint has been rewritten
				// into the new `event_name` column using the `up` SQL.
				rows = MustSelect(t, db, schema, "02_alter_column", "events")
				assert.Equal(t, []map[string]any{
					{"id": 2, "event_name": "apples"},
					{"id": 3, "event_name": "placeholder"},
					{"id": 4, "event_name": "bananas"},
					{"id": 5, "event_name": "placeholder"},
				}, rows)

				// The type of the new column in the underlying table should be `text`
				ColumnMustHaveType(t, db, schema, "events", migrations.TemporaryName("name"), "text")
			},
			afterRollback: func(t *testing.T, db *sql.DB, schema string) {
				// The new (temporary) `name` column should not exist on the underlying table.
				ColumnMustNotExist(t, db, schema, "events", migrations.TemporaryName("name"))
			},
			afterComplete: func(t *testing.T, db *sql.DB, schema string) {
				// Inserting a NULL into the new `event_name` column should fail
				MustNotInsert(t, db, schema, "02_alter_column", "events", map[string]string{
					"id": "6",
				}, testutils.NotNullViolationErrorCode)

				// Inserting a row into the new `event_name` column that violates the
				// check constraint should fail
				MustNotInsert(t, db, schema, "02_alter_column", "events", map[string]string{
					"id":         "6",
					"event_name": "x",
				}, testutils.CheckViolationErrorCode)

				// The type of the new column in the underlying table should be `text`
				ColumnMustHaveType(t, db, schema, "events", "event_name", "text")

				// The column in the underlying table should be `event_name`
				ColumnMustExist(t, db, schema, "events", "event_name")

				// The table contains the expected rows
				rows := MustSelect(t, db, schema, "02_alter_column", "events")
				assert.Equal(t, []map[string]any{
					{"id": 2, "event_name": "apples"},
					{"id": 3, "event_name": "placeholder"},
					{"id": 4, "event_name": "bananas"},
					{"id": 5, "event_name": "placeholder"},
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
									Pk:   ptr(true),
								},
								{
									Name:     "name",
									Type:     "varchar(255)",
									Nullable: ptr(true),
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
							Name:   ptr("event_name"),
							Unique: &migrations.UniqueConstraint{
								Name: "events_event_name_unique",
							},
						},
					},
				},
			},
			afterStart: func(t *testing.T, db *sql.DB, schema string) {
				// Can insert a row into the new `event_name` column
				MustInsert(t, db, schema, "02_alter_column", "events", map[string]string{
					"id":         "1",
					"event_name": "apples",
				})

				// Inserting a row with the same value into the new `event_name` column fails
				MustNotInsert(t, db, schema, "02_alter_column", "events", map[string]string{
					"id":         "2",
					"event_name": "apples",
				}, testutils.UniqueViolationErrorCode)

				// Inserting a row with a duplicate value into the old `name` column succeeds
				MustInsert(t, db, schema, "01_create_table", "events", map[string]string{
					"id":   "2",
					"name": "apples",
				})
			},
			afterRollback: func(t *testing.T, db *sql.DB, schema string) {
			},
			afterComplete: func(t *testing.T, db *sql.DB, schema string) {
				// Inserting a row with the same value into the new `event_name` column fails
				MustNotInsert(t, db, schema, "02_alter_column", "events", map[string]string{
					"id":         "2",
					"event_name": "apples",
				}, testutils.UniqueViolationErrorCode)

				// The column in the underlying table has been renamed to `event_name`
				ColumnMustExist(t, db, schema, "events", "event_name")
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
									Pk:   ptr(true),
								},
								{
									Name:     "name",
									Type:     "varchar(255)",
									Nullable: ptr(true),
								},
							},
						},
						&migrations.OpCreateTable{
							Name: "people",
							Columns: []migrations.Column{
								{
									Name: "id",
									Type: "serial",
									Pk:   ptr(true),
								},
								{
									Name:     "name",
									Type:     "varchar(255)",
									Nullable: ptr(true),
								},
								{
									Name:     "manages",
									Type:     "integer",
									Nullable: ptr(true),
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
							Up:       "(SELECT CASE WHEN manages IS NULL THEN 1 ELSE manages END)",
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
			name: "table must exist",
			migrations: []migrations.Migration{
				createTablesMigration,
				{
					Name: "01_alter_column",
					Operations: migrations.Operations{
						&migrations.OpAlterColumn{
							Table:  "doesntexist",
							Column: "title",
							Name:   ptr("renamed_title"),
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
							Table:  "posts",
							Column: "doesntexist",
							Name:   ptr("renamed_title"),
						},
					},
				},
			},
			wantStartErr: migrations.ColumnDoesNotExistError{Table: "posts", Name: "doesntexist"},
		},
		{
			name: "column rename: no up SQL allowed",
			migrations: []migrations.Migration{
				createTablesMigration,
				{
					Name: "01_alter_column",
					Operations: migrations.Operations{
						&migrations.OpAlterColumn{
							Table:  "posts",
							Column: "title",
							Name:   ptr("renamed_title"),
							Up:     "some up sql",
						},
					},
				},
			},
			wantStartErr: migrations.NoUpSQLAllowedError{},
		},
		{
			name: "column rename: no down SQL allowed",
			migrations: []migrations.Migration{
				createTablesMigration,
				{
					Name: "01_alter_column",
					Operations: migrations.Operations{
						&migrations.OpAlterColumn{
							Table:  "posts",
							Column: "title",
							Name:   ptr("renamed_title"),
							Down:   "some down sql",
						},
					},
				},
			},
			wantStartErr: migrations.NoDownSQLAllowedError{},
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
			name: "table must have a primary key on exactly one column",
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
							Table:  "orders",
							Column: "quantity",
							Name:   ptr("renamed_quantity"),
						},
					},
				},
			},
			wantStartErr: migrations.BackfillNotPossibleError{Table: "orders"},
		},
	})
}
