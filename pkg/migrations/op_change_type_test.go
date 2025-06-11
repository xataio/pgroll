// SPDX-License-Identifier: Apache-2.0

package migrations_test

import (
	"database/sql"
	"testing"

	"github.com/xataio/pgroll/internal/testutils"

	"github.com/stretchr/testify/assert"

	"github.com/xataio/pgroll/pkg/migrations"
	"github.com/xataio/pgroll/pkg/roll"
)

func TestChangeColumnType(t *testing.T) {
	t.Parallel()

	ExecuteTests(t, TestCases{
		{
			name: "change column type",
			migrations: []migrations.Migration{
				{
					Name:          "01_add_table",
					VersionSchema: "add_table",
					Operations: migrations.Operations{
						&migrations.OpCreateTable{
							Name: "reviews",
							Columns: []migrations.Column{
								{
									Name: "id",
									Type: "serial",
									Pk:   true,
								},
								{
									Name: "username",
									Type: "text",
								},
								{
									Name: "product",
									Type: "text",
								},
								{
									Name:    "rating",
									Type:    "text",
									Default: ptr("0"),
								},
							},
						},
					},
				},
				{
					Name:          "02_change_type",
					VersionSchema: "change_type",
					Operations: migrations.Operations{
						&migrations.OpAlterColumn{
							Table:  "reviews",
							Column: "rating",
							Type:   ptr("integer"),
							Up:     "CAST (rating AS integer)",
							Down:   "CAST (rating AS text)",
						},
					},
				},
			},
			afterStart: func(t *testing.T, db *sql.DB, schema string) {
				newVersionSchema := roll.VersionedSchemaName(schema, "change_type")

				// The new (temporary) `rating` column should exist on the underlying table.
				ColumnMustExist(t, db, schema, "reviews", migrations.TemporaryName("rating"))

				// The `rating` column in the new view must have the correct type.
				ColumnMustHaveType(t, db, newVersionSchema, "reviews", "rating", "integer")

				// Inserting into the new `rating` column should work.
				MustInsert(t, db, schema, "change_type", "reviews", map[string]string{
					"username": "alice",
					"product":  "apple",
					"rating":   "5",
				})

				// The value inserted into the new `rating` column has been backfilled into
				// the old `rating` column.
				rows := MustSelect(t, db, schema, "add_table", "reviews")
				assert.Equal(t, []map[string]any{
					{"id": 1, "username": "alice", "product": "apple", "rating": "5"},
				}, rows)

				// Inserting into the old `rating` column should work.
				MustInsert(t, db, schema, "add_table", "reviews", map[string]string{
					"username": "bob",
					"product":  "banana",
					"rating":   "8",
				})

				// The value inserted into the old `rating` column has been backfilled into
				// the new `rating` column.
				rows = MustSelect(t, db, schema, "change_type", "reviews")
				assert.Equal(t, []map[string]any{
					{"id": 1, "username": "alice", "product": "apple", "rating": 5},
					{"id": 2, "username": "bob", "product": "banana", "rating": 8},
				}, rows)
			},
			afterRollback: func(t *testing.T, db *sql.DB, schema string) {
				// The table is cleaned up; temporary columns, trigger functions and triggers no longer exist.
				TableMustBeCleanedUp(t, db, schema, "reviews", "rating")
			},
			afterComplete: func(t *testing.T, db *sql.DB, schema string) {
				newVersionSchema := roll.VersionedSchemaName(schema, "change_type")

				// The table is cleaned up; temporary columns, trigger functions and triggers no longer exist.
				TableMustBeCleanedUp(t, db, schema, "reviews", "rating")

				// The `rating` column in the new view must have the correct type.
				ColumnMustHaveType(t, db, newVersionSchema, "reviews", "rating", "integer")

				// Inserting into the new view should work.
				MustInsert(t, db, schema, "change_type", "reviews", map[string]string{
					"username": "carl",
					"product":  "carrot",
					"rating":   "3",
				})

				// Selecting from the new view should succeed.
				rows := MustSelect(t, db, schema, "change_type", "reviews")
				assert.Equal(t, []map[string]any{
					{"id": 1, "username": "alice", "product": "apple", "rating": 5},
					{"id": 2, "username": "bob", "product": "banana", "rating": 8},
					{"id": 3, "username": "carl", "product": "carrot", "rating": 3},
				}, rows)

				// The up function no longer exists.
				FunctionMustNotExist(t, db, schema, migrations.TriggerFunctionName("reviews", "rating"))
				// The down function no longer exists.
				FunctionMustNotExist(t, db, schema, migrations.TriggerFunctionName("reviews", migrations.TemporaryName("rating")))

				// The up trigger no longer exists.
				TriggerMustNotExist(t, db, schema, "reviews", migrations.TriggerName("reviews", "rating"))
				// The down trigger no longer exists.
				TriggerMustNotExist(t, db, schema, "reviews", migrations.TriggerName("reviews", migrations.TemporaryName("rating")))
			},
		},
		{
			name: "changing column type preserves any foreign key constraints on the column",
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
									Name: "department_id",
									Type: "integer",
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
					Name: "03_change_type",
					Operations: migrations.Operations{
						&migrations.OpAlterColumn{
							Table:  "employees",
							Column: "department_id",
							Type:   ptr("bigint"),
							Up:     "department_id",
							Down:   "department_id",
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
			name: "changing column type removes any incompatible defaults on the column",
			migrations: []migrations.Migration{
				{
					Name: "01_add_table",
					Operations: migrations.Operations{
						&migrations.OpCreateTable{
							Name: "users",
							Columns: []migrations.Column{
								{
									Name: "id",
									Type: "integer",
									Pk:   true,
								},
								{
									Name:     "age",
									Type:     "text",
									Default:  ptr("'0'"),
									Nullable: true,
								},
							},
						},
					},
				},
				{
					Name: "02_change_type",
					Operations: migrations.Operations{
						&migrations.OpAlterColumn{
							Table:  "users",
							Column: "age",
							Type:   ptr("integer"),
							Up:     "CAST(age AS integer)",
							Down:   "CAST(age AS text)",
						},
					},
				},
			},
			afterStart: func(t *testing.T, db *sql.DB, schema string) {
				// A row can be inserted into the new version of the table.
				MustInsert(t, db, schema, "02_change_type", "users", map[string]string{
					"id": "1",
				})

				// The newly inserted row contains a NULL instead of the old default value.
				rows := MustSelect(t, db, schema, "02_change_type", "users")
				assert.Equal(t, []map[string]any{
					{"id": 1, "age": nil},
				}, rows)
			},
			afterRollback: func(t *testing.T, db *sql.DB, schema string) {
			},
			afterComplete: func(t *testing.T, db *sql.DB, schema string) {
				// A row can be inserted into the new version of the table.
				MustInsert(t, db, schema, "02_change_type", "users", map[string]string{
					"id": "2",
				})

				// The newly inserted row contains a NULL instead of the old default value.
				rows := MustSelect(t, db, schema, "02_change_type", "users")
				assert.Equal(t, []map[string]any{
					{"id": 1, "age": nil},
					{"id": 2, "age": nil},
				}, rows)
			},
		},
		{
			name: "changing column type preserves any compatible defaults on the column",
			migrations: []migrations.Migration{
				{
					Name: "01_add_table",
					Operations: migrations.Operations{
						&migrations.OpCreateTable{
							Name: "users",
							Columns: []migrations.Column{
								{
									Name: "id",
									Type: "integer",
									Pk:   true,
								},
								{
									Name:     "username",
									Type:     "text",
									Default:  ptr("'alice'"),
									Nullable: true,
								},
							},
						},
					},
				},
				{
					Name: "02_change_type",
					Operations: migrations.Operations{
						&migrations.OpAlterColumn{
							Table:  "users",
							Column: "username",
							Type:   ptr("varchar(255)"),
							Up:     "username",
							Down:   "username",
						},
					},
				},
			},
			afterStart: func(t *testing.T, db *sql.DB, schema string) {
				// A row can be inserted into the new version of the table.
				MustInsert(t, db, schema, "02_change_type", "users", map[string]string{
					"id": "1",
				})

				// The newly inserted row respects the default value of the column.
				rows := MustSelect(t, db, schema, "02_change_type", "users")
				assert.Equal(t, []map[string]any{
					{"id": 1, "username": "alice"},
				}, rows)
			},
			afterRollback: func(t *testing.T, db *sql.DB, schema string) {
			},
			afterComplete: func(t *testing.T, db *sql.DB, schema string) {
				// A row can be inserted into the new version of the table.
				MustInsert(t, db, schema, "02_change_type", "users", map[string]string{
					"id": "2",
				})

				// The newly inserted row respects the default value of the column.
				rows := MustSelect(t, db, schema, "02_change_type", "users")
				assert.Equal(t, []map[string]any{
					{"id": 1, "username": "alice"},
					{"id": 2, "username": "alice"},
				}, rows)
			},
		},
		{
			name: "changing column type removes any incompatile check constraints on the column",
			migrations: []migrations.Migration{
				{
					Name: "01_add_table",
					Operations: migrations.Operations{
						&migrations.OpCreateTable{
							Name: "users",
							Columns: []migrations.Column{
								{
									Name: "id",
									Type: "integer",
									Pk:   true,
								},
								{
									Name:     "age",
									Type:     "text",
									Nullable: true,
									Check: &migrations.CheckConstraint{
										Name:       "age_length",
										Constraint: "length(age) < 3",
									},
								},
							},
						},
					},
				},
				{
					Name: "02_change_type",
					Operations: migrations.Operations{
						&migrations.OpAlterColumn{
							Table:  "users",
							Column: "age",
							Type:   ptr("integer"),
							Up:     "CAST(age AS integer)",
							Down:   "SELECT CASE WHEN age < 100 THEN age::text ELSE '0' END",
						},
					},
				},
			},
			afterStart: func(t *testing.T, db *sql.DB, schema string) {
				// Inserting a row into the new schema that violates the check
				// constraint on the old schema should succeed.
				MustInsert(t, db, schema, "02_change_type", "users", map[string]string{
					"id":  "1",
					"age": "1111",
				})
			},
			afterRollback: func(t *testing.T, db *sql.DB, schema string) {
			},
			afterComplete: func(t *testing.T, db *sql.DB, schema string) {
				// Inserting a row into the new schema that violates the check
				// constraint on the old schema should succeed.
				MustInsert(t, db, schema, "02_change_type", "users", map[string]string{
					"id":  "2",
					"age": "2222",
				})
			},
		},
		{
			name: "changing column type preserves any compatible check constraints on the column",
			migrations: []migrations.Migration{
				{
					Name: "01_add_table",
					Operations: migrations.Operations{
						&migrations.OpCreateTable{
							Name: "users",
							Columns: []migrations.Column{
								{
									Name: "id",
									Type: "integer",
									Pk:   true,
								},
								{
									Name:     "username",
									Type:     "text",
									Nullable: true,
									Check: &migrations.CheckConstraint{
										Name:       "username_length",
										Constraint: "length(username) > 3",
									},
								},
							},
						},
					},
				},
				{
					Name: "02_change_type",
					Operations: migrations.Operations{
						&migrations.OpAlterColumn{
							Table:  "users",
							Column: "username",
							Type:   ptr("varchar(255)"),
							Up:     "username",
							Down:   "username",
						},
					},
				},
			},
			afterStart: func(t *testing.T, db *sql.DB, schema string) {
				// Inserting a row that violates the check constraint should fail.
				MustNotInsert(t, db, schema, "02_change_type", "users", map[string]string{
					"id":       "1",
					"username": "a",
				}, testutils.CheckViolationErrorCode)
			},
			afterRollback: func(t *testing.T, db *sql.DB, schema string) {
			},
			afterComplete: func(t *testing.T, db *sql.DB, schema string) {
				// Inserting a row that violates the check constraint should fail.
				MustNotInsert(t, db, schema, "02_change_type", "users", map[string]string{
					"id":       "2",
					"username": "b",
				}, testutils.CheckViolationErrorCode)
			},
		},
		{
			name: "changing column type preserves column not null",
			migrations: []migrations.Migration{
				{
					Name: "01_add_table",
					Operations: migrations.Operations{
						&migrations.OpCreateTable{
							Name: "users",
							Columns: []migrations.Column{
								{
									Name: "id",
									Type: "integer",
									Pk:   true,
								},
								{
									Name:     "username",
									Type:     "text",
									Nullable: false,
								},
							},
						},
					},
				},
				{
					Name: "02_change_type",
					Operations: migrations.Operations{
						&migrations.OpAlterColumn{
							Table:  "users",
							Column: "username",
							Type:   ptr("varchar(255)"),
							Up:     "username",
							Down:   "username",
						},
					},
				},
			},
			afterStart: func(t *testing.T, db *sql.DB, schema string) {
				// Inserting a row that violates the NOT NULL constraint fails.
				MustNotInsert(t, db, schema, "02_change_type", "users", map[string]string{
					"id": "1",
				}, testutils.NotNullViolationErrorCode)
			},
			afterRollback: func(t *testing.T, db *sql.DB, schema string) {
			},
			afterComplete: func(t *testing.T, db *sql.DB, schema string) {
				// Inserting a row that violates the NOT NULL constraint fails.
				MustNotInsert(t, db, schema, "02_change_type", "users", map[string]string{
					"id": "2",
				}, testutils.NotNullViolationErrorCode)
			},
		},
		{
			name: "changing column type preserves any unique constraints on the column",
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
									Name: "username",
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
							Table:  "users",
							Column: "username",
							Unique: &migrations.UniqueConstraint{Name: "unique_username"},
							Up:     "username",
							Down:   "username",
						},
					},
				},
				{
					Name: "03_change_type",
					Operations: migrations.Operations{
						&migrations.OpAlterColumn{
							Table:  "users",
							Column: "username",
							Type:   ptr("varchar(255)"),
							Up:     "username",
							Down:   "username",
						},
					},
				},
			},
			afterStart: func(t *testing.T, db *sql.DB, schema string) {
				// Inserting an initial row succeeds
				MustInsert(t, db, schema, "03_change_type", "users", map[string]string{
					"username": "alice",
				})

				// Inserting a row with a duplicate `username` value fails
				MustNotInsert(t, db, schema, "03_change_type", "users", map[string]string{
					"username": "alice",
				}, testutils.UniqueViolationErrorCode)
			},
			afterRollback: func(t *testing.T, db *sql.DB, schema string) {
			},
			afterComplete: func(t *testing.T, db *sql.DB, schema string) {
				// The table has a unique constraint defined on it
				UniqueConstraintMustExist(t, db, schema, "users", "unique_username")

				// Inserting a row with a duplicate `username` value fails
				MustNotInsert(t, db, schema, "03_change_type", "users", map[string]string{
					"username": "alice",
				}, testutils.UniqueViolationErrorCode)

				// Inserting a row with a different `username` value succeeds
				MustInsert(t, db, schema, "03_change_type", "users", map[string]string{
					"username": "bob",
				})
			},
		},
		{
			name: "changing column type preserves any comments on the column",
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
									Name:    "username",
									Type:    "text",
									Comment: ptr("the name of the user"),
								},
							},
						},
					},
				},
				{
					Name: "02_change_type",
					Operations: migrations.Operations{
						&migrations.OpAlterColumn{
							Table:  "users",
							Column: "username",
							Type:   ptr("varchar(255)"),
							Up:     "username",
							Down:   "username",
						},
					},
				},
			},
			afterStart: func(t *testing.T, db *sql.DB, schema string) {
				// The duplicated column has a comment defined on it
				ColumnMustHaveComment(t, db, schema, "users", migrations.TemporaryName("username"), "the name of the user")
			},
			afterRollback: func(t *testing.T, db *sql.DB, schema string) {
			},
			afterComplete: func(t *testing.T, db *sql.DB, schema string) {
				// The final column has a comment defined on it
				ColumnMustHaveComment(t, db, schema, "users", "username", "the name of the user")
			},
		},
		{
			name: "can change the type of a column on a table with a case-sensitive name",
			migrations: []migrations.Migration{
				{
					Name: "01_add_table",
					Operations: migrations.Operations{
						&migrations.OpCreateTable{
							Name: "Users",
							Columns: []migrations.Column{
								{
									Name: "id",
									Type: "serial",
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
					Name: "02_change_type",
					Operations: migrations.Operations{
						&migrations.OpAlterColumn{
							Table:  "Users",
							Column: "name",
							Type:   ptr("text"),
							Up:     "name",
							Down:   "name",
						},
					},
				},
			},
			afterStart: func(t *testing.T, db *sql.DB, schema string) {
				// The new column has the expected type
				ColumnMustHaveType(t, db, schema, "Users", migrations.TemporaryName("name"), "text")
			},
			afterRollback: func(t *testing.T, db *sql.DB, schema string) {
				// The table has been cleaned up
				TableMustBeCleanedUp(t, db, schema, "Users", "name")
			},
			afterComplete: func(t *testing.T, db *sql.DB, schema string) {
				// The new column has the expected type
				ColumnMustHaveType(t, db, schema, "Users", "name", "text")
			},
		},
	})
}

func TestChangeTypeInMultiOperationMigrations(t *testing.T) {
	t.Parallel()

	ExecuteTests(t, TestCases{
		{
			name: "rename table, change type",
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
									Type:     "varchar(3)",
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
							Type:   ptr("text"),
							Up:     "name",
							Down:   "CASE WHEN LENGTH(name) > 3 THEN 'xxx' ELSE name END",
						},
					},
				},
			},
			afterStart: func(t *testing.T, db *sql.DB, schema string) {
				// The new column has the expected type
				// The table hasn't been physically renamed yet, so we need to use the old name
				ColumnMustHaveType(t, db, schema, "items", migrations.TemporaryName("name"), "text")

				// Can insert a row into the new schema
				MustInsert(t, db, schema, "02_multi_operation", "products", map[string]string{
					"id":   "1",
					"name": "apple",
				})

				// Can insert a row into the old schema
				MustInsert(t, db, schema, "01_create_table", "items", map[string]string{
					"id":   "2",
					"name": "abc",
				})

				// The new view has the expected rows
				rows := MustSelect(t, db, schema, "02_multi_operation", "products")
				assert.Equal(t, []map[string]any{
					{"id": 1, "name": "apple"},
					{"id": 2, "name": "abc"},
				}, rows)

				// The old view has the expected rows
				rows = MustSelect(t, db, schema, "01_create_table", "items")
				assert.Equal(t, []map[string]any{
					{"id": 1, "name": "xxx"},
					{"id": 2, "name": "abc"},
				}, rows)
			},
			afterRollback: func(t *testing.T, db *sql.DB, schema string) {
				// The table has been cleaned up
				TableMustBeCleanedUp(t, db, schema, "items", "name")
			},
			afterComplete: func(t *testing.T, db *sql.DB, schema string) {
				// The new column has the expected type
				ColumnMustHaveType(t, db, schema, "products", "name", "text")

				// Can insert a row into the new schema
				MustInsert(t, db, schema, "02_multi_operation", "products", map[string]string{
					"id":   "3",
					"name": "carrot",
				})

				// The new view has the expected rows
				rows := MustSelect(t, db, schema, "02_multi_operation", "products")
				assert.Equal(t, []map[string]any{
					{"id": 1, "name": "xxx"},
					{"id": 2, "name": "abc"},
					{"id": 3, "name": "carrot"},
				}, rows)

				// The table has been cleaned up
				TableMustBeCleanedUp(t, db, schema, "products", "name")
			},
		},
		{
			name: "rename table, rename column, change type",
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
									Type:     "varchar(3)",
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
							Type:   ptr("text"),
							Up:     "item_name",
							Down:   "CASE WHEN LENGTH(item_name) > 3 THEN 'xxx' ELSE item_name END",
						},
					},
				},
			},
			afterStart: func(t *testing.T, db *sql.DB, schema string) {
				// The new column has the expected type
				// The table hasn't been physically renamed yet, so we need to use the old name
				ColumnMustHaveType(t, db, schema, "items", migrations.TemporaryName("item_name"), "text")

				// Can insert a row into the new schema
				MustInsert(t, db, schema, "02_multi_operation", "products", map[string]string{
					"id":        "1",
					"item_name": "apple",
				})

				// Can insert a row into the old schema
				MustInsert(t, db, schema, "01_create_table", "items", map[string]string{
					"id":   "2",
					"name": "abc",
				})

				// The new view has the expected rows
				rows := MustSelect(t, db, schema, "02_multi_operation", "products")
				assert.Equal(t, []map[string]any{
					{"id": 1, "item_name": "apple"},
					{"id": 2, "item_name": "abc"},
				}, rows)

				// The old view has the expected rows
				rows = MustSelect(t, db, schema, "01_create_table", "items")
				assert.Equal(t, []map[string]any{
					{"id": 1, "name": "xxx"},
					{"id": 2, "name": "abc"},
				}, rows)
			},
			afterRollback: func(t *testing.T, db *sql.DB, schema string) {
				// The table has been cleaned up
				TableMustBeCleanedUp(t, db, schema, "items", "name")
				TableMustBeCleanedUp(t, db, schema, "items", "item_name")
			},
			afterComplete: func(t *testing.T, db *sql.DB, schema string) {
				// The new column has the expected type
				ColumnMustHaveType(t, db, schema, "products", "item_name", "text")

				// Can insert a row into the new schema
				MustInsert(t, db, schema, "02_multi_operation", "products", map[string]string{
					"id":        "3",
					"item_name": "carrot",
				})

				// The new view has the expected rows
				rows := MustSelect(t, db, schema, "02_multi_operation", "products")
				assert.Equal(t, []map[string]any{
					{"id": 1, "item_name": "xxx"},
					{"id": 2, "item_name": "abc"},
					{"id": 3, "item_name": "carrot"},
				}, rows)

				// The table has been cleaned up
				TableMustBeCleanedUp(t, db, schema, "products", "name")
				TableMustBeCleanedUp(t, db, schema, "products", "item_name")
			},
		},
	})
}

func TestChangeColumnTypeValidation(t *testing.T) {
	t.Parallel()

	createTableMigration := migrations.Migration{
		Name: "01_add_table",
		Operations: migrations.Operations{
			&migrations.OpCreateTable{
				Name: "reviews",
				Columns: []migrations.Column{
					{
						Name: "id",
						Type: "serial",
						Pk:   true,
					},
					{
						Name: "username",
						Type: "text",
					},
					{
						Name: "product",
						Type: "text",
					},
					{
						Name: "rating",
						Type: "text",
					},
				},
			},
		},
	}

	ExecuteTests(t, TestCases{
		{
			name: "up SQL is mandatory",
			migrations: []migrations.Migration{
				createTableMigration,
				{
					Name: "02_change_type",
					Operations: migrations.Operations{
						&migrations.OpAlterColumn{
							Table:  "reviews",
							Column: "rating",
							Type:   ptr("integer"),
							Down:   "CAST (rating AS text)",
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
					Name: "02_change_type",
					Operations: migrations.Operations{
						&migrations.OpAlterColumn{
							Table:  "reviews",
							Column: "rating",
							Type:   ptr("integer"),
							Up:     "CAST (rating AS integer)",
						},
					},
				},
			},
			wantStartErr: migrations.FieldRequiredError{Name: "down"},
		},
	})
}
