// SPDX-License-Identifier: Apache-2.0

package migrations_test

import (
	"database/sql"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/xataio/pgroll/pkg/migrations"
	"github.com/xataio/pgroll/pkg/testutils"
)

func TestAddColumn(t *testing.T) {
	t.Parallel()

	ExecuteTests(t, TestCases{{
		name: "add column",
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
								Name:   "name",
								Type:   "varchar(255)",
								Unique: ptr(true),
							},
						},
					},
				},
			},
			{
				Name: "02_add_column",
				Operations: migrations.Operations{
					&migrations.OpAddColumn{
						Table: "users",
						Column: migrations.Column{
							Name:     "age",
							Type:     "integer",
							Nullable: ptr(false),
							Default:  ptr("0"),
							Comment:  ptr("the age of the user"),
						},
					},
				},
			},
		},
		afterStart: func(t *testing.T, db *sql.DB, schema string) {
			// old and new views of the table should exist
			ViewMustExist(t, db, schema, "01_add_table", "users")
			ViewMustExist(t, db, schema, "02_add_column", "users")

			// inserting via both the old and the new views works
			MustInsert(t, db, schema, "01_add_table", "users", map[string]string{
				"name": "Alice",
			})
			MustInsert(t, db, schema, "02_add_column", "users", map[string]string{
				"name": "Bob",
				"age":  "21",
			})

			// selecting from both the old and the new views works
			resOld := MustSelect(t, db, schema, "01_add_table", "users")
			assert.Equal(t, []map[string]any{
				{"id": 1, "name": "Alice"},
				{"id": 2, "name": "Bob"},
			}, resOld)
			resNew := MustSelect(t, db, schema, "02_add_column", "users")
			assert.Equal(t, []map[string]any{
				{"id": 1, "name": "Alice", "age": 0},
				{"id": 2, "name": "Bob", "age": 21},
			}, resNew)
		},
		afterRollback: func(t *testing.T, db *sql.DB, schema string) {
			// The new column has been dropped from the underlying table
			columnName := migrations.TemporaryName("age")
			ColumnMustNotExist(t, db, schema, "users", columnName)

			// The table's column count reflects the drop of the new column
			TableMustHaveColumnCount(t, db, schema, "users", 2)
		},
		afterComplete: func(t *testing.T, db *sql.DB, schema string) {
			// The new view still exists
			ViewMustExist(t, db, schema, "02_add_column", "users")

			// Inserting into the new view still works
			MustInsert(t, db, schema, "02_add_column", "users", map[string]string{
				"name": "Carl",
				"age":  "31",
			})

			// Selecting from the new view still works
			res := MustSelect(t, db, schema, "02_add_column", "users")
			assert.Equal(t, []map[string]any{
				{"id": 1, "name": "Alice", "age": 0},
				{"id": 2, "name": "Bob", "age": 0},
				{"id": 3, "name": "Carl", "age": 31},
			}, res)
		},
	}})
}

func TestAddForeignKeyColumn(t *testing.T) {
	t.Parallel()

	ExecuteTests(t, TestCases{
		{
			name: "add nullable foreign key column",
			migrations: []migrations.Migration{
				{
					Name: "01_create_table",
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
									Name:   "name",
									Type:   "varchar(255)",
									Unique: ptr(true),
								},
							},
						},
						&migrations.OpCreateTable{
							Name: "orders",
							Columns: []migrations.Column{
								{
									Name: "id",
									Type: "serial",
									Pk:   ptr(true),
								},
								{
									Name: "quantity",
									Type: "integer",
								},
							},
						},
					},
				},
				{
					Name: "02_add_column",
					Operations: migrations.Operations{
						&migrations.OpAddColumn{
							Table: "orders",
							Column: migrations.Column{
								Name: "user_id",
								Type: "integer",
								References: &migrations.ForeignKeyReference{
									Name:   "fk_users_id",
									Table:  "users",
									Column: "id",
								},
								Nullable: ptr(true),
							},
						},
					},
				},
			},
			afterStart: func(t *testing.T, db *sql.DB, schema string) {
				// The foreign key constraint exists on the new table.
				ValidatedForeignKeyMustExist(t, db, schema, "orders", "fk_users_id")

				// Inserting a row into the referenced table succeeds.
				MustInsert(t, db, schema, "01_create_table", "users", map[string]string{
					"name": "alice",
				})

				// Inserting a row into the referencing table succeeds as the referenced row exists.
				MustInsert(t, db, schema, "02_add_column", "orders", map[string]string{
					"user_id":  "1",
					"quantity": "100",
				})

				// Inserting a row into the referencing table fails as the referenced row does not exist.
				MustNotInsert(t, db, schema, "02_add_column", "orders", map[string]string{
					"user_id":  "2",
					"quantity": "200",
				}, testutils.FKViolationErrorCode)
			},
			afterRollback: func(t *testing.T, db *sql.DB, schema string) {
				// The new column has been dropped, so the foreign key constraint is gone.
			},
			afterComplete: func(t *testing.T, db *sql.DB, schema string) {
				// The foreign key constraint still exists on the new table
				ValidatedForeignKeyMustExist(t, db, schema, "orders", "fk_users_id")

				// Inserting a row into the referenced table succeeds.
				MustInsert(t, db, schema, "02_add_column", "users", map[string]string{
					"name": "bob",
				})

				// Inserting a row into the referencing table succeeds as the referenced row exists.
				MustInsert(t, db, schema, "02_add_column", "orders", map[string]string{
					"user_id":  "2",
					"quantity": "200",
				})

				// Inserting a row into the referencing table fails as the referenced row does not exist.
				MustNotInsert(t, db, schema, "02_add_column", "orders", map[string]string{
					"user_id":  "3",
					"quantity": "300",
				}, testutils.FKViolationErrorCode)
			},
		},
		{
			name: "add non-nullable foreign key column",
			migrations: []migrations.Migration{
				{
					Name: "01_create_table",
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
									Name:   "name",
									Type:   "varchar(255)",
									Unique: ptr(true),
								},
							},
						},
						&migrations.OpCreateTable{
							Name: "orders",
							Columns: []migrations.Column{
								{
									Name: "id",
									Type: "serial",
									Pk:   ptr(true),
								},
								{
									Name: "quantity",
									Type: "integer",
								},
							},
						},
					},
				},
				{
					Name: "02_add_column",
					Operations: migrations.Operations{
						&migrations.OpAddColumn{
							Table: "orders",
							Column: migrations.Column{
								Name: "user_id",
								Type: "integer",
								References: &migrations.ForeignKeyReference{
									Name:   "fk_users_id",
									Table:  "users",
									Column: "id",
								},
								Nullable: ptr(false),
							},
							Up: ptr("1"),
						},
					},
				},
			},
			afterStart: func(t *testing.T, db *sql.DB, schema string) {
				// The foreign key constraint exists on the new table.
				ValidatedForeignKeyMustExist(t, db, schema, "orders", "fk_users_id")

				// Inserting a row into the referenced table succeeds.
				MustInsert(t, db, schema, "01_create_table", "users", map[string]string{
					"name": "alice",
				})

				// Inserting a row into the referencing table succeeds as the referenced row exists.
				MustInsert(t, db, schema, "02_add_column", "orders", map[string]string{
					"user_id":  "1",
					"quantity": "100",
				})

				// Inserting a row into the referencing table fails as the referenced row does not exist.
				MustNotInsert(t, db, schema, "02_add_column", "orders", map[string]string{
					"user_id":  "2",
					"quantity": "200",
				}, testutils.FKViolationErrorCode)
			},
			afterRollback: func(t *testing.T, db *sql.DB, schema string) {
				// The new column has been dropped, so the foreign key constraint is gone.
			},
			afterComplete: func(t *testing.T, db *sql.DB, schema string) {
				// The foreign key constraint still exists on the new table
				ValidatedForeignKeyMustExist(t, db, schema, "orders", "fk_users_id")

				// Inserting a row into the referenced table succeeds.
				MustInsert(t, db, schema, "02_add_column", "users", map[string]string{
					"name": "bob",
				})

				// Inserting a row into the referencing table succeeds as the referenced row exists.
				MustInsert(t, db, schema, "02_add_column", "orders", map[string]string{
					"user_id":  "2",
					"quantity": "200",
				})

				// Inserting a row into the referencing table fails as the referenced row does not exist.
				MustNotInsert(t, db, schema, "02_add_column", "orders", map[string]string{
					"user_id":  "3",
					"quantity": "300",
				}, testutils.FKViolationErrorCode)
			},
		},
	})
}

func TestAddColumnWithUpSql(t *testing.T) {
	t.Parallel()

	ExecuteTests(t, TestCases{
		{
			name: "add column with up sql and a serial pk",
			migrations: []migrations.Migration{
				{
					Name: "01_add_table",
					Operations: migrations.Operations{
						&migrations.OpCreateTable{
							Name: "products",
							Columns: []migrations.Column{
								{
									Name: "id",
									Type: "serial",
									Pk:   ptr(true),
								},
								{
									Name:   "name",
									Type:   "varchar(255)",
									Unique: ptr(true),
								},
							},
						},
					},
				},
				{
					Name: "02_add_column",
					Operations: migrations.Operations{
						&migrations.OpAddColumn{
							Table: "products",
							Up:    ptr("UPPER(name)"),
							Column: migrations.Column{
								Name:     "description",
								Type:     "varchar(255)",
								Nullable: ptr(true),
							},
						},
					},
				},
			},
			afterStart: func(t *testing.T, db *sql.DB, schema string) {
				// inserting via both the old and the new views works
				MustInsert(t, db, schema, "01_add_table", "products", map[string]string{
					"name": "apple",
				})
				MustInsert(t, db, schema, "02_add_column", "products", map[string]string{
					"name":        "banana",
					"description": "a yellow banana",
				})

				res := MustSelect(t, db, schema, "02_add_column", "products")
				assert.Equal(t, []map[string]any{
					// the description column has been populated for the product inserted into the old view.
					{"id": 1, "name": "apple", "description": "APPLE"},
					// the description column for the product inserted into the new view is as inserted.
					{"id": 2, "name": "banana", "description": "a yellow banana"},
				}, res)
			},
			afterRollback: func(t *testing.T, db *sql.DB, schema string) {
				// The trigger function has been dropped.
				triggerFnName := migrations.TriggerFunctionName("products", "description")
				FunctionMustNotExist(t, db, schema, triggerFnName)

				// The trigger has been dropped.
				triggerName := migrations.TriggerName("products", "description")
				TriggerMustNotExist(t, db, schema, "products", triggerName)
			},
			afterComplete: func(t *testing.T, db *sql.DB, schema string) {
				// after rollback + restart + complete, all 'description' values are the backfilled ones.
				res := MustSelect(t, db, schema, "02_add_column", "products")
				assert.Equal(t, []map[string]any{
					{"id": 1, "name": "apple", "description": "APPLE"},
					{"id": 2, "name": "banana", "description": "BANANA"},
				}, res)

				// The trigger function has been dropped.
				triggerFnName := migrations.TriggerFunctionName("products", "description")
				FunctionMustNotExist(t, db, schema, triggerFnName)

				// The trigger has been dropped.
				triggerName := migrations.TriggerName("products", "description")
				TriggerMustNotExist(t, db, schema, "products", triggerName)
			},
		},
		{
			name: "add column with up sql and a text pk",
			migrations: []migrations.Migration{
				{
					Name: "01_add_table",
					Operations: migrations.Operations{
						&migrations.OpCreateTable{
							Name: "products",
							Columns: []migrations.Column{
								{
									Name: "id",
									Type: "text",
									Pk:   ptr(true),
								},
								{
									Name:   "name",
									Type:   "varchar(255)",
									Unique: ptr(true),
								},
							},
						},
					},
				},
				{
					Name: "02_add_column",
					Operations: migrations.Operations{
						&migrations.OpAddColumn{
							Table: "products",
							Up:    ptr("UPPER(name)"),
							Column: migrations.Column{
								Name:     "description",
								Type:     "varchar(255)",
								Nullable: ptr(true),
							},
						},
					},
				},
			},
			afterStart: func(t *testing.T, db *sql.DB, schema string) {
				// inserting via both the old and the new views works
				MustInsert(t, db, schema, "01_add_table", "products", map[string]string{
					"id":   "a",
					"name": "apple",
				})
				MustInsert(t, db, schema, "02_add_column", "products", map[string]string{
					"id":          "b",
					"name":        "banana",
					"description": "a yellow banana",
				})

				res := MustSelect(t, db, schema, "02_add_column", "products")
				assert.Equal(t, []map[string]any{
					// the description column has been populated for the product inserted into the old view.
					{"id": "a", "name": "apple", "description": "APPLE"},
					// the description column for the product inserted into the new view is as inserted.
					{"id": "b", "name": "banana", "description": "a yellow banana"},
				}, res)
			},
			afterRollback: func(t *testing.T, db *sql.DB, schema string) {
				// The trigger function has been dropped.
				triggerFnName := migrations.TriggerFunctionName("products", "description")
				FunctionMustNotExist(t, db, schema, triggerFnName)

				// The trigger has been dropped.
				triggerName := migrations.TriggerName("products", "description")
				TriggerMustNotExist(t, db, schema, "products", triggerName)
			},
			afterComplete: func(t *testing.T, db *sql.DB, schema string) {
				// after rollback + restart + complete, all 'description' values are the backfilled ones.
				res := MustSelect(t, db, schema, "02_add_column", "products")
				assert.Equal(t, []map[string]any{
					{"id": "a", "name": "apple", "description": "APPLE"},
					{"id": "b", "name": "banana", "description": "BANANA"},
				}, res)

				// The trigger function has been dropped.
				triggerFnName := migrations.TriggerFunctionName("products", "description")
				FunctionMustNotExist(t, db, schema, triggerFnName)

				// The trigger has been dropped.
				triggerName := migrations.TriggerName("products", "description")
				TriggerMustNotExist(t, db, schema, "products", triggerName)
			},
		},
		{
			name: "add column with up sql and no pk",
			migrations: []migrations.Migration{
				{
					Name: "01_add_table",
					Operations: migrations.Operations{
						&migrations.OpCreateTable{
							Name: "products",
							Columns: []migrations.Column{
								{
									Name: "id",
									Type: "text",
								},
								{
									Name:     "name",
									Type:     "varchar(255)",
									Unique:   ptr(true),
									Nullable: ptr(false),
								},
							},
						},
						// insert some data into the table to test backfill in the next migration
						&migrations.OpRawSQL{
							Up:         "INSERT INTO products (id, name) VALUES ('c', 'cherries')",
							OnComplete: true,
						},
					},
				},
				{
					Name: "02_add_column",
					Operations: migrations.Operations{
						&migrations.OpAddColumn{
							Table: "products",
							Up:    ptr("UPPER(name)"),
							Column: migrations.Column{
								Name:     "description",
								Type:     "varchar(255)",
								Nullable: ptr(true),
							},
						},
					},
				},
			},
			afterStart: func(t *testing.T, db *sql.DB, schema string) {
				// inserting via both the old and the new views works
				MustInsert(t, db, schema, "01_add_table", "products", map[string]string{
					"id":   "a",
					"name": "apple",
				})
				MustInsert(t, db, schema, "02_add_column", "products", map[string]string{
					"id":          "b",
					"name":        "banana",
					"description": "a yellow banana",
				})

				res := MustSelect(t, db, schema, "02_add_column", "products")
				assert.Equal(t, []map[string]any{
					// the description column has been populated by the backfill process
					{"id": "c", "name": "cherries", "description": "CHERRIES"},
					// the description column has been populated for the product inserted into the old view.
					{"id": "a", "name": "apple", "description": "APPLE"},
					// the description column for the product inserted into the new view is as inserted.
					{"id": "b", "name": "banana", "description": "a yellow banana"},
				}, res)
			},
			afterRollback: func(t *testing.T, db *sql.DB, schema string) {
				// The trigger function has been dropped.
				triggerFnName := migrations.TriggerFunctionName("products", "description")
				FunctionMustNotExist(t, db, schema, triggerFnName)

				// The trigger has been dropped.
				triggerName := migrations.TriggerName("products", "description")
				TriggerMustNotExist(t, db, schema, "products", triggerName)
			},
			afterComplete: func(t *testing.T, db *sql.DB, schema string) {
				// after rollback + restart + complete, all 'description' values are the backfilled ones.
				res := MustSelect(t, db, schema, "02_add_column", "products")
				assert.Equal(t, []map[string]any{
					{"id": "a", "name": "apple", "description": "APPLE"},
					{"id": "b", "name": "banana", "description": "BANANA"},
					{"id": "c", "name": "cherries", "description": "CHERRIES"},
				}, res)

				// The trigger function has been dropped.
				triggerFnName := migrations.TriggerFunctionName("products", "description")
				FunctionMustNotExist(t, db, schema, triggerFnName)

				// The trigger has been dropped.
				triggerName := migrations.TriggerName("products", "description")
				TriggerMustNotExist(t, db, schema, "products", triggerName)
			},
		},
	})
}

func TestAddNotNullColumnWithNoDefault(t *testing.T) {
	t.Parallel()

	ExecuteTests(t, TestCases{{
		name: "add not null column with no default",
		migrations: []migrations.Migration{
			{
				Name: "01_add_table",
				Operations: migrations.Operations{
					&migrations.OpCreateTable{
						Name: "products",
						Columns: []migrations.Column{
							{
								Name: "id",
								Type: "serial",
								Pk:   ptr(true),
							},
							{
								Name:   "name",
								Type:   "varchar(255)",
								Unique: ptr(true),
							},
						},
					},
				},
			},
			{
				Name: "02_add_column",
				Operations: migrations.Operations{
					&migrations.OpAddColumn{
						Table: "products",
						Up:    ptr("UPPER(name)"),
						Column: migrations.Column{
							Name:     "description",
							Type:     "varchar(255)",
							Nullable: ptr(false),
						},
					},
				},
			},
		},
		afterStart: func(t *testing.T, db *sql.DB, schema string) {
			// Inserting a null description through the old view works (due to `up` sql populating the column).
			MustInsert(t, db, schema, "01_add_table", "products", map[string]string{
				"name": "apple",
			})
			// Inserting a null description through the new view fails.
			MustNotInsert(t, db, schema, "02_add_column", "products", map[string]string{
				"name": "banana",
			}, testutils.CheckViolationErrorCode)
		},
		afterRollback: func(t *testing.T, db *sql.DB, schema string) {
			// the check constraint has been dropped.
			constraintName := migrations.NotNullConstraintName("description")
			CheckConstraintMustNotExist(t, db, schema, "products", constraintName)
		},
		afterComplete: func(t *testing.T, db *sql.DB, schema string) {
			// the check constraint has been dropped.
			constraintName := migrations.NotNullConstraintName("description")
			CheckConstraintMustNotExist(t, db, schema, "products", constraintName)

			// can't insert a null description into the new view; the column now has a NOT NULL constraint.
			MustNotInsert(t, db, schema, "02_add_column", "products", map[string]string{
				"name": "orange",
			}, testutils.NotNullViolationErrorCode)
		},
	}})
}

func TestAddColumnValidation(t *testing.T) {
	t.Parallel()

	addTableMigration := migrations.Migration{
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
						Name:   "name",
						Type:   "varchar(255)",
						Unique: ptr(true),
					},
				},
			},
		},
	}

	ExecuteTests(t, TestCases{
		{
			name: "table must exist",
			migrations: []migrations.Migration{
				addTableMigration,
				{
					Name: "02_add_column",
					Operations: migrations.Operations{
						&migrations.OpAddColumn{
							Table: "doesntexist",
							Column: migrations.Column{
								Name:     "age",
								Type:     "integer",
								Nullable: ptr(false),
								Default:  ptr("0"),
							},
						},
					},
				},
			},
			wantStartErr: migrations.TableDoesNotExistError{Name: "doesntexist"},
		},
		{
			name: "column must not exist",
			migrations: []migrations.Migration{
				addTableMigration,
				{
					Name: "02_add_column",
					Operations: migrations.Operations{
						&migrations.OpAddColumn{
							Table: "users",
							Column: migrations.Column{
								Name: "name",
								Type: "varchar(255)",
							},
						},
					},
				},
			},
			wantStartErr: migrations.ColumnAlreadyExistsError{Table: "users", Name: "name"},
		},
		{
			name: "up SQL is mandatory when adding a NOT NULL column with no DEFAULT",
			migrations: []migrations.Migration{
				addTableMigration,
				{
					Name: "02_add_column",
					Operations: migrations.Operations{
						&migrations.OpAddColumn{
							Table: "users",
							Column: migrations.Column{
								Name:     "age",
								Type:     "integer",
								Nullable: ptr(false),
							},
						},
					},
				},
			},
			wantStartErr: migrations.FieldRequiredError{Name: "up"},
		},
		{
			name: "table must have a primary key on exactly one column if up is defined",
			migrations: []migrations.Migration{
				{
					Name: "01_add_table",
					Operations: migrations.Operations{
						&migrations.OpRawSQL{
							Up:   "CREATE TABLE orders(id integer, order_id integer, name text, primary key (id, order_id))",
							Down: "DROP TABLE orders",
						},
					},
				},
				{
					Name: "02_add_column",
					Operations: migrations.Operations{
						&migrations.OpAddColumn{
							Table: "orders",
							Up:    ptr("UPPER(name)"),
							Column: migrations.Column{
								Name: "description",
								Type: "text",
							},
						},
					},
				},
			},
			wantStartErr: migrations.BackfillNotPossibleError{Table: "orders"},
		},
		{
			name: "table has no restrictions on primary keys if up is not defined",
			migrations: []migrations.Migration{
				{
					Name: "01_add_table",
					Operations: migrations.Operations{
						&migrations.OpRawSQL{
							Up:   "CREATE TABLE orders(id integer, order_id integer, name text, primary key (id, order_id))",
							Down: "DROP TABLE orders",
						},
					},
				},
				{
					Name: "02_add_column",
					Operations: migrations.Operations{
						&migrations.OpAddColumn{
							Table: "orders",
							Column: migrations.Column{
								Default: ptr("'foo'"),
								Name:    "description",
								Type:    "text",
							},
						},
					},
				},
			},
			wantStartErr: nil,
		},
	})
}

func TestAddColumnWithCheckConstraint(t *testing.T) {
	t.Parallel()

	ExecuteTests(t, TestCases{{
		name: "add column",
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
								Name:   "name",
								Type:   "varchar(255)",
								Unique: ptr(true),
							},
						},
					},
				},
			},
			{
				Name: "02_add_column",
				Operations: migrations.Operations{
					&migrations.OpAddColumn{
						Table: "users",
						Column: migrations.Column{
							Name:    "age",
							Type:    "integer",
							Default: ptr("18"),
							Check: &migrations.CheckConstraint{
								Name:       "age_check",
								Constraint: "age >= 18",
							},
						},
					},
				},
			},
		},
		afterStart: func(t *testing.T, db *sql.DB, schema string) {
			// Inserting a row that meets the constraint into the new view succeeds.
			MustInsert(t, db, schema, "02_add_column", "users", map[string]string{
				"name": "alice",
				"age":  "30",
			})

			// Inserting a row that does not meet the constraint into the new view fails.
			MustNotInsert(t, db, schema, "02_add_column", "users", map[string]string{
				"name": "bob",
				"age":  "3",
			}, testutils.CheckViolationErrorCode)
		},
		afterRollback: func(t *testing.T, db *sql.DB, schema string) {
		},
		afterComplete: func(t *testing.T, db *sql.DB, schema string) {
			// Inserting a row that meets the constraint into the new view succeeds.
			MustInsert(t, db, schema, "02_add_column", "users", map[string]string{
				"name": "carl",
				"age":  "30",
			})

			// Inserting a row that does not meet the constraint into the new view fails.
			MustNotInsert(t, db, schema, "02_add_column", "users", map[string]string{
				"name": "dana",
				"age":  "3",
			}, testutils.CheckViolationErrorCode)
		},
	}})
}

func TestAddColumnWithComment(t *testing.T) {
	t.Parallel()

	ExecuteTests(t, TestCases{{
		name: "add column",
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
								Name:   "name",
								Type:   "varchar(255)",
								Unique: ptr(true),
							},
						},
					},
				},
			},
			{
				Name: "02_add_column",
				Operations: migrations.Operations{
					&migrations.OpAddColumn{
						Table: "users",
						Column: migrations.Column{
							Name:     "age",
							Type:     "integer",
							Nullable: ptr(false),
							Default:  ptr("0"),
							Comment:  ptr("the age of the user"),
						},
					},
				},
			},
		},
		afterStart: func(t *testing.T, db *sql.DB, schema string) {
			// The comment has been added to the underlying column.
			columnName := migrations.TemporaryName("age")
			ColumnMustHaveComment(t, db, schema, "users", columnName, "the age of the user")
		},
		afterRollback: func(t *testing.T, db *sql.DB, schema string) {
		},
		afterComplete: func(t *testing.T, db *sql.DB, schema string) {
			// The comment is still present on the underlying column.
			ColumnMustHaveComment(t, db, schema, "users", "age", "the age of the user")
		},
	}})
}
