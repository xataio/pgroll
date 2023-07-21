package migrations_test

import (
	"database/sql"
	"testing"

	"github.com/stretchr/testify/assert"

	"pg-roll/pkg/migrations"
)

func TestAddColumn(t *testing.T) {
	t.Parallel()

	ptr := func(s string) *string { return &s }

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
								Name:       "id",
								Type:       "serial",
								PrimaryKey: true,
							},
							{
								Name:   "name",
								Type:   "varchar(255)",
								Unique: true,
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
							Nullable: false,
							Default:  ptr("0"),
						},
					},
				},
			},
		},
		afterStart: func(t *testing.T, db *sql.DB) {
			// old and new views of the table should exist
			ViewMustExist(t, db, "public", "01_add_table", "users")
			ViewMustExist(t, db, "public", "02_add_column", "users")

			// inserting via both the old and the new views works
			MustInsert(t, db, "public", "01_add_table", "users", map[string]string{
				"name": "Alice",
			})
			MustInsert(t, db, "public", "02_add_column", "users", map[string]string{
				"name": "Bob",
				"age":  "21",
			})

			// selecting from both the old and the new views works
			resOld := MustSelect(t, db, "public", "01_add_table", "users")
			assert.Equal(t, []map[string]any{
				{"id": 1, "name": "Alice"},
				{"id": 2, "name": "Bob"},
			}, resOld)
			resNew := MustSelect(t, db, "public", "02_add_column", "users")
			assert.Equal(t, []map[string]any{
				{"id": 1, "name": "Alice", "age": 0},
				{"id": 2, "name": "Bob", "age": 21},
			}, resNew)
		},
		afterRollback: func(t *testing.T, db *sql.DB) {
			// The new column has been dropped from the underlying table
			columnName := migrations.TemporaryName("age")
			ColumnMustNotExist(t, db, "public", "users", columnName)

			// The table's column count reflects the drop of the new column
			TableMustHaveColumnCount(t, db, "public", "users", 2)
		},
		afterComplete: func(t *testing.T, db *sql.DB) {
			// The new view still exists
			ViewMustExist(t, db, "public", "02_add_column", "users")

			// Inserting into the new view still works
			MustInsert(t, db, "public", "02_add_column", "users", map[string]string{
				"name": "Carl",
				"age":  "31",
			})

			// Selecting from the new view still works
			res := MustSelect(t, db, "public", "02_add_column", "users")
			assert.Equal(t, []map[string]any{
				{"id": 1, "name": "Alice", "age": 0},
				{"id": 2, "name": "Bob", "age": 0},
				{"id": 3, "name": "Carl", "age": 31},
			}, res)
		},
	}})
}

func TestAddColumnWithUpSql(t *testing.T) {
	t.Parallel()

	ptr := func(s string) *string { return &s }

	ExecuteTests(t, TestCases{{
		name: "add column",
		migrations: []migrations.Migration{
			{
				Name: "01_add_table",
				Operations: migrations.Operations{
					&migrations.OpCreateTable{
						Name: "products",
						Columns: []migrations.Column{
							{
								Name:       "id",
								Type:       "serial",
								PrimaryKey: true,
							},
							{
								Name:   "name",
								Type:   "varchar(255)",
								Unique: true,
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
							Nullable: true,
						},
					},
				},
			},
		},
		afterStart: func(t *testing.T, db *sql.DB) {
			// inserting via both the old and the new views works
			MustInsert(t, db, "public", "01_add_table", "products", map[string]string{
				"name": "apple",
			})
			MustInsert(t, db, "public", "02_add_column", "products", map[string]string{
				"name":        "banana",
				"description": "a yellow banana",
			})

			res := MustSelect(t, db, "public", "02_add_column", "products")
			assert.Equal(t, []map[string]any{
				// the description column has been populated for the product inserted into the old view.
				{"id": 1, "name": "apple", "description": "APPLE"},
				// the description column for the product inserted into the new view is as inserted.
				{"id": 2, "name": "banana", "description": "a yellow banana"},
			}, res)
		},
		afterRollback: func(t *testing.T, db *sql.DB) {
			// The trigger function has been dropped.
			triggerFnName := migrations.TriggerFunctionName("products", "description")
			FunctionMustNotExist(t, db, "public", triggerFnName)

			// The trigger has been dropped.
			triggerName := migrations.TriggerName("products", "description")
			TriggerMustNotExist(t, db, "public", "products", triggerName)
		},
		afterComplete: func(t *testing.T, db *sql.DB) {
			// after rollback + restart + complete, all 'description' values are the backfilled ones.
			res := MustSelect(t, db, "public", "02_add_column", "products")
			assert.Equal(t, []map[string]any{
				{"id": 1, "name": "apple", "description": "APPLE"},
				{"id": 2, "name": "banana", "description": "BANANA"},
			}, res)
		},
	}})
}

func TestAddNotNullColumnWithNoDefault(t *testing.T) {
	t.Parallel()

	ptr := func(s string) *string { return &s }

	ExecuteTests(t, TestCases{{
		name: "add column",
		migrations: []migrations.Migration{
			{
				Name: "01_add_table",
				Operations: migrations.Operations{
					&migrations.OpCreateTable{
						Name: "products",
						Columns: []migrations.Column{
							{
								Name:       "id",
								Type:       "serial",
								PrimaryKey: true,
							},
							{
								Name:   "name",
								Type:   "varchar(255)",
								Unique: true,
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
							Nullable: false,
						},
					},
				},
			},
		},
		afterStart: func(t *testing.T, db *sql.DB) {
			// Inserting a null description through the old view works (due to `up` sql populating the column).
			MustInsert(t, db, "public", "01_add_table", "products", map[string]string{
				"name": "apple",
			})
			// Inserting a null description through the new view fails.
			MustNotInsert(t, db, "public", "02_add_column", "products", map[string]string{
				"name": "banana",
			})
		},
		afterRollback: func(t *testing.T, db *sql.DB) {
			// the check constraint has been dropped.
			constraintname := migrations.NotNullConstraintName("description")
			ConstraintMustNotExist(t, db, "public", "products", constraintname)
		},
		afterComplete: func(t *testing.T, db *sql.DB) {
		},
	}})
}
