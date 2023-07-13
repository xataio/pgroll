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
