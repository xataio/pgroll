package migrations_test

import (
	"database/sql"
	"testing"

	"pg-roll/pkg/migrations"

	"github.com/stretchr/testify/assert"
)

func TestCreateTable(t *testing.T) {
	t.Parallel()

	ExecuteTests(t, TestCases{TestCase{
		name: "create table",
		migrations: []migrations.Migration{
			{
				Name: "01_create_table",
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
		},
		afterStart: func(t *testing.T, db *sql.DB) {
			// The new view exists in the new version schema.
			ViewMustExist(t, db, "public", "01_create_table", "users")

			// Data can be inserted into the new view.
			MustInsert(t, db, "public", "01_create_table", "users", map[string]string{
				"name": "Alice",
			})

			// Data can be retrieved from the new view.
			rows := MustSelect(t, db, "public", "01_create_table", "users")
			assert.Equal(t, []map[string]any{
				{"id": 1, "name": "Alice"},
			}, rows)
		},
		afterRollback: func(t *testing.T, db *sql.DB) {
			// The underlying table has been dropped.
			TableMustNotExist(t, db, "public", "users")
		},
		afterComplete: func(t *testing.T, db *sql.DB) {
			// The view still exists
			ViewMustExist(t, db, "public", "01_create_table", "users")

			// Data can be inserted into the new view.
			MustInsert(t, db, "public", "01_create_table", "users", map[string]string{
				"name": "Alice",
			})

			// Data can be retrieved from the new view.
			rows := MustSelect(t, db, "public", "01_create_table", "users")
			assert.Equal(t, []map[string]any{
				{"id": 1, "name": "Alice"},
			}, rows)
		},
	}})
}
