package migrations_test

import (
	"database/sql"
	"testing"

	"github.com/stretchr/testify/assert"

	"pg-roll/pkg/migrations"
)

func TestRenameColumn(t *testing.T) {
	t.Parallel()

	ExecuteTests(t, TestCases{{
		name: "rename column",
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
								Name:     "username",
								Type:     "varchar(255)",
								Nullable: false,
							},
						},
					},
				},
			},
			{
				Name: "02_rename_column",
				Operations: migrations.Operations{
					&migrations.OpRenameColumn{
						Table: "users",
						From:  "username",
						To:    "name",
					},
				},
			},
		},
		afterStart: func(t *testing.T, db *sql.DB) {
			// The column in the underlying table has not been renamed.
			ColumnMustExist(t, db, "public", "users", "username")

			// Insertions to the new column name in the new version schema should work.
			MustInsert(t, db, "public", "02_rename_column", "users", map[string]string{"name": "alice"})

			// Insertions to the old column name in the old version schema should work.
			MustInsert(t, db, "public", "01_add_table", "users", map[string]string{"username": "bob"})

			// Data can be read from the view in the new version schema.
			rows := MustSelect(t, db, "public", "02_rename_column", "users")
			assert.Equal(t, []map[string]any{
				{"id": 1, "name": "alice"},
				{"id": 2, "name": "bob"},
			}, rows)
		},
		afterRollback: func(t *testing.T, db *sql.DB) {
			// no-op
		},
		afterComplete: func(t *testing.T, db *sql.DB) {
			// The column in the underlying table has been renamed.
			ColumnMustExist(t, db, "public", "users", "name")

			// Data can be read from the view in the new version schema.
			rows := MustSelect(t, db, "public", "02_rename_column", "users")
			assert.Equal(t, []map[string]any{
				{"id": 1, "name": "alice"},
				{"id": 2, "name": "bob"},
			}, rows)
		},
	}})
}
