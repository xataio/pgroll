package migrations_test

import (
	"database/sql"
	"testing"

	"pg-roll/pkg/migrations"
)

func TestDropIndex(t *testing.T) {
	t.Parallel()

	idxName := migrations.GenerateIndexName("users", []string{"name"})

	ExecuteTests(t, TestCases{{
		name: "drop index",
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
								Name:     "name",
								Type:     "varchar(255)",
								Nullable: false,
							},
						},
					},
				},
			},
			{
				Name: "02_create_index",
				Operations: migrations.Operations{
					&migrations.OpCreateIndex{
						Table:   "users",
						Columns: []string{"name"},
					},
				},
			},
			{
				Name: "03_drop_index",
				Operations: migrations.Operations{
					&migrations.OpDropIndex{
						Name: idxName,
					},
				},
			},
		},
		afterStart: func(t *testing.T, db *sql.DB) {
			// The index has not yet been dropped.
			IndexMustExist(t, db, "public", "users", idxName)
		},
		afterRollback: func(t *testing.T, db *sql.DB) {
			// Rollback is a no-op.
		},
		afterComplete: func(t *testing.T, db *sql.DB) {
			// The index has been removed from the underlying table.
			IndexMustNotExist(t, db, "public", "users", idxName)
		},
	}})
}
