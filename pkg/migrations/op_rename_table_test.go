package migrations_test

import (
	"database/sql"
	"testing"

	"pg-roll/pkg/migrations"
)

func TestRenameTable(t *testing.T) {
	t.Parallel()

	tests := TestCases{
		{
			name: "rename table",
			migrations: []migrations.Migration{
				{
					Name: "01_create_table",
					Operations: migrations.Operations{
						&migrations.OpCreateTable{
							Name: "test_table",
							Columns: []migrations.Column{
								{
									Name: "id",
									Type: "serial",
								},
							},
						},
					},
				},
				{
					Name: "02_rename_table",
					Operations: migrations.Operations{
						&migrations.OpRenameTable{
							From: "test_table",
							To:   "renamed_table",
						},
					},
				},
			},
			beforeComplete: func(t *testing.T, db *sql.DB) {
				// check that the table with the new name can be accessed
				TableMustExist(t, db, "public", "01_create_table", "test_table")
				TableMustExist(t, db, "public", "02_rename_table", "renamed_table")
			},
			afterComplete: func(t *testing.T, db *sql.DB) {
				// the table still exists with the new name
				TableMustExist(t, db, "public", "02_rename_table", "renamed_table")
				TableMustNotExist(t, db, "public", "02_rename_table", "testTable")
			},
		},
	}

	ExecuteTests(t, tests)
}
