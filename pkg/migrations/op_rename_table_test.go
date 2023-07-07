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
				ViewMustExist(t, db, "public", "01_create_table", "test_table")
				ViewMustExist(t, db, "public", "02_rename_table", "renamed_table")
			},
			afterComplete: func(t *testing.T, db *sql.DB) {
				ViewMustExist(t, db, "public", "02_rename_table", "renamed_table")
			},
		},
	}

	ExecuteTests(t, tests)
}
