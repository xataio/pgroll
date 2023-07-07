package migrations_test

import (
	"database/sql"
	"testing"

	"pg-roll/pkg/migrations"
)

func TestRenameTable(t *testing.T) {
	t.Parallel()

	ExecuteTests(t, TestCases{
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
								{
									Name: "name",
									Type: "text",
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
			afterStart: func(t *testing.T, db *sql.DB) {
				// check that the table with the new name can be accessed
				TableMustExist(t, db, "public", "01_create_table", "test_table")
				TableMustExist(t, db, "public", "02_rename_table", "renamed_table")

				// inserts work
				MustInsert(t, db, "public", "01_create_table", "test_table", map[string]string{
					"name": "foo",
				})
				MustInsert(t, db, "public", "02_rename_table", "renamed_table", map[string]string{
					"name": "bar",
				})
			},
			afterComplete: func(t *testing.T, db *sql.DB) {
				// the table still exists with the new name
				TableMustNotExist(t, db, "public", "02_rename_table", "testTable")
				TableMustExist(t, db, "public", "02_rename_table", "renamed_table")

				// inserts work
				MustInsert(t, db, "public", "02_rename_table", "renamed_table", map[string]string{
					"name": "baz",
				})
			},
		},
	})
}
