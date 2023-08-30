package migrations_test

import (
	"database/sql"
	"testing"

	"github.com/xataio/pg-roll/pkg/migrations"
)

func TestRawSQL(t *testing.T) {
	t.Parallel()

	ExecuteTests(t, TestCases{
		{
			name: "raw SQL",
			migrations: []migrations.Migration{
				{
					Name: "01_create_table",
					Operations: migrations.Operations{
						&migrations.OpRawSQL{
							Up: `
								CREATE TABLE test_table (
									id serial,
									name text
								)
							`,
							Down: `
								DROP TABLE test_table
							`,
						},
					},
				},
			},
			afterStart: func(t *testing.T, db *sql.DB) {
				// table can be accessed after start
				ViewMustExist(t, db, "public", "01_create_table", "test_table")

				// inserts work
				MustInsert(t, db, "public", "01_create_table", "test_table", map[string]string{
					"name": "foo",
				})
			},
			afterRollback: func(t *testing.T, db *sql.DB) {
				// table is dropped after rollback
				TableMustNotExist(t, db, "public", "test_table")
			},
			afterComplete: func(t *testing.T, db *sql.DB) {
				// inserts still work after complete
				MustInsert(t, db, "public", "01_create_table", "test_table", map[string]string{
					"name": "foo",
				})
			},
		},
		{
			name: "migration on top of raw SQL",
			migrations: []migrations.Migration{
				{
					Name: "01_create_table",
					Operations: migrations.Operations{
						&migrations.OpRawSQL{
							Up: `
								CREATE TABLE test_table (
									id serial,
									name text
								)
							`,
							Down: `
								DROP TABLE test_table
							`,
						},
					},
				},
				{
					Name: "02_rename_table",
					Operations: migrations.Operations{
						&migrations.OpRenameTable{
							From: "test_table",
							To:   "test_table_renamed",
						},
					},
				},
			},
			afterStart: func(t *testing.T, db *sql.DB) {
				// table can be accessed after start
				ViewMustExist(t, db, "public", "01_create_table", "test_table")

				// table is renamed in new version
				ViewMustExist(t, db, "public", "02_rename_table", "test_table_renamed")

				// inserts work
				MustInsert(t, db, "public", "01_create_table", "test_table", map[string]string{
					"name": "foo",
				})
				MustInsert(t, db, "public", "02_rename_table", "test_table_renamed", map[string]string{
					"name": "foo",
				})
			},
			afterComplete: func(t *testing.T, db *sql.DB) {
				// table can still be accessed after complete
				ViewMustExist(t, db, "public", "02_rename_table", "test_table_renamed")

				// inserts work
				MustInsert(t, db, "public", "02_rename_table", "test_table_renamed", map[string]string{
					"name": "foo",
				})
			},
		},
	})
}
