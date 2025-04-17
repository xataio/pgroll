// SPDX-License-Identifier: Apache-2.0

package migrations_test

import (
	"database/sql"
	"testing"

	"github.com/lib/pq"

	"github.com/xataio/pgroll/pkg/migrations"
)

func TestSQLInTransaction(t *testing.T) {
	t.Parallel()

	ExecuteTests(t, TestCases{
		{
			name: "simple SQL",
			migrations: []migrations.Migration{
				{
					Name: "01_create_table",
					Operations: migrations.Operations{
						&migrations.OpSQLInTransaction{
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
			afterStart: func(t *testing.T, db *sql.DB, schema string) {
				// view can be accessed after start
				ViewMustExist(t, db, schema, "01_create_table", "test_table")
				// table can be accessed after start
				TableMustExist(t, db, schema, "test_table")
				// inserts work
				MustInsert(t, db, schema, "01_create_table", "test_table", map[string]string{
					"name": "foo",
				})
			},
			afterRollback: func(t *testing.T, db *sql.DB, schema string) {
				// table is dropped after rollback
				TableMustNotExist(t, db, schema, "test_table")
			},
			afterComplete: func(t *testing.T, db *sql.DB, schema string) {
				TableMustExist(t, db, schema, "test_table")
				// inserts still work after complete
				MustInsert(t, db, schema, "01_create_table", "test_table", map[string]string{
					"name": "foo",
				})
			},
		},
		{
			name: "SQL statements that fails with no down migration",
			migrations: []migrations.Migration{
				{
					Name: "01_create_table",
					Operations: migrations.Operations{
						&migrations.OpSQLInTransaction{
							Up: `
								CREATE TABLE test_table (
									id serial,
									name text
								); SELECT * FROM bad_table;
							`,
						},
					},
				},
			},
			wantStartErr: &pq.Error{Message: "relation \"bad_table\" does not exist"},
			afterRollback: func(t *testing.T, db *sql.DB, schema string) {
				// table does not exist because start fails
				TableMustNotExist(t, db, schema, "test_table")
			},
			afterComplete: func(t *testing.T, db *sql.DB, schema string) {
				// table does not exist because start fails
				TableMustNotExist(t, db, schema, "test_table")
			},
		},
	})
}
