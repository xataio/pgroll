// SPDX-License-Identifier: Apache-2.0

package migrations_test

import (
	"database/sql"
	"fmt"
	"testing"

	"github.com/xataio/pgroll/pkg/migrations"
	"github.com/xataio/pgroll/pkg/roll"
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
			afterStart: func(t *testing.T, db *sql.DB, schema string) {
				// table can be accessed after start
				ViewMustExist(t, db, schema, "01_create_table", "test_table")

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
				// inserts still work after complete
				MustInsert(t, db, schema, "01_create_table", "test_table", map[string]string{
					"name": "foo",
				})
			},
		},
		{
			name: "raw SQL with onComplete",
			migrations: []migrations.Migration{
				{
					Name: "01_create_table",
					Operations: migrations.Operations{
						&migrations.OpRawSQL{
							OnComplete: true,
							Up: `
								CREATE TABLE test_table (
									id serial,
									name text
								)
							`,
						},
					},
				},
			},
			afterStart: func(t *testing.T, db *sql.DB, schema string) {
				// SQL didn't run yet
				TableMustNotExist(t, db, schema, "test_table")
			},
			afterComplete: func(t *testing.T, db *sql.DB, schema string) {
				// table can be accessed after start
				TableMustExist(t, db, schema, "test_table")

				// inserts work
				MustInsert(t, db, schema, "01_create_table", "test_table", map[string]string{
					"name": "foo",
				})
			},
		},
		{
			name: "raw SQL after a migration with onComplete",
			migrations: []migrations.Migration{
				{
					Name: "01_create_table",
					Operations: migrations.Operations{
						&migrations.OpCreateTable{
							Name: "test_table",
							Columns: []migrations.Column{
								{Name: "id", Type: "serial"},
								{Name: "name", Type: "text"},
							},
						},
						&migrations.OpRawSQL{
							OnComplete: true,
							Up: `
								ALTER TABLE test_table ADD COLUMN age int
							`,
						},
					},
				},
			},
			afterStart: func(t *testing.T, db *sql.DB, schema string) {
				// SQL didn't run yet
				ViewMustExist(t, db, schema, "01_create_table", "test_table")
				ColumnMustNotExist(t, db, schema, "test_table", "age")
			},
			afterRollback: func(t *testing.T, db *sql.DB, schema string) {
				// table is dropped after rollback
				TableMustNotExist(t, db, schema, "test_table")
			},
			afterComplete: func(t *testing.T, db *sql.DB, schema string) {
				// table can be accessed after start
				TableMustExist(t, db, schema, "test_table")
				ColumnMustExist(t, db, schema, "test_table", "age")

				// inserts work
				MustInsert(t, db, schema, "01_create_table", "test_table", map[string]string{
					"name": "foo",
					"age":  "42",
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
			afterStart: func(t *testing.T, db *sql.DB, schema string) {
				// table can be accessed after start
				ViewMustExist(t, db, schema, "01_create_table", "test_table")

				// table is renamed in new version
				ViewMustExist(t, db, schema, "02_rename_table", "test_table_renamed")

				// inserts work
				MustInsert(t, db, schema, "01_create_table", "test_table", map[string]string{
					"name": "foo",
				})
				MustInsert(t, db, schema, "02_rename_table", "test_table_renamed", map[string]string{
					"name": "foo",
				})
			},
			afterComplete: func(t *testing.T, db *sql.DB, schema string) {
				// table can still be accessed after complete
				ViewMustExist(t, db, schema, "02_rename_table", "test_table_renamed")

				// inserts work
				MustInsert(t, db, schema, "02_rename_table", "test_table_renamed", map[string]string{
					"name": "foo",
				})
			},
		},
	})
}

func TestRawSQLTransformation(t *testing.T) {
	t.Parallel()

	t.Run("for normal raw SQL operations with up and down SQL", func(t *testing.T) {
		ExecuteTests(t, TestCases{
			{
				name: "SQL transformer rewrites up and down SQL",
				migrations: []migrations.Migration{
					{
						Name: "01_create_table",
						Operations: migrations.Operations{
							&migrations.OpRawSQL{
								Up:   "CREATE TABLE apples(id int)",
								Down: "CREATE TABLE bananas(id int)",
							},
						},
					},
				},
				afterStart: func(t *testing.T, db *sql.DB, schema string) {
					// The transformed `up` SQL was used in place of the original SQL
					TableMustExist(t, db, schema, "table_1")
					TableMustNotExist(t, db, schema, "apples")
				},
				afterRollback: func(t *testing.T, db *sql.DB, schema string) {
					// The transformed `down` SQL was used in place of the original SQL
					TableMustExist(t, db, schema, "table_2")
					TableMustNotExist(t, db, schema, "bananas")
				},
				afterComplete: func(t *testing.T, db *sql.DB, schema string) {
				},
			},
		}, roll.WithSQLTransformer(&simpleSQLTransformer{}))
	})

	t.Run("for raw SQL operations that run on complete", func(t *testing.T) {
		ExecuteTests(t, TestCases{
			{
				name: "SQL transformer rewrites up SQL when up is run on completion",
				migrations: []migrations.Migration{
					{
						Name: "01_create_table",
						Operations: migrations.Operations{
							&migrations.OpRawSQL{
								Up:         "CREATE TABLE apples(id int)",
								OnComplete: true,
							},
						},
					},
				},
				afterStart: func(t *testing.T, db *sql.DB, schema string) {
				},
				afterRollback: func(t *testing.T, db *sql.DB, schema string) {
				},
				afterComplete: func(t *testing.T, db *sql.DB, schema string) {
					// The transformed `up` SQL was used in place of the original SQL
					TableMustExist(t, db, schema, "table_1")
					TableMustNotExist(t, db, schema, "apples")
				},
			},
		}, roll.WithSQLTransformer(&simpleSQLTransformer{}))
	})
}

type simpleSQLTransformer struct {
	counter int
}

func (s *simpleSQLTransformer) TransformSQL(sql string) (string, error) {
	s.counter++
	return fmt.Sprintf("CREATE TABLE table_%d(id int)", s.counter), nil
}
