// SPDX-License-Identifier: Apache-2.0

package migrations_test

import (
	"database/sql"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/xataio/pgroll/pkg/migrations"
)

func TestRenameColumn(t *testing.T) {
	t.Parallel()

	addTableMigration := migrations.Migration{
		Name: "01_add_table",
		Operations: migrations.Operations{
			&migrations.OpCreateTable{
				Name: "users",
				Columns: []migrations.Column{
					{
						Name: "id",
						Type: "serial",
						Pk:   true,
					},
					{
						Name:     "username",
						Type:     "varchar(255)",
						Nullable: ptr(false),
					},
				},
			},
		},
	}

	ExecuteTests(t, TestCases{{
		name: "rename column",
		migrations: []migrations.Migration{
			addTableMigration,
			{
				Name: "02_rename_column",
				Operations: migrations.Operations{
					&migrations.OpAlterColumn{
						Table:  "users",
						Column: "username",
						Name:   ptr("name"),
					},
				},
			},
		},
		afterStart: func(t *testing.T, db *sql.DB, schema string) {
			// The column in the underlying table has not been renamed.
			ColumnMustExist(t, db, schema, "users", "username")

			// Insertions to the new column name in the new version schema should work.
			MustInsert(t, db, schema, "02_rename_column", "users", map[string]string{"name": "alice"})

			// Insertions to the old column name in the old version schema should work.
			MustInsert(t, db, schema, "01_add_table", "users", map[string]string{"username": "bob"})

			// Data can be read from the view in the new version schema.
			rows := MustSelect(t, db, schema, "02_rename_column", "users")
			assert.Equal(t, []map[string]any{
				{"id": 1, "name": "alice"},
				{"id": 2, "name": "bob"},
			}, rows)
		},
		afterRollback: func(t *testing.T, db *sql.DB, schema string) {
			// no-op
		},
		afterComplete: func(t *testing.T, db *sql.DB, schema string) {
			// The column in the underlying table has been renamed.
			ColumnMustExist(t, db, schema, "users", "name")

			// Data can be read from the view in the new version schema.
			rows := MustSelect(t, db, schema, "02_rename_column", "users")
			assert.Equal(t, []map[string]any{
				{"id": 1, "name": "alice"},
				{"id": 2, "name": "bob"},
			}, rows)
		},
	}, {
		name: "column must not exist",
		migrations: []migrations.Migration{
			addTableMigration,
			{
				Name: "02_rename_column",
				Operations: migrations.Operations{
					&migrations.OpAlterColumn{
						Table:  "users",
						Column: "username",
						Name:   ptr("id"),
					},
				},
			},
		},
		wantStartErr: migrations.ColumnAlreadyExistsError{Table: "users", Name: "id"},
	}})
}
