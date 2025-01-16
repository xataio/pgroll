// SPDX-License-Identifier: Apache-2.0

package migrations_test

import (
	"database/sql"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/xataio/pgroll/pkg/migrations"
)

func TestOpRenameColumn(t *testing.T) {
	t.Parallel()

	ExecuteTests(t, TestCases{
		{
			name: "rename column",
			migrations: []migrations.Migration{
				{
					Name: "01_create_table",
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
			afterStart: func(t *testing.T, db *sql.DB, schema string) {
				// The column in the underlying table has not been renamed.
				ColumnMustExist(t, db, schema, "users", "username")

				// Insertions to the new column name in the new version schema should work.
				MustInsert(t, db, schema, "02_rename_column", "users", map[string]string{
					"name": "alice",
				})

				// Insertions to the old column name in the old version schema should work.
				MustInsert(t, db, schema, "01_create_table", "users", map[string]string{
					"username": "bob",
				})

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
		},
	})
}

func TestOpRenameColumnValidation(t *testing.T) {
	t.Parallel()

	createTableMigration := migrations.Migration{
		Name: "01_create_table",
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
						Nullable: false,
					},
				},
			},
		},
	}

	ExecuteTests(t, TestCases{
		{
			name: "table must exist",
			migrations: []migrations.Migration{
				createTableMigration,
				{
					Name: "02_rename_column",
					Operations: migrations.Operations{
						&migrations.OpRenameColumn{
							Table: "doesntexist",
							From:  "username",
							To:    "id",
						},
					},
				},
			},
			wantStartErr: migrations.TableDoesNotExistError{Name: "doesntexist"},
		},
		{
			name: "source column must exist",
			migrations: []migrations.Migration{
				createTableMigration,
				{
					Name: "02_rename_column",
					Operations: migrations.Operations{
						&migrations.OpRenameColumn{
							Table: "users",
							From:  "doesntexist",
							To:    "id",
						},
					},
				},
			},
			wantStartErr: migrations.ColumnDoesNotExistError{Table: "users", Name: "doesntexist"},
		},
		{
			name: "target column must not exist",
			migrations: []migrations.Migration{
				createTableMigration,
				{
					Name: "02_rename_column",
					Operations: migrations.Operations{
						&migrations.OpRenameColumn{
							Table: "users",
							From:  "username",
							To:    "id",
						},
					},
				},
			},
			wantStartErr: migrations.ColumnAlreadyExistsError{Table: "users", Name: "id"},
		},
	})
}
