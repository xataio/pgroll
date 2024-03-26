// SPDX-License-Identifier: Apache-2.0

package migrations_test

import (
	"testing"

	"github.com/xataio/pgroll/pkg/migrations"
)

func TestAlterColumnValidation(t *testing.T) {
	t.Parallel()

	createTablesMigration := migrations.Migration{
		Name: "01_add_tables",
		Operations: migrations.Operations{
			&migrations.OpCreateTable{
				Name: "users",
				Columns: []migrations.Column{
					{
						Name: "id",
						Type: "serial",
						Pk:   ptr(true),
					},
					{
						Name: "name",
						Type: "text",
					},
				},
			},
			&migrations.OpCreateTable{
				Name: "posts",
				Columns: []migrations.Column{
					{
						Name: "id",
						Type: "serial",
						Pk:   ptr(true),
					},
					{
						Name: "title",
						Type: "text",
					},
					{
						Name: "user_id",
						Type: "integer",
					},
				},
			},
		},
	}

	ExecuteTests(t, TestCases{
		{
			name: "table must exist",
			migrations: []migrations.Migration{
				createTablesMigration,
				{
					Name: "01_alter_column",
					Operations: migrations.Operations{
						&migrations.OpAlterColumn{
							Table:  "doesntexist",
							Column: "title",
							Name:   ptr("renamed_title"),
						},
					},
				},
			},
			wantStartErr: migrations.TableDoesNotExistError{Name: "doesntexist"},
		},
		{
			name: "column must exist",
			migrations: []migrations.Migration{
				createTablesMigration,
				{
					Name: "01_alter_column",
					Operations: migrations.Operations{
						&migrations.OpAlterColumn{
							Table:  "posts",
							Column: "doesntexist",
							Name:   ptr("renamed_title"),
						},
					},
				},
			},
			wantStartErr: migrations.ColumnDoesNotExistError{Table: "posts", Name: "doesntexist"},
		},
		{
			name: "column rename: no up SQL allowed",
			migrations: []migrations.Migration{
				createTablesMigration,
				{
					Name: "01_alter_column",
					Operations: migrations.Operations{
						&migrations.OpAlterColumn{
							Table:  "posts",
							Column: "title",
							Name:   ptr("renamed_title"),
							Up:     "some up sql",
						},
					},
				},
			},
			wantStartErr: migrations.NoUpSQLAllowedError{},
		},
		{
			name: "column rename: no down SQL allowed",
			migrations: []migrations.Migration{
				createTablesMigration,
				{
					Name: "01_alter_column",
					Operations: migrations.Operations{
						&migrations.OpAlterColumn{
							Table:  "posts",
							Column: "title",
							Name:   ptr("renamed_title"),
							Down:   "some down sql",
						},
					},
				},
			},
			wantStartErr: migrations.NoDownSQLAllowedError{},
		},
		{
			name: "cant make no changes",
			migrations: []migrations.Migration{
				createTablesMigration,
				{
					Name: "01_alter_column",
					Operations: migrations.Operations{
						&migrations.OpAlterColumn{
							Table:  "posts",
							Column: "title",
						},
					},
				},
			},
			wantStartErr: migrations.MultipleAlterColumnChangesError{Changes: 0},
		},
		{
			name: "only one change at at time",
			migrations: []migrations.Migration{
				createTablesMigration,
				{
					Name: "01_alter_column",
					Operations: migrations.Operations{
						&migrations.OpAlterColumn{
							Table:  "posts",
							Column: "title",
							Name:   ptr("renamed_title"),
							Type:   ptr("varchar(255)"),
						},
					},
				},
			},
			wantStartErr: migrations.MultipleAlterColumnChangesError{Changes: 2},
		},
		{
			name: "table must have a primary key on exactly one column",
			migrations: []migrations.Migration{
				{
					Name: "01_add_table",
					Operations: migrations.Operations{
						&migrations.OpRawSQL{
							Up:   "CREATE TABLE orders(id integer, order_id integer, quantity integer, primary key (id, order_id))",
							Down: "DROP TABLE orders",
						},
					},
				},
				{
					Name: "02_alter_column",
					Operations: migrations.Operations{
						&migrations.OpAlterColumn{
							Table:  "orders",
							Column: "quantity",
							Name:   ptr("renamed_quantity"),
						},
					},
				},
			},
			wantStartErr: migrations.BackfillNotPossibleError{Table: "orders"},
		},
	})
}
