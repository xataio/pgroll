package migrations_test

import (
	"testing"

	"github.com/xataio/pg-roll/pkg/migrations"
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
						Name:       "id",
						Type:       "serial",
						PrimaryKey: true,
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
						Name:       "id",
						Type:       "serial",
						PrimaryKey: true,
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
			name: "column rename: no up SQL allowed",
			migrations: []migrations.Migration{
				createTablesMigration,
				{
					Name: "01_alter_column",
					Operations: migrations.Operations{
						&migrations.OpAlterColumn{
							Table:  "posts",
							Column: "title",
							Name:   "renamed_title",
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
							Name:   "renamed_title",
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
			wantStartErr: migrations.MultipleAlterColumnChangesError{},
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
							Name:   "renamed_title",
							Type:   "varchar(255)",
						},
					},
				},
			},
			wantStartErr: migrations.MultipleAlterColumnChangesError{},
		},
	})
}
