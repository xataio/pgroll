package migrations_test

import (
	"testing"

	"github.com/xataio/pg-roll/pkg/migrations"
)

func TestSetNotNullValidation(t *testing.T) {
	t.Parallel()

	ptr := func(s string) *string { return &s }

	createTableMigration := migrations.Migration{
		Name: "01_add_table",
		Operations: migrations.Operations{
			&migrations.OpCreateTable{
				Name: "reviews",
				Columns: []migrations.Column{
					{
						Name:       "id",
						Type:       "serial",
						PrimaryKey: true,
					},
					{
						Name:     "username",
						Type:     "text",
						Nullable: false,
					},
					{
						Name:     "product",
						Type:     "text",
						Nullable: false,
					},
					{
						Name:     "review",
						Type:     "text",
						Nullable: true,
					},
				},
			},
		},
	}

	ExecuteTests(t, TestCases{
		{
			name: "up SQL is mandatory",
			migrations: []migrations.Migration{
				createTableMigration,
				{
					Name: "02_set_not_null",
					Operations: migrations.Operations{
						&migrations.OpSetNotNull{
							Table:  "reviews",
							Column: "review",
						},
					},
				},
			},
			wantStartErr: migrations.UpSQLRequiredError{},
		},
		{
			name: "table must exist",
			migrations: []migrations.Migration{
				createTableMigration,
				{
					Name: "02_set_not_null",
					Operations: migrations.Operations{
						&migrations.OpSetNotNull{
							Table:  "doesntexist",
							Column: "review",
							Up:     ptr("product || ' is good'"),
						},
					},
				},
			},
			wantStartErr: migrations.TableDoesNotExistError{Name: "doesntexist"},
		},
		{
			name: "column must exist",
			migrations: []migrations.Migration{
				createTableMigration,
				{
					Name: "02_set_not_null",
					Operations: migrations.Operations{
						&migrations.OpSetNotNull{
							Table:  "reviews",
							Column: "doesntexist",
							Up:     ptr("product || ' is good'"),
						},
					},
				},
			},
			wantStartErr: migrations.ColumnDoesNotExistError{Table: "reviews", Name: "doesntexist"},
		},
	})
}
