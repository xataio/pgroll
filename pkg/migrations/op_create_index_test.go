package migrations_test

import (
	"database/sql"
	"testing"

	"pg-roll/pkg/migrations"
)

func TestCreateIndex(t *testing.T) {
	t.Parallel()

	ExecuteTests(t, TestCases{{
		name: "create index",
		migrations: []migrations.Migration{
			{
				Name: "01_add_table",
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
								Name:     "name",
								Type:     "varchar(255)",
								Nullable: false,
							},
						},
					},
				},
			},
			{
				Name: "02_create_index",
				Operations: migrations.Operations{
					&migrations.OpCreateIndex{
						Table:   "users",
						Columns: []string{"name"},
					},
				},
			},
		},
		afterStart: func(t *testing.T, db *sql.DB) {
			// The index has been created on the underlying table.
			idxName := migrations.IndexName("users", []string{"name"})
			IndexMustExist(t, db, "public", "users", idxName)
		},
		afterRollback: func(t *testing.T, db *sql.DB) {
			// The index has been dropped from the the underlying table.
			idxName := migrations.IndexName("users", []string{"name"})
			IndexMustNotExist(t, db, "public", "users", idxName)
		},
		afterComplete: func(t *testing.T, db *sql.DB) {
			// Complete is a no-op.
		},
	}})
}

func TestCreateIndexOnMultipleColumns(t *testing.T) {
	t.Parallel()

	ExecuteTests(t, TestCases{{
		name: "create index",
		migrations: []migrations.Migration{
			{
				Name: "01_add_table",
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
								Name:     "name",
								Type:     "varchar(255)",
								Nullable: false,
							},
							{
								Name:     "email",
								Type:     "varchar(255)",
								Nullable: false,
							},
						},
					},
				},
			},
			{
				Name: "02_create_index",
				Operations: migrations.Operations{
					&migrations.OpCreateIndex{
						Table:   "users",
						Columns: []string{"name", "email"},
					},
				},
			},
		},
		afterStart: func(t *testing.T, db *sql.DB) {
			// The index has been created on the underlying table.
			idxName := migrations.IndexName("users", []string{"name", "email"})
			IndexMustExist(t, db, "public", "users", idxName)
		},
		afterRollback: func(t *testing.T, db *sql.DB) {
			// The index has been dropped from the the underlying table.
			idxName := migrations.IndexName("users", []string{"name", "email"})
			IndexMustNotExist(t, db, "public", "users", idxName)
		},
		afterComplete: func(t *testing.T, db *sql.DB) {
			// Complete is a no-op.
		},
	}})
}
