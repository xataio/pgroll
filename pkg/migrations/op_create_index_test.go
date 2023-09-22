// SPDX-License-Identifier: Apache-2.0

package migrations_test

import (
	"database/sql"
	"testing"

	"github.com/xataio/pg-roll/pkg/migrations"
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
						Name:    "idx_users_name",
						Table:   "users",
						Columns: []string{"name"},
					},
				},
			},
		},
		afterStart: func(t *testing.T, db *sql.DB) {
			// The index has been created on the underlying table.
			IndexMustExist(t, db, "public", "users", "idx_users_name")
		},
		afterRollback: func(t *testing.T, db *sql.DB) {
			// The index has been dropped from the the underlying table.
			IndexMustNotExist(t, db, "public", "users", "idx_users_name")
		},
		afterComplete: func(t *testing.T, db *sql.DB) {
			// Complete is a no-op.
		},
	}})
}

func TestCreateIndexOnMultipleColumns(t *testing.T) {
	t.Parallel()

	ExecuteTests(t, TestCases{{
		name: "create index on multiple columns",
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
						Name:    "idx_users_name_email",
						Table:   "users",
						Columns: []string{"name", "email"},
					},
				},
			},
		},
		afterStart: func(t *testing.T, db *sql.DB) {
			// The index has been created on the underlying table.
			IndexMustExist(t, db, "public", "users", "idx_users_name_email")
		},
		afterRollback: func(t *testing.T, db *sql.DB) {
			// The index has been dropped from the the underlying table.
			IndexMustNotExist(t, db, "public", "users", "idx_users_name_email")
		},
		afterComplete: func(t *testing.T, db *sql.DB) {
			// Complete is a no-op.
		},
	}})
}
