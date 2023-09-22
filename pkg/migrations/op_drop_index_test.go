// SPDX-License-Identifier: Apache-2.0

package migrations_test

import (
	"database/sql"
	"testing"

	"github.com/xataio/pg-roll/pkg/migrations"
)

func TestDropIndex(t *testing.T) {
	t.Parallel()

	ExecuteTests(t, TestCases{{
		name: "drop index",
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
			{
				Name: "03_drop_index",
				Operations: migrations.Operations{
					&migrations.OpDropIndex{
						Name: "idx_users_name",
					},
				},
			},
		},
		afterStart: func(t *testing.T, db *sql.DB) {
			// The index has not yet been dropped.
			IndexMustExist(t, db, "public", "users", "idx_users_name")
		},
		afterRollback: func(t *testing.T, db *sql.DB) {
			// Rollback is a no-op.
		},
		afterComplete: func(t *testing.T, db *sql.DB) {
			// The index has been removed from the underlying table.
			IndexMustNotExist(t, db, "public", "users", "idx_users_name")
		},
	}})
}
