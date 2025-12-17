// SPDX-License-Identifier: Apache-2.0

package migrations_test

import (
	"database/sql"
	"testing"

	"github.com/xataio/pgroll/pkg/migrations"
)

func TestDropIndex(t *testing.T) {
	t.Parallel()

	ExecuteTests(t, TestCases{
		{
			name: "drop index",
			migrations: []migrations.Migration{
				{
					Name:          "01_add_table",
					VersionSchema: "add_table",
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
									Name:     "name",
									Type:     "varchar(255)",
									Nullable: false,
								},
							},
						},
					},
				},
				{
					Name:          "02_create_index",
					VersionSchema: "create_index",
					Operations: migrations.Operations{
						&migrations.OpCreateIndex{
							Name:    "idx_users_name",
							Table:   "users",
							Columns: []migrations.IndexField{{Column: "name"}},
						},
					},
				},
				{
					Name:          "03_drop_index",
					VersionSchema: "drop_index",
					Operations: migrations.Operations{
						&migrations.OpDropIndex{
							Name: "idx_users_name",
						},
					},
				},
			},
			afterStart: func(t *testing.T, db *sql.DB, schema string) {
				// The index has not yet been dropped.
				IndexMustExist(t, db, schema, "users", "idx_users_name")
			},
			afterRollback: func(t *testing.T, db *sql.DB, schema string) {
				// Rollback is a no-op.
			},
			afterComplete: func(t *testing.T, db *sql.DB, schema string) {
				// The index has been removed from the underlying table.
				IndexMustNotExist(t, db, schema, "users", "idx_users_name")
			},
		},
		{
			name: "drop index with a mixed case name",
			migrations: []migrations.Migration{
				{
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
							Name:    "idx_USERS_name",
							Table:   "users",
							Columns: []migrations.IndexField{{Column: "name"}},
						},
					},
				},
				{
					Name: "03_drop_index",
					Operations: migrations.Operations{
						&migrations.OpDropIndex{
							Name: "idx_USERS_name",
						},
					},
				},
			},
			afterStart: func(t *testing.T, db *sql.DB, schema string) {
				// The index has not yet been dropped.
				IndexMustExist(t, db, schema, "users", "idx_USERS_name")
			},
			afterRollback: func(t *testing.T, db *sql.DB, schema string) {
				// Rollback is a no-op.
			},
			afterComplete: func(t *testing.T, db *sql.DB, schema string) {
				// The index has been removed from the underlying table.
				IndexMustNotExist(t, db, schema, "users", "idx_USERS_name")
			},
		},
	})
}
