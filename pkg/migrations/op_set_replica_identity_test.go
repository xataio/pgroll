// SPDX-License-Identifier: Apache-2.0

package migrations_test

import (
	"database/sql"
	"testing"

	"github.com/xataio/pgroll/pkg/migrations"
)

func TestSetReplicaIdentity(t *testing.T) {
	t.Parallel()

	createTableMigration := migrations.Migration{
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
	}

	ExecuteTests(t, TestCases{
		{
			name: "set replica identity to FULL",
			migrations: []migrations.Migration{
				createTableMigration,
				{
					Name: "02_set_replica_identity",
					Operations: migrations.Operations{
						&migrations.OpSetReplicaIdentity{
							Table:    "users",
							Identity: migrations.ReplicaIdentity{Type: "full"},
						},
					},
				},
			},
			afterStart: func(t *testing.T, db *sql.DB, schema string) {
				// The replica identity has been set to 'f' (full).
				ReplicaIdentityMustBe(t, db, schema, "users", "f")
			},
			afterRollback: func(t *testing.T, db *sql.DB, schema string) {
				// Rollback is a no-op
			},
			afterComplete: func(t *testing.T, db *sql.DB, schema string) {
				// Complete is a no-op
			},
		},
		{
			name: "set replica identity to NOTHING",
			migrations: []migrations.Migration{
				createTableMigration,
				{
					Name: "02_set_replica_identity",
					Operations: migrations.Operations{
						&migrations.OpSetReplicaIdentity{
							Table:    "users",
							Identity: migrations.ReplicaIdentity{Type: "nothing"},
						},
					},
				},
			},
			afterStart: func(t *testing.T, db *sql.DB, schema string) {
				// The replica identity has been set to 'n' (nothing).
				ReplicaIdentityMustBe(t, db, schema, "users", "n")
			},
			afterRollback: func(t *testing.T, db *sql.DB, schema string) {
				// Rollback is a no-op
			},
			afterComplete: func(t *testing.T, db *sql.DB, schema string) {
				// Complete is a no-op
			},
		},
		{
			name: "set replica identity to DEFAULT",
			migrations: []migrations.Migration{
				createTableMigration,
				{
					Name: "02_set_replica_identity",
					Operations: migrations.Operations{
						&migrations.OpSetReplicaIdentity{
							Table:    "users",
							Identity: migrations.ReplicaIdentity{Type: "default"},
						},
					},
				},
			},
			afterStart: func(t *testing.T, db *sql.DB, schema string) {
				// The replica identity has been set to 'd' (default).
				ReplicaIdentityMustBe(t, db, schema, "users", "d")
			},
			afterRollback: func(t *testing.T, db *sql.DB, schema string) {
				// Rollback is a no-op
			},
			afterComplete: func(t *testing.T, db *sql.DB, schema string) {
				// Complete is a no-op
			},
		},
		{
			name: "set replica identity to USING INDEX",
			migrations: []migrations.Migration{
				createTableMigration,
				{
					Name: "02_set_replica_identity",
					Operations: migrations.Operations{
						&migrations.OpSetReplicaIdentity{
							Table: "users",
							Identity: migrations.ReplicaIdentity{
								Type:  "index",
								Index: "users_pkey",
							},
						},
					},
				},
			},
			afterStart: func(t *testing.T, db *sql.DB, schema string) {
				// The replica identity has been set to 'i' (index).
				ReplicaIdentityMustBe(t, db, schema, "users", "i")
			},
			afterRollback: func(t *testing.T, db *sql.DB, schema string) {
				// Rollback is a no-op
			},
			afterComplete: func(t *testing.T, db *sql.DB, schema string) {
				// Complete is a no-op
			},
		},
	})
}

func TestSetReplicaIdentityValidation(t *testing.T) {
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
						Name:   "name",
						Type:   "varchar(255)",
						Unique: true,
					},
				},
			},
		},
	}

	ExecuteTests(t, TestCases{
		{
			name: "table must exist",
			migrations: []migrations.Migration{
				addTableMigration,
				{
					Name: "02_add_column",
					Operations: migrations.Operations{
						&migrations.OpSetReplicaIdentity{
							Table:    "doesntexist",
							Identity: migrations.ReplicaIdentity{Type: "default"},
						},
					},
				},
			},
			wantStartErr: migrations.TableDoesNotExistError{Name: "doesntexist"},
		},
		{
			name: "identity must be valid",
			migrations: []migrations.Migration{
				addTableMigration,
				{
					Name: "02_add_column",
					Operations: migrations.Operations{
						&migrations.OpSetReplicaIdentity{
							Table:    "users",
							Identity: migrations.ReplicaIdentity{Type: "invalid_identity"},
						},
					},
				},
			},
			wantStartErr: migrations.InvalidReplicaIdentityError{Table: "users", Identity: "invalid_identity"},
		},
		{
			name: "index name must be valid",
			migrations: []migrations.Migration{
				addTableMigration,
				{
					Name: "02_add_column",
					Operations: migrations.Operations{
						&migrations.OpSetReplicaIdentity{
							Table:    "users",
							Identity: migrations.ReplicaIdentity{Type: "index", Index: "invalid_index"},
						},
					},
				},
			},
			wantStartErr: migrations.IndexDoesNotExistError{Name: "invalid_index"},
		},
	})
}
