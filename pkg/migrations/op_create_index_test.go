// SPDX-License-Identifier: Apache-2.0

package migrations_test

import (
	"database/sql"
	"fmt"
	"strings"
	"testing"

	"github.com/xataio/pgroll/pkg/migrations"
)

func TestCreateIndex(t *testing.T) {
	t.Parallel()

	invalidName := strings.Repeat("x", 64)
	ExecuteTests(t, TestCases{
		{
			name: "create index",
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
									Pk:   ptr(true),
								},
								{
									Name:     "name",
									Type:     "varchar(255)",
									Nullable: ptr(false),
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
			afterStart: func(t *testing.T, db *sql.DB, schema string) {
				// The index has been created on the underlying table.
				IndexMustExist(t, db, schema, "users", "idx_users_name")
			},
			afterRollback: func(t *testing.T, db *sql.DB, schema string) {
				// The index has been dropped from the the underlying table.
				IndexMustNotExist(t, db, schema, "users", "idx_users_name")
			},
			afterComplete: func(t *testing.T, db *sql.DB, schema string) {
				// Complete is a no-op.
			},
		},
		{
			name: "create index with a mixed case name",
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
									Pk:   ptr(true),
								},
								{
									Name:     "name",
									Type:     "varchar(255)",
									Nullable: ptr(false),
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
							Columns: []string{"name"},
						},
					},
				},
			},
			afterStart: func(t *testing.T, db *sql.DB, schema string) {
				// The index has been created on the underlying table.
				IndexMustExist(t, db, schema, "users", "idx_USERS_name")
			},
			afterRollback: func(t *testing.T, db *sql.DB, schema string) {
				// The index has been dropped from the the underlying table.
				IndexMustNotExist(t, db, schema, "users", "idx_USERS_name")
			},
			afterComplete: func(t *testing.T, db *sql.DB, schema string) {
				// Complete is a no-op.
			},
		},
		{
			name: "create partial index",
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
									Pk:   ptr(true),
								},
								{
									Name:     "name",
									Type:     "varchar(255)",
									Nullable: ptr(false),
								},
								{
									Name:     "registered_at_year",
									Type:     "integer",
									Nullable: ptr(false),
								},
							},
						},
					},
				},
				{
					Name: "02_create_index_for_new_users_after_2019",
					Operations: migrations.Operations{
						&migrations.OpCreateIndex{
							Name:      "idx_users_name_after_2019",
							Table:     "users",
							Columns:   []string{"registered_at_year"},
							Predicate: ptr("registered_at_year > 2019"),
						},
					},
				},
			},
			afterStart: func(t *testing.T, db *sql.DB, schema string) {
				// The index has been created on the underlying table.
				IndexMustExist(t, db, schema, "users", "idx_users_name_after_2019")
				CheckIndexDefinition(t, db, schema, "users", "idx_users_name_after_2019", fmt.Sprintf("CREATE INDEX idx_users_name_after_2019 ON %s.users USING btree (registered_at_year) WHERE (registered_at_year > 2019)", schema))
			},
			afterRollback: func(t *testing.T, db *sql.DB, schema string) {
				// The index has been dropped from the the underlying table.
				IndexMustNotExist(t, db, schema, "users", "idx_users_name_after_2019")
			},
			afterComplete: func(t *testing.T, db *sql.DB, schema string) {
				// Complete is a no-op.
			},
		},
		{
			name: "invalid name",
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
									Pk:   ptr(true),
								},
								{
									Name:     "name",
									Type:     "varchar(255)",
									Nullable: ptr(false),
								},
								{
									Name:     "registered_at_year",
									Type:     "integer",
									Nullable: ptr(false),
								},
							},
						},
					},
				},
				{
					Name: "02_create_index_with_invalid_name",
					Operations: migrations.Operations{
						&migrations.OpCreateIndex{
							Name:    invalidName,
							Table:   "users",
							Columns: []string{"registered_at_year"},
						},
					},
				},
			},
			wantStartErr:  migrations.ValidateIdentifierLength(invalidName),
			afterStart:    func(t *testing.T, db *sql.DB, schema string) {},
			afterRollback: func(t *testing.T, db *sql.DB, schema string) {},
			afterComplete: func(t *testing.T, db *sql.DB, schema string) {},
		},
		{
			name: "create hash index with option",
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
									Pk:   ptr(true),
								},
								{
									Name:     "name",
									Type:     "varchar(255)",
									Nullable: ptr(false),
								},
							},
						},
					},
				},
				{
					Name: "02_create_hash_index",
					Operations: migrations.Operations{
						&migrations.OpCreateIndex{
							Name:              "idx_users_name_hash",
							Table:             "users",
							Columns:           []string{"name"},
							Method:            ptr(migrations.OpCreateIndexMethodHash),
							StorageParameters: ptr("fillfactor = 70"),
						},
					},
				},
			},
			afterStart: func(t *testing.T, db *sql.DB, schema string) {
				// The index has been created on the underlying table.
				IndexMustExist(t, db, schema, "users", "idx_users_name_hash")
				// Check the index definition.
				CheckIndexDefinition(t, db, schema, "users", "idx_users_name_hash", fmt.Sprintf("CREATE INDEX idx_users_name_hash ON %s.users USING hash (name) WITH (fillfactor='70')", schema))
			},
			afterRollback: func(t *testing.T, db *sql.DB, schema string) {
				// The index has been dropped from the the underlying table.
				IndexMustNotExist(t, db, schema, "users", "idx_users_name_hash")
			},
			afterComplete: func(t *testing.T, db *sql.DB, schema string) {
				// Complete is a no-op.
			},
		},
	})
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
								Name: "id",
								Type: "serial",
								Pk:   ptr(true),
							},
							{
								Name:     "name",
								Type:     "varchar(255)",
								Nullable: ptr(false),
							},
							{
								Name:     "email",
								Type:     "varchar(255)",
								Nullable: ptr(false),
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
		afterStart: func(t *testing.T, db *sql.DB, schema string) {
			// The index has been created on the underlying table.
			IndexMustExist(t, db, schema, "users", "idx_users_name_email")
		},
		afterRollback: func(t *testing.T, db *sql.DB, schema string) {
			// The index has been dropped from the the underlying table.
			IndexMustNotExist(t, db, schema, "users", "idx_users_name_email")
		},
		afterComplete: func(t *testing.T, db *sql.DB, schema string) {
			// Complete is a no-op.
		},
	}})
}

func TestCreateIndexOnObjectsCreatedInSameMigration(t *testing.T) {
	t.Parallel()

	ExecuteTests(t, TestCases{
		{
			name: "create index on newly created table",
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
									Pk:   ptr(true),
								},
								{
									Name:     "name",
									Type:     "varchar(255)",
									Nullable: ptr(false),
								},
							},
						},
						&migrations.OpCreateIndex{
							Name:    "idx_users_name",
							Table:   "users",
							Columns: []string{"name"},
						},
					},
				},
			},
			afterStart: func(t *testing.T, db *sql.DB, schema string) {
				// The index has been created on the underlying table.
				IndexMustExist(t, db, schema, migrations.TemporaryName("users"), "idx_users_name")
			},
			afterRollback: func(t *testing.T, db *sql.DB, schema string) {
				// The index has been dropped from the the underlying table.
				IndexMustNotExist(t, db, schema, "users", "idx_users_name")
			},
			afterComplete: func(t *testing.T, db *sql.DB, schema string) {
				// The index remains on the underlying table.
				IndexMustExist(t, db, schema, "users", "idx_users_name")
			},
		},
		{
			name: "create index on newly created column",
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
									Pk:   ptr(true),
								},
								{
									Name:     "name",
									Type:     "varchar(255)",
									Nullable: ptr(false),
								},
							},
						},
					},
				},
				{
					Name: "02_add_column_and_index",
					Operations: migrations.Operations{
						&migrations.OpAddColumn{
							Table: "users",
							Column: migrations.Column{
								Name:     "age",
								Type:     "integer",
								Nullable: ptr(true),
							},
							Up: "18",
						},
						&migrations.OpCreateIndex{
							Name:    "idx_users_age",
							Table:   "users",
							Columns: []string{"age"},
						},
					},
				},
			},
			afterStart: func(t *testing.T, db *sql.DB, schema string) {
				// The index has been created on the underlying table.
				IndexMustExist(t, db, schema, "users", "idx_users_age")
			},
			afterRollback: func(t *testing.T, db *sql.DB, schema string) {
				// The index has been dropped from the the underlying table.
				IndexMustNotExist(t, db, schema, "users", "idx_users_age")
			},
			afterComplete: func(t *testing.T, db *sql.DB, schema string) {
				// The index has been created on the underlying table.
				IndexMustExist(t, db, schema, "users", "idx_users_age")
			},
		},
	})
}
