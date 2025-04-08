// SPDX-License-Identifier: Apache-2.0

package migrations_test

import (
	"database/sql"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/xataio/pgroll/internal/testutils"
	"github.com/xataio/pgroll/pkg/migrations"
)

func TestCreateConstraint(t *testing.T) {
	t.Parallel()

	ExecuteTests(t, TestCases{
		{
			name: "create unique constraint on single column",
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
					Name: "02_create_constraint",
					Operations: migrations.Operations{
						&migrations.OpCreateConstraint{
							Name:    "unique_name",
							Table:   "users",
							Type:    "unique",
							Columns: []string{"name"},
							Up: map[string]string{
								"name": "name || random()",
							},
							Down: map[string]string{
								"name": "name",
							},
						},
					},
				},
			},
			afterStart: func(t *testing.T, db *sql.DB, schema string) {
				// The index has been created on the underlying table.
				IndexMustExist(t, db, schema, "users", "unique_name")

				// Inserting values into the old schema that violate uniqueness should succeed.
				MustInsert(t, db, schema, "01_add_table", "users", map[string]string{
					"name": "alice",
				})
				MustInsert(t, db, schema, "01_add_table", "users", map[string]string{
					"name": "alice",
				})

				// Inserting values into the new schema that violate uniqueness should fail.
				MustInsert(t, db, schema, "02_create_constraint", "users", map[string]string{
					"name": "bob",
				})
				MustNotInsert(t, db, schema, "02_create_constraint", "users", map[string]string{
					"name": "bob",
				}, testutils.UniqueViolationErrorCode)
			},
			afterRollback: func(t *testing.T, db *sql.DB, schema string) {
				// The index has been dropped from the the underlying table.
				IndexMustNotExist(t, db, schema, "users", "uniue_name")

				// Functions, triggers and temporary columns are dropped.
				TableMustBeCleanedUp(t, db, schema, "users", "name")
			},
			afterComplete: func(t *testing.T, db *sql.DB, schema string) {
				// Functions, triggers and temporary columns are dropped.
				TableMustBeCleanedUp(t, db, schema, "users", "name")

				// Inserting values into the new schema that violate uniqueness should fail.
				MustInsert(t, db, schema, "02_create_constraint", "users", map[string]string{
					"name": "carol",
				})
				MustNotInsert(t, db, schema, "02_create_constraint", "users", map[string]string{
					"name": "carol",
				}, testutils.UniqueViolationErrorCode)
			},
		},
		{
			name: "create check constraint on single column",
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
					Name: "02_create_constraint",
					Operations: migrations.Operations{
						&migrations.OpCreateConstraint{
							Name:    "name_letters",
							Table:   "users",
							Type:    "check",
							Check:   ptr("name ~ '^[a-zA-Z]+$'"),
							Columns: []string{"name"},
							Up: map[string]string{
								"name": "regexp_replace(name, '\\d+', '', 'g')",
							},
							Down: map[string]string{
								"name": "name",
							},
						},
					},
				},
			},
			afterStart: func(t *testing.T, db *sql.DB, schema string) {
				// The new (temporary) column should exist on the underlying table.
				ColumnMustExist(t, db, schema, "users", migrations.TemporaryName("name"))
				// The check constraint exists on the new table.
				CheckConstraintMustExist(t, db, schema, "users", "name_letters")
				// Inserting values into the old schema that violate the check constraint must succeed.
				MustInsert(t, db, schema, "01_add_table", "users", map[string]string{
					"name": "alice11",
				})

				// Inserting values into the new schema that violate the check constraint should fail.
				MustInsert(t, db, schema, "02_create_constraint", "users", map[string]string{
					"name": "bob",
				})
				MustNotInsert(t, db, schema, "02_create_constraint", "users", map[string]string{
					"name": "bob2",
				}, testutils.CheckViolationErrorCode)
			},
			afterRollback: func(t *testing.T, db *sql.DB, schema string) {
				// Functions, triggers and temporary columns are dropped.
				TableMustBeCleanedUp(t, db, schema, "users", "name")
			},
			afterComplete: func(t *testing.T, db *sql.DB, schema string) {
				// Functions, triggers and temporary columns are dropped.
				TableMustBeCleanedUp(t, db, schema, "users", "name")

				// Inserting values into the new schema that violate the check constraint should fail.
				MustNotInsert(t, db, schema, "02_create_constraint", "users", map[string]string{
					"name": "carol0",
				}, testutils.CheckViolationErrorCode)
			},
		},
		{
			name: "create unique constraint on multiple columns",
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
					Name: "02_create_constraint",
					Operations: migrations.Operations{
						&migrations.OpCreateConstraint{
							Name:    "unique_name_email",
							Table:   "users",
							Type:    "unique",
							Columns: []string{"name", "email"},
							Up: map[string]string{
								"name":  "name || random()",
								"email": "email || random()",
							},
							Down: map[string]string{
								"name":  "name",
								"email": "email",
							},
						},
					},
				},
			},
			afterStart: func(t *testing.T, db *sql.DB, schema string) {
				// The index has been created on the underlying table.
				IndexMustExist(t, db, schema, "users", "unique_name_email")

				// Inserting values into the old schema that violate uniqueness should succeed.
				MustInsert(t, db, schema, "01_add_table", "users", map[string]string{
					"name":  "alice",
					"email": "alice@alice.me",
				})
				MustInsert(t, db, schema, "01_add_table", "users", map[string]string{
					"name":  "alice",
					"email": "alice@alice.me",
				})

				// Inserting values into the new schema that violate uniqueness should fail.
				MustInsert(t, db, schema, "02_create_constraint", "users", map[string]string{
					"name":  "bob",
					"email": "bob@bob.me",
				})
				MustNotInsert(t, db, schema, "02_create_constraint", "users", map[string]string{
					"name":  "bob",
					"email": "bob@bob.me",
				}, testutils.UniqueViolationErrorCode)
			},
			afterRollback: func(t *testing.T, db *sql.DB, schema string) {
				// The index has been dropped from the the underlying table.
				IndexMustNotExist(t, db, schema, "users", "unique_name_email")

				// Functions, triggers and temporary columns are dropped.
				TableMustBeCleanedUp(t, db, schema, "users", "name")
				TableMustBeCleanedUp(t, db, schema, "users", "email")
			},
			afterComplete: func(t *testing.T, db *sql.DB, schema string) {
				// Complete is a no-op.
			},
		},
		{
			name: "create check constraint on multiple columns",
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
					Name: "02_create_constraint",
					Operations: migrations.Operations{
						&migrations.OpCreateConstraint{
							Name:    "check_name_email",
							Table:   "users",
							Type:    "check",
							Check:   ptr("name != email"),
							Columns: []string{"name", "email"},
							Up: map[string]string{
								"name":  "name",
								"email": "SELECT CASE WHEN email ~ '@' THEN email ELSE email || '@example.com' END",
							},
							Down: map[string]string{
								"name":  "name",
								"email": "email",
							},
						},
					},
				},
			},
			afterStart: func(t *testing.T, db *sql.DB, schema string) {
				// The new (temporary) column should exist on the underlying table.
				ColumnMustExist(t, db, schema, "users", migrations.TemporaryName("name"))
				// The new (temporary) column should exist on the underlying table.
				ColumnMustExist(t, db, schema, "users", migrations.TemporaryName("email"))
				// The check constraint exists on the new table.
				CheckConstraintMustExist(t, db, schema, "users", "check_name_email")

				// Inserting values into the old schema that the violate the check constraint must succeed.
				MustInsert(t, db, schema, "01_add_table", "users", map[string]string{
					"name":  "alice",
					"email": "alice",
				})

				// Inserting values into the new schema that meet the check constraint should succeed.
				MustInsert(t, db, schema, "02_create_constraint", "users", map[string]string{
					"name":  "bob",
					"email": "bob@bob.me",
				})
				MustNotInsert(t, db, schema, "02_create_constraint", "users", map[string]string{
					"name":  "bob",
					"email": "bob",
				}, testutils.CheckViolationErrorCode)
			},
			afterRollback: func(t *testing.T, db *sql.DB, schema string) {
				// The check constraint must not exists on the table.
				CheckConstraintMustNotExist(t, db, schema, "users", "check_name_email")
				// Functions, triggers and temporary columns are dropped.
				TableMustBeCleanedUp(t, db, schema, "users", "name")
				TableMustBeCleanedUp(t, db, schema, "users", "email")
			},
			afterComplete: func(t *testing.T, db *sql.DB, schema string) {
				// Functions, triggers and temporary columns are dropped.
				TableMustBeCleanedUp(t, db, schema, "users", "name")
				TableMustBeCleanedUp(t, db, schema, "users", "email")

				// Inserting values into the new schema that the violate the check constraint must fail.
				MustNotInsert(t, db, schema, "02_create_constraint", "users", map[string]string{
					"name":  "carol",
					"email": "carol",
				}, testutils.CheckViolationErrorCode)

				rows := MustSelect(t, db, schema, "02_create_constraint", "users")
				assert.Equal(t, []map[string]any{
					{"id": 1, "name": "alice", "email": "alice@example.com"},
					{"id": 2, "name": "bob", "email": "bob@bob.me"},
				}, rows)
			},
		},
		{
			name: "create foreign key constraint on multiple columns",
			migrations: []migrations.Migration{
				{
					Name: "01_add_tables",
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
									Name: "zip",
									Type: "integer",
									Pk:   true,
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
						&migrations.OpCreateTable{
							Name: "reports",
							Columns: []migrations.Column{
								{
									Name: "id",
									Type: "serial",
									Pk:   true,
								},
								{
									Name:     "users_id",
									Type:     "integer",
									Nullable: true,
								},
								{
									Name:     "users_zip",
									Type:     "integer",
									Nullable: true,
								},
								{
									Name:     "description",
									Type:     "varchar(255)",
									Nullable: false,
								},
							},
						},
					},
				},
				{
					Name: "02_create_constraint",
					Operations: migrations.Operations{
						&migrations.OpCreateConstraint{
							Name:    "fk_users",
							Table:   "reports",
							Type:    "foreign_key",
							Columns: []string{"users_id", "users_zip"},
							References: &migrations.TableForeignKeyReference{
								Table:   "users",
								Columns: []string{"id", "zip"},
							},
							Up: map[string]string{
								"users_id":  "1",
								"users_zip": "12345",
							},
							Down: map[string]string{
								"users_id":  "users_id",
								"users_zip": "users_zip",
							},
						},
					},
				},
			},
			afterStart: func(t *testing.T, db *sql.DB, schema string) {
				// The new (temporary) column should exist on the underlying table.
				ColumnMustExist(t, db, schema, "reports", migrations.TemporaryName("users_id"))
				// The new (temporary) column should exist on the underlying table.
				ColumnMustExist(t, db, schema, "reports", migrations.TemporaryName("users_zip"))
				// A temporary FK constraint has been created on the temporary column
				NotValidatedForeignKeyMustExist(t, db, schema, "reports", "fk_users")

				// Insert values to refer to.
				MustInsert(t, db, schema, "01_add_tables", "users", map[string]string{
					"name":  "alice",
					"email": "alice@example.com",
					"zip":   "12345",
				})

				// Inserting values into the old schema that the violate the fk constraint must succeed.
				MustInsert(t, db, schema, "01_add_tables", "reports", map[string]string{
					"description": "random report",
				})

				// Inserting values into the new schema that meet the FK constraint should succeed.
				MustInsert(t, db, schema, "02_create_constraint", "reports", map[string]string{
					"description": "alice report",
					"users_id":    "1",
					"users_zip":   "12345",
				})
				// Inserting data into the new `reports` view with an invalid user reference fails.
				MustNotInsert(t, db, schema, "02_create_constraint", "reports", map[string]string{
					"description": "no one report",
					"users_id":    "100",
					"users_zip":   "100",
				}, testutils.FKViolationErrorCode)
			},
			afterRollback: func(t *testing.T, db *sql.DB, schema string) {
				// The check constraint must not exists on the table.
				CheckConstraintMustNotExist(t, db, schema, "reports", "fk_users")
				// Functions, triggers and temporary columns are dropped.
				TableMustBeCleanedUp(t, db, schema, "reports", "users_id")
				TableMustBeCleanedUp(t, db, schema, "reports", "users_zip")
			},
			afterComplete: func(t *testing.T, db *sql.DB, schema string) {
				// Functions, triggers and temporary columns are dropped.
				TableMustBeCleanedUp(t, db, schema, "reports", "users_id")
				TableMustBeCleanedUp(t, db, schema, "reports", "users_zip")

				// Inserting values into the new schema that the violate the check constraint must fail.
				MustNotInsert(t, db, schema, "02_create_constraint", "reports", map[string]string{
					"description": "no one report",
					"users_id":    "100",
					"users_zip":   "100",
				}, testutils.FKViolationErrorCode)

				rows := MustSelect(t, db, schema, "02_create_constraint", "reports")
				assert.Equal(t, []map[string]any{
					{"id": 1, "description": "random report", "users_id": 1, "users_zip": 12345},
					{"id": 2, "description": "alice report", "users_id": 1, "users_zip": 12345},
				}, rows)
			},
		},
		{
			name: "create foreign key constraint on multiple columns with on delete",
			migrations: []migrations.Migration{
				{
					Name: "01_add_tables",
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
									Name: "zip",
									Type: "integer",
									Pk:   true,
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
						&migrations.OpCreateTable{
							Name: "reports",
							Columns: []migrations.Column{
								{
									Name: "id",
									Type: "serial",
									Pk:   true,
								},
								{
									Name:     "users_id",
									Type:     "integer",
									Nullable: true,
								},
								{
									Name:     "users_zip",
									Type:     "integer",
									Nullable: true,
								},
								{
									Name:     "description",
									Type:     "varchar(255)",
									Nullable: false,
								},
							},
						},
					},
				},
				{
					Name: "02_create_constraint",
					Operations: migrations.Operations{
						&migrations.OpCreateConstraint{
							Name:    "fk_users",
							Table:   "reports",
							Type:    "foreign_key",
							Columns: []string{"users_id", "users_zip"},
							References: &migrations.TableForeignKeyReference{
								Table:    "users",
								Columns:  []string{"id", "zip"},
								OnDelete: migrations.ForeignKeyActionSETNULL,
							},
							Up: map[string]string{
								"users_id":  "1",
								"users_zip": "12345",
							},
							Down: map[string]string{
								"users_id":  "users_id",
								"users_zip": "users_zip",
							},
						},
					},
				},
			},
			afterStart: func(t *testing.T, db *sql.DB, schema string) {
				// The new (temporary) column should exist on the underlying table.
				ColumnMustExist(t, db, schema, "reports", migrations.TemporaryName("users_id"))
				// The new (temporary) column should exist on the underlying table.
				ColumnMustExist(t, db, schema, "reports", migrations.TemporaryName("users_zip"))
				// A temporary FK constraint has been created on the temporary column
				NotValidatedForeignKeyMustExistWithReferentialAction(t, db, schema, "reports", "fk_users", migrations.ForeignKeyActionSETNULL, migrations.ForeignKeyActionNOACTION)

				// Insert values to refer to.
				MustInsert(t, db, schema, "01_add_tables", "users", map[string]string{
					"name":  "alice",
					"email": "alice@example.com",
					"zip":   "12345",
				})

				// Inserting values into the old schema that the violate the fk constraint must succeed.
				MustInsert(t, db, schema, "01_add_tables", "reports", map[string]string{
					"description": "random report",
				})

				// Inserting values into the new schema that meet the FK constraint should succeed.
				MustInsert(t, db, schema, "02_create_constraint", "reports", map[string]string{
					"description": "alice report",
					"users_id":    "1",
					"users_zip":   "12345",
				})
				// Inserting data into the new `reports` view with an invalid user reference fails.
				MustNotInsert(t, db, schema, "02_create_constraint", "reports", map[string]string{
					"description": "no one report",
					"users_id":    "100",
					"users_zip":   "100",
				}, testutils.FKViolationErrorCode)
			},
			afterRollback: func(t *testing.T, db *sql.DB, schema string) {
				// The check constraint must not exists on the table.
				CheckConstraintMustNotExist(t, db, schema, "reports", "fk_users")
				// Functions, triggers and temporary columns are dropped.
				TableMustBeCleanedUp(t, db, schema, "reports", "users_id")
				TableMustBeCleanedUp(t, db, schema, "reports", "users_zip")
			},
			afterComplete: func(t *testing.T, db *sql.DB, schema string) {
				ValidatedForeignKeyMustExistWithReferentialAction(t, db, schema, "reports", "fk_users", migrations.ForeignKeyActionSETNULL, migrations.ForeignKeyActionNOACTION)
				// Functions, triggers and temporary columns are dropped.
				TableMustBeCleanedUp(t, db, schema, "reports", "users_id")
				TableMustBeCleanedUp(t, db, schema, "reports", "users_zip")

				// Inserting values into the new schema that the violate the check constraint must fail.
				MustNotInsert(t, db, schema, "02_create_constraint", "reports", map[string]string{
					"description": "no one report",
					"users_id":    "100",
					"users_zip":   "100",
				}, testutils.FKViolationErrorCode)

				rows := MustSelect(t, db, schema, "02_create_constraint", "reports")
				assert.Equal(t, []map[string]any{
					{"id": 1, "description": "random report", "users_id": 1, "users_zip": 12345},
					{"id": 2, "description": "alice report", "users_id": 1, "users_zip": 12345},
				}, rows)
			},
		},
		{
			name: "create unique constraint on a unique column and another column",
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
								{
									Name:     "email",
									Type:     "varchar(255)",
									Nullable: true,
								},
							},
							Constraints: []migrations.Constraint{
								{
									Name:    "unique_name",
									Type:    "unique",
									Columns: []string{"name"},
								},
							},
						},
					},
				},
				{
					Name: "02_create_constraint",
					Operations: migrations.Operations{
						&migrations.OpCreateConstraint{
							Name:    "unique_name_email",
							Table:   "users",
							Type:    "unique",
							Columns: []string{"email", "name"},
							Up: map[string]string{
								"name":  "name || random()",
								"email": "email || random()",
							},
							Down: map[string]string{
								"name":  "name",
								"email": "email",
							},
						},
					},
				},
			},
			afterStart: func(t *testing.T, db *sql.DB, schema string) {
				// The index has been created on the underlying table.
				IndexMustExist(t, db, schema, "users", "unique_name")
				IndexMustExist(t, db, schema, "users", "unique_name_email")

				// Inserting values into the old schema that violate uniqueness should succeed.
				MustInsert(t, db, schema, "01_add_table", "users", map[string]string{
					"name":  "alice",
					"email": "email",
				})
				MustInsert(t, db, schema, "01_add_table", "users", map[string]string{
					"name":  "bob",
					"email": "email",
				})

				// Inserting values into the new schema that violate uniqueness should fail.
				MustInsert(t, db, schema, "02_create_constraint", "users", map[string]string{
					"name":  "cat",
					"email": "email",
				})
				MustNotInsert(t, db, schema, "02_create_constraint", "users", map[string]string{
					"name":  "cat",
					"email": "email",
				}, testutils.UniqueViolationErrorCode)
			},
			afterRollback: func(t *testing.T, db *sql.DB, schema string) {
				// The index has been dropped from the the underlying table.
				IndexMustNotExist(t, db, schema, "users", "unique_name_email")

				// Functions, triggers and temporary columns are dropped.
				TableMustBeCleanedUp(t, db, schema, "users", "name", "email")
			},
			afterComplete: func(t *testing.T, db *sql.DB, schema string) {
				// Functions, triggers and temporary columns are dropped.
				TableMustBeCleanedUp(t, db, schema, "users", "name", "email")

				// Inserting values into the new schema that violate uniqueness should fail.
				MustInsert(t, db, schema, "02_create_constraint", "users", map[string]string{
					"name": "carol",
				})
				MustNotInsert(t, db, schema, "02_create_constraint", "users", map[string]string{
					"name": "carol",
				}, testutils.UniqueViolationErrorCode)
			},
		},
		{
			name: "create primary key constraint on multiple columns",
			migrations: []migrations.Migration{
				{
					Name: "01_add_table",
					Operations: migrations.Operations{
						&migrations.OpCreateTable{
							Name: "users",
							Columns: []migrations.Column{
								{
									Name: "id1",
									Type: "uuid",
								},
								{
									Name: "id2",
									Type: "uuid",
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
					Name: "02_create_constraint",
					Operations: migrations.Operations{
						&migrations.OpCreateConstraint{
							Name:    "id_pkey",
							Table:   "users",
							Type:    "primary_key",
							Columns: []string{"id1", "id2"},
							Up: map[string]string{
								"id1": "gen_random_uuid()",
								"id2": "gen_random_uuid()",
							},
							Down: map[string]string{
								"id1": "id1",
								"id2": "id2",
							},
						},
					},
				},
			},
			afterStart: func(t *testing.T, db *sql.DB, schema string) {
				// The index has been created on the underlying table.
				IndexMustExist(t, db, schema, "users", "id_pkey")

				// Inserting values into the old schema that violate uniqueness should succeed.
				MustInsert(t, db, schema, "01_add_table", "users", map[string]string{
					"id1":  "00000000-0000-0000-0000-000000000001",
					"id2":  "00000000-0000-0000-0000-000000000002",
					"name": "alice",
				})
				MustInsert(t, db, schema, "01_add_table", "users", map[string]string{
					"id1":  "00000000-0000-0000-0000-000000000001",
					"id2":  "00000000-0000-0000-0000-000000000002",
					"name": "bob",
				})

				MustInsert(t, db, schema, "02_create_constraint", "users", map[string]string{
					"id1":  "00000000-0000-0000-0000-000000000003",
					"id2":  "00000000-0000-0000-0000-000000000004",
					"name": "alice",
				})
				// Inserting values into the new schema that violate uniqueness should fail.
				MustNotInsert(t, db, schema, "02_create_constraint", "users", map[string]string{
					"id1":  "00000000-0000-0000-0000-000000000003",
					"id2":  "00000000-0000-0000-0000-000000000004",
					"name": "bob",
				}, testutils.UniqueViolationErrorCode)
			},
			afterRollback: func(t *testing.T, db *sql.DB, schema string) {
				// The index has been dropped from the the underlying table.
				IndexMustNotExist(t, db, schema, "users", "id_pkey")

				// Functions, triggers and temporary columns are dropped.
				TableMustBeCleanedUp(t, db, schema, "users", "id1")
				TableMustBeCleanedUp(t, db, schema, "users", "id2")
			},
			afterComplete: func(t *testing.T, db *sql.DB, schema string) {
				PrimaryKeyConstraintMustExist(t, db, schema, "users", "id_pkey")
			},
		},
	})
}

func TestCreateConstraintInMultiOperationMigrations(t *testing.T) {
	t.Parallel()

	ExecuteTests(t, TestCases{
		{
			name: "rename table, create check constraint",
			migrations: []migrations.Migration{
				{
					Name: "01_create_table",
					Operations: migrations.Operations{
						&migrations.OpCreateTable{
							Name: "items",
							Columns: []migrations.Column{
								{
									Name: "id",
									Type: "int",
									Pk:   true,
								},
								{
									Name:     "name",
									Type:     "varchar(255)",
									Nullable: true,
								},
							},
						},
					},
				},
				{
					Name: "02_multi_operation",
					Operations: migrations.Operations{
						&migrations.OpRenameTable{
							From: "items",
							To:   "products",
						},
						&migrations.OpCreateConstraint{
							Table:   "products",
							Type:    migrations.OpCreateConstraintTypeCheck,
							Name:    "check_name",
							Check:   ptr("length(name) > 3"),
							Columns: []string{"name"},
							Up: map[string]string{
								"name": "CASE WHEN length(name) <= 3 THEN name || '-xxx' ELSE name END",
							},
							Down: map[string]string{
								"name": "name",
							},
						},
					},
				},
			},
			afterStart: func(t *testing.T, db *sql.DB, schema string) {
				// Can insert a row into the new schema that meets the constraint
				MustInsert(t, db, schema, "02_multi_operation", "products", map[string]string{
					"id":   "1",
					"name": "apple",
				})

				// Can't insert a row into the new schema that violates the constraint
				MustNotInsert(t, db, schema, "02_multi_operation", "products", map[string]string{
					"id":   "2",
					"name": "abc",
				}, testutils.CheckViolationErrorCode)

				// Can insert a row into the old schema that violates the constraint
				MustInsert(t, db, schema, "01_create_table", "items", map[string]string{
					"id":   "2",
					"name": "abc",
				})

				// The new view has the expected rows
				rows := MustSelect(t, db, schema, "02_multi_operation", "products")
				assert.Equal(t, []map[string]any{
					{"id": 1, "name": "apple"},
					{"id": 2, "name": "abc-xxx"},
				}, rows)

				// The old view has the expected rows
				rows = MustSelect(t, db, schema, "01_create_table", "items")
				assert.Equal(t, []map[string]any{
					{"id": 1, "name": "apple"},
					{"id": 2, "name": "abc"},
				}, rows)
			},
			afterRollback: func(t *testing.T, db *sql.DB, schema string) {
				// The table has been cleaned up
				TableMustBeCleanedUp(t, db, schema, "items", "name")
			},
			afterComplete: func(t *testing.T, db *sql.DB, schema string) {
				// Can insert a row into the new schema that meets the constraint
				MustInsert(t, db, schema, "02_multi_operation", "products", map[string]string{
					"id":   "3",
					"name": "banana",
				})

				// Can't insert a row into the new schema that violates the constraint
				MustNotInsert(t, db, schema, "02_multi_operation", "products", map[string]string{
					"id":   "3",
					"name": "abc",
				}, testutils.CheckViolationErrorCode)

				// The new view has the expected rows
				rows := MustSelect(t, db, schema, "02_multi_operation", "products")
				assert.Equal(t, []map[string]any{
					{"id": 1, "name": "apple"},
					{"id": 2, "name": "abc-xxx"},
					{"id": 3, "name": "banana"},
				}, rows)

				// The table has been cleaned up
				TableMustBeCleanedUp(t, db, schema, "products", "name")
			},
		},
		{
			name: "rename table, rename column, create check constraint",
			migrations: []migrations.Migration{
				{
					Name: "01_create_table",
					Operations: migrations.Operations{
						&migrations.OpCreateTable{
							Name: "items",
							Columns: []migrations.Column{
								{
									Name: "id",
									Type: "int",
									Pk:   true,
								},
								{
									Name:     "name",
									Type:     "varchar(255)",
									Nullable: true,
								},
							},
						},
					},
				},
				{
					Name: "02_multi_operation",
					Operations: migrations.Operations{
						&migrations.OpRenameTable{
							From: "items",
							To:   "products",
						},
						&migrations.OpRenameColumn{
							Table: "products",
							From:  "name",
							To:    "item_name",
						},
						&migrations.OpCreateConstraint{
							Table:   "products",
							Type:    migrations.OpCreateConstraintTypeCheck,
							Name:    "check_item_name",
							Check:   ptr("length(item_name) > 3"),
							Columns: []string{"item_name"},
							Up: map[string]string{
								"item_name": "CASE WHEN length(item_name) <= 3 THEN item_name || '-xxx' ELSE item_name END",
							},
							Down: map[string]string{
								"item_name": "item_name",
							},
						},
					},
				},
			},
			afterStart: func(t *testing.T, db *sql.DB, schema string) {
				// Can insert a row into the new schema that meets the constraint
				MustInsert(t, db, schema, "02_multi_operation", "products", map[string]string{
					"id":        "1",
					"item_name": "apple",
				})

				// Can't insert a row into the new schema that violates the constraint
				MustNotInsert(t, db, schema, "02_multi_operation", "products", map[string]string{
					"id":        "2",
					"item_name": "abc",
				}, testutils.CheckViolationErrorCode)

				// Can insert a row into the old schema that violates the constraint
				MustInsert(t, db, schema, "01_create_table", "items", map[string]string{
					"id":   "2",
					"name": "abc",
				})

				// The new view has the expected rows
				rows := MustSelect(t, db, schema, "02_multi_operation", "products")
				assert.Equal(t, []map[string]any{
					{"id": 1, "item_name": "apple"},
					{"id": 2, "item_name": "abc-xxx"},
				}, rows)

				// The old view has the expected rows
				rows = MustSelect(t, db, schema, "01_create_table", "items")
				assert.Equal(t, []map[string]any{
					{"id": 1, "name": "apple"},
					{"id": 2, "name": "abc"},
				}, rows)
			},
			afterRollback: func(t *testing.T, db *sql.DB, schema string) {
				// The table has been cleaned up
				TableMustBeCleanedUp(t, db, schema, "items", "name")
			},
			afterComplete: func(t *testing.T, db *sql.DB, schema string) {
				// Can insert a row into the new schema that meets the constraint
				MustInsert(t, db, schema, "02_multi_operation", "products", map[string]string{
					"id":        "3",
					"item_name": "banana",
				})

				// Can't insert a row into the new schema that violates the constraint
				MustNotInsert(t, db, schema, "02_multi_operation", "products", map[string]string{
					"id":        "3",
					"item_name": "abc",
				}, testutils.CheckViolationErrorCode)

				// The new view has the expected rows
				rows := MustSelect(t, db, schema, "02_multi_operation", "products")
				assert.Equal(t, []map[string]any{
					{"id": 1, "item_name": "apple"},
					{"id": 2, "item_name": "abc-xxx"},
					{"id": 3, "item_name": "banana"},
				}, rows)

				// The table has been cleaned up
				TableMustBeCleanedUp(t, db, schema, "products", "name")
			},
		},
		{
			name: "rename table, rename column, create unique constraint",
			migrations: []migrations.Migration{
				{
					Name: "01_create_table",
					Operations: migrations.Operations{
						&migrations.OpCreateTable{
							Name: "items",
							Columns: []migrations.Column{
								{
									Name: "id",
									Type: "int",
									Pk:   true,
								},
								{
									Name:     "name",
									Type:     "varchar(255)",
									Nullable: true,
								},
							},
						},
					},
				},
				{
					Name: "02_multi_operation",
					Operations: migrations.Operations{
						&migrations.OpRenameTable{
							From: "items",
							To:   "products",
						},
						&migrations.OpRenameColumn{
							Table: "products",
							From:  "name",
							To:    "item_name",
						},
						&migrations.OpCreateConstraint{
							Table:   "products",
							Type:    migrations.OpCreateConstraintTypeUnique,
							Name:    "unique_item_name",
							Columns: []string{"item_name"},
							Up: map[string]string{
								"item_name": "item_name",
							},
							Down: map[string]string{
								"item_name": "item_name",
							},
						},
					},
				},
			},
			afterStart: func(t *testing.T, db *sql.DB, schema string) {
				// Can insert a row into the new schema
				MustInsert(t, db, schema, "02_multi_operation", "products", map[string]string{
					"id":        "1",
					"item_name": "apple",
				})

				// Can insert a row into the new schema that meets the constraint
				MustInsert(t, db, schema, "02_multi_operation", "products", map[string]string{
					"id":        "2",
					"item_name": "banana",
				})

				// Can't insert a row into the new schema that violates the constraint
				MustNotInsert(t, db, schema, "02_multi_operation", "products", map[string]string{
					"id":        "3",
					"item_name": "apple",
				}, testutils.UniqueViolationErrorCode)

				// The new view has the expected rows
				rows := MustSelect(t, db, schema, "02_multi_operation", "products")
				assert.Equal(t, []map[string]any{
					{"id": 1, "item_name": "apple"},
					{"id": 2, "item_name": "banana"},
				}, rows)

				// The old view has the expected rows
				rows = MustSelect(t, db, schema, "01_create_table", "items")
				assert.Equal(t, []map[string]any{
					{"id": 1, "name": "apple"},
					{"id": 2, "name": "banana"},
				}, rows)
			},
			afterRollback: func(t *testing.T, db *sql.DB, schema string) {
				// The table has been cleaned up
				TableMustBeCleanedUp(t, db, schema, "items", "name")
			},
			afterComplete: func(t *testing.T, db *sql.DB, schema string) {
				// Can insert a row into the new schema that meets the constraint
				MustInsert(t, db, schema, "02_multi_operation", "products", map[string]string{
					"id":        "3",
					"item_name": "carrot",
				})

				// Can't insert a row into the new schema that violates the constraint
				MustNotInsert(t, db, schema, "02_multi_operation", "products", map[string]string{
					"id":        "4",
					"item_name": "carrot",
				}, testutils.UniqueViolationErrorCode)

				// The new view has the expected rows
				rows := MustSelect(t, db, schema, "02_multi_operation", "products")
				assert.Equal(t, []map[string]any{
					{"id": 1, "item_name": "apple"},
					{"id": 2, "item_name": "banana"},
					{"id": 3, "item_name": "carrot"},
				}, rows)

				// The table has been cleaned up
				TableMustBeCleanedUp(t, db, schema, "products", "name")
			},
		},
		{
			name: "rename table, rename column, create foreign key constraint",
			migrations: []migrations.Migration{
				{
					Name: "01_create_tables",
					Operations: migrations.Operations{
						&migrations.OpCreateTable{
							Name: "users",
							Columns: []migrations.Column{
								{
									Name: "id",
									Type: "int",
									Pk:   true,
								},
								{
									Name:     "name",
									Type:     "varchar(255)",
									Nullable: true,
								},
							},
						},
						&migrations.OpCreateTable{
							Name: "items",
							Columns: []migrations.Column{
								{
									Name: "id",
									Type: "int",
									Pk:   true,
								},
								{
									Name:     "name",
									Type:     "varchar(255)",
									Nullable: true,
								},
								{
									Name:     "owner",
									Type:     "int",
									Nullable: true,
								},
							},
						},
					},
				},
				{
					Name: "02_multi_operation",
					Operations: migrations.Operations{
						&migrations.OpRenameTable{
							From: "items",
							To:   "products",
						},
						&migrations.OpRenameColumn{
							Table: "products",
							From:  "owner",
							To:    "owner_id",
						},
						&migrations.OpCreateConstraint{
							Table:   "products",
							Type:    migrations.OpCreateConstraintTypeForeignKey,
							Name:    "fk_item_owner",
							Columns: []string{"owner_id"},
							References: &migrations.TableForeignKeyReference{
								Table:   "users",
								Columns: []string{"id"},
							},
							Up: map[string]string{
								"owner_id": "SELECT CASE WHEN EXISTS (SELECT 1 FROM users WHERE users.id = owner_id) THEN owner_id ELSE NULL END",
							},
							Down: map[string]string{
								"owner_id": "owner_id",
							},
						},
					},
				},
			},
			afterStart: func(t *testing.T, db *sql.DB, schema string) {
				// Can insert a row into the users table
				MustInsert(t, db, schema, "02_multi_operation", "users", map[string]string{
					"id":   "1",
					"name": "alice",
				})

				// Can insert a row that meets the constraint into the new schema
				MustInsert(t, db, schema, "02_multi_operation", "products", map[string]string{
					"id":       "1",
					"name":     "apple",
					"owner_id": "1",
				})

				// Can't insert a row that violates the constraint into the new schema
				MustNotInsert(t, db, schema, "02_multi_operation", "products", map[string]string{
					"id":       "2",
					"name":     "banana",
					"owner_id": "2", // no such user
				}, testutils.FKViolationErrorCode)

				// Can insert a row that violates the constraint into the old schema
				MustInsert(t, db, schema, "01_create_tables", "items", map[string]string{
					"id":    "2",
					"name":  "banana",
					"owner": "2",
				})

				// The new view has the expected rows
				rows := MustSelect(t, db, schema, "02_multi_operation", "products")
				assert.Equal(t, []map[string]any{
					{"id": 1, "name": "apple", "owner_id": 1},
					{"id": 2, "name": "banana", "owner_id": nil},
				}, rows)

				// The old view has the expected rows
				rows = MustSelect(t, db, schema, "01_create_tables", "items")
				assert.Equal(t, []map[string]any{
					{"id": 1, "name": "apple", "owner": 1},
					{"id": 2, "name": "banana", "owner": 2},
				}, rows)
			},
			afterRollback: func(t *testing.T, db *sql.DB, schema string) {
				// The table has been cleaned up
				TableMustBeCleanedUp(t, db, schema, "items", "name")
			},
			afterComplete: func(t *testing.T, db *sql.DB, schema string) {
				// Can insert a row that meets the constraint into the new schema
				MustInsert(t, db, schema, "02_multi_operation", "products", map[string]string{
					"id":       "3",
					"name":     "carrot",
					"owner_id": "1",
				})

				// Can't insert a row into the new schema that violates the constraint
				MustNotInsert(t, db, schema, "02_multi_operation", "products", map[string]string{
					"id":       "4",
					"name":     "durian",
					"owner_id": "2", // no such user
				}, testutils.FKViolationErrorCode)

				// The new view has the expected rows
				rows := MustSelect(t, db, schema, "02_multi_operation", "products")
				assert.Equal(t, []map[string]any{
					{"id": 1, "name": "apple", "owner_id": 1},
					{"id": 2, "name": "banana", "owner_id": nil},
					{"id": 3, "name": "carrot", "owner_id": 1},
				}, rows)

				// The table has been cleaned up
				TableMustBeCleanedUp(t, db, schema, "products", "name")
			},
		},
	})
}

func TestCreateConstraintValidation(t *testing.T) {
	t.Parallel()

	invalidName := strings.Repeat("x", 64)
	ExecuteTests(t, TestCases{
		{
			name: "invalid constraint name",
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
								{
									Name:     "registered_at_year",
									Type:     "integer",
									Nullable: false,
								},
							},
						},
					},
				},
				{
					Name: "02_create_constraint_with_invalid_name",
					Operations: migrations.Operations{
						&migrations.OpCreateConstraint{
							Name:    invalidName,
							Table:   "users",
							Columns: []string{"registered_at_year"},
							Type:    "unique",
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
			name: "missing migration for constraint creation",
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
					Name: "02_create_constraint_with_missing_migration",
					Operations: migrations.Operations{
						&migrations.OpCreateConstraint{
							Name:    "unique_name",
							Table:   "users",
							Columns: []string{"name"},
							Type:    "unique",
							Up:      map[string]string{},
							Down: map[string]string{
								"name": "name",
							},
						},
					},
				},
			},
			wantStartErr:  migrations.ColumnMigrationMissingError{Table: "users", Name: "name"},
			afterStart:    func(t *testing.T, db *sql.DB, schema string) {},
			afterRollback: func(t *testing.T, db *sql.DB, schema string) {},
			afterComplete: func(t *testing.T, db *sql.DB, schema string) {},
		},
		{
			name: "expression of check constraint is missing",
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
					Name: "02_create_constraint_with_missing_migration",
					Operations: migrations.Operations{
						&migrations.OpCreateConstraint{
							Name:    "check_name",
							Table:   "users",
							Columns: []string{"name"},
							Type:    "check",
							Up: map[string]string{
								"name": "name",
							},
							Down: map[string]string{
								"name": "name",
							},
						},
					},
				},
			},
			wantStartErr:  migrations.FieldRequiredError{Name: "check"},
			afterStart:    func(t *testing.T, db *sql.DB, schema string) {},
			afterRollback: func(t *testing.T, db *sql.DB, schema string) {},
			afterComplete: func(t *testing.T, db *sql.DB, schema string) {},
		},
		{
			name: "missing referenced table for foreign key constraint creation",
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
					Name: "02_create_constraint_with_missing_referenced_table",
					Operations: migrations.Operations{
						&migrations.OpCreateConstraint{
							Name:    "fk_missing_table",
							Table:   "users",
							Columns: []string{"name"},
							Type:    "foreign_key",
							References: &migrations.TableForeignKeyReference{
								Table:   "missing_table",
								Columns: []string{"id"},
							},
							Up: map[string]string{
								"name": "name",
							},
							Down: map[string]string{
								"name": "name",
							},
						},
					},
				},
			},
			wantStartErr:  migrations.TableDoesNotExistError{Name: "missing_table"},
			afterStart:    func(t *testing.T, db *sql.DB, schema string) {},
			afterRollback: func(t *testing.T, db *sql.DB, schema string) {},
			afterComplete: func(t *testing.T, db *sql.DB, schema string) {},
		},
		{
			name: "create unique constraint on serial column",
			migrations: []migrations.Migration{
				{
					Name: "01_add_table",
					Operations: migrations.Operations{
						&migrations.OpCreateTable{
							Name: "users",
							Columns: []migrations.Column{
								{
									Name: "email",
									Type: "text",
									Pk:   true,
								},
								{
									Name: "id",
									Type: "serial",
								},
							},
						},
					},
				},
				{
					Name: "02_create_constraint",
					Operations: migrations.Operations{
						&migrations.OpCreateConstraint{
							Name:    "unique_id",
							Table:   "users",
							Type:    "unique",
							Columns: []string{"id"},
							Up: map[string]string{
								"id": "id",
							},
							Down: map[string]string{
								"id": "id",
							},
						},
					},
				},
			},
			afterStart: func(t *testing.T, db *sql.DB, schema string) {
				// The index has been created on the underlying table.
				IndexMustExist(t, db, schema, "users", "unique_id")
			},
			afterRollback: func(t *testing.T, db *sql.DB, schema string) {
				// The index has been dropped from the the underlying table.
				IndexMustNotExist(t, db, schema, "users", "unique_id")
			},
			afterComplete: func(t *testing.T, db *sql.DB, schema string) {
				// Functions, triggers and temporary columns are dropped.
				TableMustBeCleanedUp(t, db, schema, "users", "id")

				// Inserting values into the new schema that violate uniqueness should fail.
				MustInsert(t, db, schema, "02_create_constraint", "users", map[string]string{
					"email": "alice@xata.io", "id": "1",
				})
				MustNotInsert(t, db, schema, "02_create_constraint", "users", map[string]string{
					"email": "bob@xata.io", "id": "1",
				}, testutils.UniqueViolationErrorCode)
			},
		},
	})
}
