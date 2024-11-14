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

	invalidName := strings.Repeat("x", 64)
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
					Name: "02_create_constraint",
					Operations: migrations.Operations{
						&migrations.OpCreateConstraint{
							Name:    "unique_name",
							Table:   "users",
							Type:    "unique",
							Columns: []string{"name"},
							Up: migrations.OpCreateConstraintUp(map[string]string{
								"name": "name || random()",
							}),
							Down: migrations.OpCreateConstraintDown(map[string]string{
								"name": "name",
							}),
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
				tableCleanedUp(t, db, schema, "users", "name")
			},
			afterComplete: func(t *testing.T, db *sql.DB, schema string) {
				// Functions, triggers and temporary columns are dropped.
				tableCleanedUp(t, db, schema, "users", "name")

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
					Name: "02_create_constraint",
					Operations: migrations.Operations{
						&migrations.OpCreateConstraint{
							Name:    "name_letters",
							Table:   "users",
							Type:    "check",
							Check:   ptr("name ~ '^[a-zA-Z]+$'"),
							Columns: []string{"name"},
							Up: migrations.OpCreateConstraintUp(map[string]string{
								"name": "regexp_replace(name, '\\d+', '', 'g')",
							}),
							Down: migrations.OpCreateConstraintDown(map[string]string{
								"name": "name",
							}),
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
				tableCleanedUp(t, db, schema, "users", "name")
			},
			afterComplete: func(t *testing.T, db *sql.DB, schema string) {
				// Functions, triggers and temporary columns are dropped.
				tableCleanedUp(t, db, schema, "users", "name")

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
					Name: "02_create_constraint",
					Operations: migrations.Operations{
						&migrations.OpCreateConstraint{
							Name:    "unique_name_email",
							Table:   "users",
							Type:    "unique",
							Columns: []string{"name", "email"},
							Up: migrations.OpCreateConstraintUp(map[string]string{
								"name":  "name || random()",
								"email": "email || random()",
							}),
							Down: migrations.OpCreateConstraintDown(map[string]string{
								"name":  "name",
								"email": "email",
							}),
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
				tableCleanedUp(t, db, schema, "users", "name")
				tableCleanedUp(t, db, schema, "users", "email")
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
					Name: "02_create_constraint",
					Operations: migrations.Operations{
						&migrations.OpCreateConstraint{
							Name:    "check_name_email",
							Table:   "users",
							Type:    "check",
							Check:   ptr("name != email"),
							Columns: []string{"name", "email"},
							Up: migrations.OpCreateConstraintUp(map[string]string{
								"name":  "name",
								"email": "(SELECT CASE WHEN email ~ '@' THEN email ELSE email || '@example.com' END)",
							}),
							Down: migrations.OpCreateConstraintDown(map[string]string{
								"name":  "name",
								"email": "email",
							}),
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

				// Inserting values into the new schema that violate uniqueness should fail.
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
				tableCleanedUp(t, db, schema, "users", "name")
				tableCleanedUp(t, db, schema, "users", "email")
			},
			afterComplete: func(t *testing.T, db *sql.DB, schema string) {
				// Functions, triggers and temporary columns are dropped.
				tableCleanedUp(t, db, schema, "users", "name")
				tableCleanedUp(t, db, schema, "users", "email")

				rows := MustSelect(t, db, schema, "02_create_constraint", "users")
				assert.Equal(t, []map[string]any{
					{"id": 1, "name": "alice", "email": "alice@example.com"},
					{"id": 2, "name": "bob", "email": "bob@bob.me"},
				}, rows)
			},
		},
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
					Name: "02_create_constraint_with_missing_migration",
					Operations: migrations.Operations{
						&migrations.OpCreateConstraint{
							Name:    "unique_name",
							Table:   "users",
							Columns: []string{"name"},
							Type:    "unique",
							Up:      migrations.OpCreateConstraintUp(map[string]string{}),
							Down: migrations.OpCreateConstraintDown(map[string]string{
								"name": "name",
							}),
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
					Name: "02_create_constraint_with_missing_migration",
					Operations: migrations.Operations{
						&migrations.OpCreateConstraint{
							Name:    "check_name",
							Table:   "users",
							Columns: []string{"name"},
							Type:    "check",
							Up: migrations.OpCreateConstraintUp(map[string]string{
								"name": "name",
							}),
							Down: migrations.OpCreateConstraintDown(map[string]string{
								"name": "name",
							}),
						},
					},
				},
			},
			wantStartErr:  migrations.FieldRequiredError{Name: "check"},
			afterStart:    func(t *testing.T, db *sql.DB, schema string) {},
			afterRollback: func(t *testing.T, db *sql.DB, schema string) {},
			afterComplete: func(t *testing.T, db *sql.DB, schema string) {},
		},
	})
}

func tableCleanedUp(t *testing.T, db *sql.DB, schema, table, column string) {
	// The new, temporary column should not exist on the underlying table.
	ColumnMustNotExist(t, db, schema, table, migrations.TemporaryName(column))

	// The up function no longer exists.
	FunctionMustNotExist(t, db, schema, migrations.TriggerFunctionName(table, column))
	// The down function no longer exists.
	FunctionMustNotExist(t, db, schema, migrations.TriggerFunctionName(table, migrations.TemporaryName(column)))

	// The up trigger no longer exists.
	TriggerMustNotExist(t, db, schema, table, migrations.TriggerName(table, column))
	// The down trigger no longer exists.
	TriggerMustNotExist(t, db, schema, table, migrations.TriggerName(table, migrations.TemporaryName(column)))
}
