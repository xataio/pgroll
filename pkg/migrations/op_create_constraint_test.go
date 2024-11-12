// SPDX-License-Identifier: Apache-2.0

package migrations_test

import (
	"database/sql"
	"strings"
	"testing"

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
							Up: migrations.OpCreateConstraintUp(map[string]interface{}{
								"name": "name || random()",
							}),
							Down: migrations.OpCreateConstraintDown(map[string]interface{}{
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
							Up: migrations.OpCreateConstraintUp(map[string]interface{}{
								"name":  "name || random()",
								"email": "email || random()",
							}),
							Down: migrations.OpCreateConstraintDown(map[string]interface{}{
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
