// SPDX-License-Identifier: Apache-2.0

package migrations_test

import (
	"database/sql"
	"strings"
	"testing"

	"github.com/xataio/pgroll/internal/testutils"
	"github.com/xataio/pgroll/pkg/migrations"
)

func TestRenameConstraint(t *testing.T) {
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
						Name:     "username",
						Type:     "text",
						Nullable: false,
						Check:    &migrations.CheckConstraint{Constraint: `LENGTH("username") > 2`, Name: "users_check"},
					},
				},
			},
		},
	}

	ExecuteTests(t, TestCases{{
		name: "rename constraint",
		migrations: []migrations.Migration{
			addTableMigration,
			{
				Name: "02_rename_constraint",
				Operations: migrations.Operations{
					&migrations.OpRenameConstraint{
						Table: "users",
						From:  "users_check",
						To:    "users_check_username_length",
					},
				},
			},
		},
		afterStart: func(t *testing.T, db *sql.DB, schema string) {
			// The check constraint in the underlying table has not been renamed.
			CheckConstraintMustExist(t, db, schema, "users", "users_check")

			// The new check constraint in the underlying table has not been created.
			CheckConstraintMustNotExist(t, db, schema, "users", "users_check_username_length")

			// Inserting a row that violates the check constraint should fail.
			MustNotInsert(t, db, schema, "02_rename_constraint", "users", map[string]string{
				"username": "a",
			}, testutils.CheckViolationErrorCode)
		},
		afterRollback: func(t *testing.T, db *sql.DB, schema string) {
			// // The check constraint in the underlying table has not been renamed.
			CheckConstraintMustExist(t, db, schema, "users", "users_check")
		},
		afterComplete: func(t *testing.T, db *sql.DB, schema string) {
			// The check constraint in the underlying table has been renamed.
			CheckConstraintMustExist(t, db, schema, "users", "users_check_username_length")

			// The old check constraint in the underlying table has been dropped.
			CheckConstraintMustNotExist(t, db, schema, "users", "users_check")

			// Inserting a row that violates the check constraint should fail.
			MustNotInsert(t, db, schema, "02_rename_constraint", "users", map[string]string{
				"username": "a",
			}, testutils.CheckViolationErrorCode)
		},
	}})
}

func TestRenameConstraintValidation(t *testing.T) {
	t.Parallel()

	invalidName := strings.Repeat("x", 64)
	createTableMigration := migrations.Migration{
		Name: "01_create_table",
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
						Name: "name",
						Type: "text",
						Check: &migrations.CheckConstraint{
							Constraint: `LENGTH("name") <= 2048`,
							Name:       "users_text_length_username",
						},
					},
				},
			},
		},
	}

	ExecuteTests(t, TestCases{
		{
			name: "the table must exist",
			migrations: []migrations.Migration{
				createTableMigration,
				{
					Name: "02_rename_constraint",
					Operations: migrations.Operations{
						&migrations.OpRenameConstraint{
							Table: "doesntexist",
							From:  "users_text_length_username",
							To:    "users_text_length_name",
						},
					},
				},
			},
			wantStartErr: migrations.TableDoesNotExistError{Name: "doesntexist"},
		},
		{
			name: "the from constraint must exist",
			migrations: []migrations.Migration{
				createTableMigration,
				{
					Name: "02_rename_constraint",
					Operations: migrations.Operations{
						&migrations.OpRenameConstraint{
							Table: "users",
							From:  "doesntexist",
							To:    "users_text_length_name",
						},
					},
				},
			},
			wantStartErr: migrations.ConstraintDoesNotExistError{Table: "users", Constraint: "doesntexist"},
		},
		{
			name: "the to constraint must not exist",
			migrations: []migrations.Migration{
				createTableMigration,
				{
					Name: "02_rename_constraint",
					Operations: migrations.Operations{
						&migrations.OpRenameConstraint{
							Table: "users",
							From:  "users_text_length_username",
							To:    "users_text_length_username",
						},
					},
				},
			},
			wantStartErr: migrations.ConstraintAlreadyExistsError{Table: "users", Constraint: "users_text_length_username"},
		},
		{
			name: "the new name must be valid",
			migrations: []migrations.Migration{
				createTableMigration,
				{
					Name: "02_rename_constraint",
					Operations: migrations.Operations{
						&migrations.OpRenameConstraint{
							Table: "users",
							From:  "users_text_length_username",
							To:    invalidName,
						},
					},
				},
			},
			wantStartErr: migrations.ValidateIdentifierLength(invalidName),
		},
	})
}
