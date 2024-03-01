// SPDX-License-Identifier: Apache-2.0

package migrations_test

import (
	"database/sql"
	"testing"

	"github.com/stretchr/testify/assert"
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
						Pk:   ptr(true),
					},
					{
						Name:     "username",
						Type:     "text",
						Nullable: ptr(false),
						Check:    &migrations.CheckConstraint{Constraint: `LENGTH("username") <= 2048`, Name: "users_text_length_username"},
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
					&migrations.OpAlterColumn{
						Table:  "users",
						Column: "username",
						Name:   ptr("name"),
					},
					&migrations.OpRenameConstraint{
						Table: "users",
						From:  "users_text_length_username",
						To:    "users_text_length_name",
					},
				},
			},
		},
		afterStart: func(t *testing.T, db *sql.DB, schema string) {
			// The column in the underlying table has not been renamed.
			ColumnMustExist(t, db, schema, "users", "username")

			// Insertions to the new column name in the new version schema should work.
			MustInsert(t, db, schema, "02_rename_constraint", "users", map[string]string{"name": "alice"})

			// Insertions to the old column name in the old version schema should work.
			MustInsert(t, db, schema, "01_add_table", "users", map[string]string{"username": "bob"})

			// The check constraint in the underlying table has not been renamed.
			CheckConstraintMustExist(t, db, schema, "users", "users_text_length_username")

			// The new check constraint in the underlying table has not been created.
			CheckConstraintMustNotExist(t, db, schema, "users", "users_text_length_name")

			// Data can be read from the view in the new version schema.
			rows := MustSelect(t, db, schema, "02_rename_constraint", "users")
			assert.Equal(t, []map[string]any{
				{"id": 1, "name": "alice"},
				{"id": 2, "name": "bob"},
			}, rows)
		},
		afterRollback: func(t *testing.T, db *sql.DB, schema string) {
			// no-op
		},
		afterComplete: func(t *testing.T, db *sql.DB, schema string) {
			// The column in the underlying table has been renamed.
			ColumnMustExist(t, db, schema, "users", "name")

			// The check constraint in the underlying table has been renamed.
			CheckConstraintMustExist(t, db, schema, "users", "users_text_length_name")

			// The old check constraint in the underlying table has been dropped.
			CheckConstraintMustNotExist(t, db, schema, "users", "users_text_length_username")

			// Data can be read from the view in the new version schema.
			rows := MustSelect(t, db, schema, "02_rename_constraint", "users")
			assert.Equal(t, []map[string]any{
				{"id": 1, "name": "alice"},
				{"id": 2, "name": "bob"},
			}, rows)
		},
	}})
}
