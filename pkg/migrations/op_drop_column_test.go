package migrations_test

import (
	"database/sql"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/xataio/pg-roll/pkg/migrations"
	"github.com/xataio/pg-roll/pkg/roll"
)

func TestDropColumnWithDownSQL(t *testing.T) {
	t.Parallel()

	ExecuteTests(t, TestCases{{
		name: "drop column",
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
				Name: "02_drop_column",
				Operations: migrations.Operations{
					&migrations.OpDropColumn{
						Table:  "users",
						Column: "name",
						Down:   ptr("UPPER(email)"),
					},
				},
			},
		},
		afterStart: func(t *testing.T, db *sql.DB) {
			// The deleted column is not present on the view in the new version schema.
			versionSchema := roll.VersionedSchemaName("public", "02_drop_column")
			ColumnMustNotExist(t, db, versionSchema, "users", "name")

			// But the column is still present on the underlying table.
			ColumnMustExist(t, db, "public", "users", "name")

			// Inserting into the view in the new version schema should succeed.
			MustInsert(t, db, "public", "02_drop_column", "users", map[string]string{
				"email": "foo@example.com",
			})

			// The "down" SQL has populated the removed column ("name")
			results := MustSelect(t, db, "public", "01_add_table", "users")
			assert.Equal(t, []map[string]any{
				{"id": 1, "name": "FOO@EXAMPLE.COM", "email": "foo@example.com"},
			}, results)
		},
		afterRollback: func(t *testing.T, db *sql.DB) {
			// The trigger function has been dropped.
			triggerFnName := migrations.TriggerFunctionName("users", "name")
			FunctionMustNotExist(t, db, "public", triggerFnName)

			// The trigger has been dropped.
			triggerName := migrations.TriggerName("users", "name")
			TriggerMustNotExist(t, db, "public", "users", triggerName)
		},
		afterComplete: func(t *testing.T, db *sql.DB) {
			// The column has been deleted from the underlying table.
			ColumnMustNotExist(t, db, "public", "users", "name")

			// The trigger function has been dropped.
			triggerFnName := migrations.TriggerFunctionName("users", "name")
			FunctionMustNotExist(t, db, "public", triggerFnName)

			// The trigger has been dropped.
			triggerName := migrations.TriggerName("users", "name")
			TriggerMustNotExist(t, db, "public", "users", triggerName)

			// Inserting into the view in the new version schema should succeed.
			MustInsert(t, db, "public", "02_drop_column", "users", map[string]string{
				"email": "bar@example.com",
			})
			results := MustSelect(t, db, "public", "02_drop_column", "users")
			assert.Equal(t, []map[string]any{
				{"id": 1, "email": "foo@example.com"},
				{"id": 2, "email": "bar@example.com"},
			}, results)
		},
	}})
}
