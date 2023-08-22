package migrations_test

import (
	"database/sql"
	"testing"

	"github.com/xataio/pg-roll/pkg/migrations"
)

func TestDropTable(t *testing.T) {
	t.Parallel()

	ExecuteTests(t, TestCases{TestCase{
		name: "drop table",
		migrations: []migrations.Migration{
			{
				Name: "01_create_table",
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
								Name:   "name",
								Type:   "varchar(255)",
								Unique: true,
							},
						},
					},
				},
			},
			{
				Name: "02_drop_table",
				Operations: migrations.Operations{
					&migrations.OpDropTable{
						Name: "users",
					},
				},
			},
		},
		afterStart: func(t *testing.T, db *sql.DB) {
			// The view for the deleted table does not exist in the new version schema.
			ViewMustNotExist(t, db, "public", "02_drop_table", "users")

			// But the underlying table has not been deleted.
			TableMustExist(t, db, "public", "users")
		},
		afterRollback: func(t *testing.T, db *sql.DB) {
			// Rollback is a no-op.
		},
		afterComplete: func(t *testing.T, db *sql.DB) {
			// The underlying table has been deleted.
			TableMustNotExist(t, db, "public", "users")
		},
	}})
}
