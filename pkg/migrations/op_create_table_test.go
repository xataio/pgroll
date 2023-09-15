package migrations_test

import (
	"database/sql"
	"testing"

	"github.com/xataio/pg-roll/pkg/migrations"

	"github.com/stretchr/testify/assert"
)

func TestCreateTable(t *testing.T) {
	t.Parallel()

	ExecuteTests(t, TestCases{
		{
			name: "create table",
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
			},
			afterStart: func(t *testing.T, db *sql.DB) {
				// The new view exists in the new version schema.
				ViewMustExist(t, db, "public", "01_create_table", "users")

				// Data can be inserted into the new view.
				MustInsert(t, db, "public", "01_create_table", "users", map[string]string{
					"name": "Alice",
				})

				// Data can be retrieved from the new view.
				rows := MustSelect(t, db, "public", "01_create_table", "users")
				assert.Equal(t, []map[string]any{
					{"id": 1, "name": "Alice"},
				}, rows)
			},
			afterRollback: func(t *testing.T, db *sql.DB) {
				// The underlying table has been dropped.
				TableMustNotExist(t, db, "public", "users")
			},
			afterComplete: func(t *testing.T, db *sql.DB) {
				// The view still exists
				ViewMustExist(t, db, "public", "01_create_table", "users")

				// Data can be inserted into the new view.
				MustInsert(t, db, "public", "01_create_table", "users", map[string]string{
					"name": "Alice",
				})

				// Data can be retrieved from the new view.
				rows := MustSelect(t, db, "public", "01_create_table", "users")
				assert.Equal(t, []map[string]any{
					{"id": 1, "name": "Alice"},
				}, rows)
			},
		},
		{
			name: "create table with foreign key",
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
					Name: "02_create_table_with_fk",
					Operations: migrations.Operations{
						&migrations.OpCreateTable{
							Name: "orders",
							Columns: []migrations.Column{
								{
									Name:       "id",
									Type:       "serial",
									PrimaryKey: true,
								},
								{
									Name: "user_id",
									Type: "integer",
									References: &migrations.ForeignKeyReference{
										Name:   "fk_users_id",
										Table:  "users",
										Column: "id",
									},
								},
								{
									Name: "quantity",
									Type: "integer",
								},
							},
						},
					},
				},
			},
			afterStart: func(t *testing.T, db *sql.DB) {
				// The foreign key constraint exists on the new table.
				ConstraintMustExist(t, db, "public", migrations.TemporaryName("orders"), "fk_users_id")

				// Inserting a row into the referenced table succeeds.
				MustInsert(t, db, "public", "01_create_table", "users", map[string]string{
					"name": "alice",
				})

				// Inserting a row into the referencing table succeeds as the referenced row exists.
				MustInsert(t, db, "public", "02_create_table_with_fk", "orders", map[string]string{
					"user_id":  "1",
					"quantity": "100",
				})

				// Inserting a row into the referencing table fails as the referenced row does not exist.
				MustNotInsert(t, db, "public", "02_create_table_with_fk", "orders", map[string]string{
					"user_id":  "2",
					"quantity": "200",
				})
			},
			afterRollback: func(t *testing.T, db *sql.DB) {
				// The table has been dropped, so the foreign key constraint is gone.
			},
			afterComplete: func(t *testing.T, db *sql.DB) {
				ConstraintMustExist(t, db, "public", "orders", "fk_users_id")

				// Inserting a row into the referenced table succeeds.
				MustInsert(t, db, "public", "02_create_table_with_fk", "users", map[string]string{
					"name": "bob",
				})

				// Inserting a row into the referencing table succeeds as the referenced row exists.
				MustInsert(t, db, "public", "02_create_table_with_fk", "orders", map[string]string{
					"user_id":  "2",
					"quantity": "200",
				})

				// Inserting a row into the referencing table fails as the referenced row does not exist.
				MustNotInsert(t, db, "public", "02_create_table_with_fk", "orders", map[string]string{
					"user_id":  "3",
					"quantity": "300",
				})
			},
		},
	})
}

func TestCreateTableValidation(t *testing.T) {
	t.Parallel()

	ExecuteTests(t, TestCases{TestCase{
		name: "foreign key validity",
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
				Name: "02_create_table_with_fk",
				Operations: migrations.Operations{
					&migrations.OpCreateTable{
						Name: "orders",
						Columns: []migrations.Column{
							{
								Name:       "id",
								Type:       "serial",
								PrimaryKey: true,
							},
							{
								Name: "user_id",
								Type: "integer",
								References: &migrations.ForeignKeyReference{
									Name:   "fk_users_doesntexist",
									Table:  "users",
									Column: "doesntexist",
								},
							},
							{
								Name: "quantity",
								Type: "integer",
							},
						},
					},
				},
			},
		},
		wantStartErr: migrations.ColumnReferenceError{
			Table:  "orders",
			Column: "user_id",
			Err:    migrations.ColumnDoesNotExistError{Table: "users", Name: "doesntexist"},
		},
	}})
}
