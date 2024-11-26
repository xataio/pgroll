// SPDX-License-Identifier: Apache-2.0

package migrations_test

import (
	"database/sql"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/xataio/pgroll/internal/testutils"
	"github.com/xataio/pgroll/pkg/migrations"
)

func TestDropMultiColumnConstraint(t *testing.T) {
	t.Parallel()

	ExecuteTests(t, TestCases{
		{
			name: "can drop a multi-column check constraint",
			migrations: []migrations.Migration{
				{
					Name: "01_add_table",
					Operations: migrations.Operations{
						&migrations.OpCreateTable{
							Name: "products",
							Columns: []migrations.Column{
								{
									Name: "id",
									Type: "integer",
									Pk:   ptr(true),
								},
								{
									Name: "name",
									Type: "text",
								},
								{
									Name: "price",
									Type: "integer",
								},
								{
									Name: "discount",
									Type: "integer",
								},
							},
						},
					},
				},
				{
					Name: "02_create_check_constraint",
					Operations: migrations.Operations{
						&migrations.OpCreateConstraint{
							Table:   "products",
							Name:    "products_check_price_discount",
							Columns: []string{"price", "discount"},
							Type:    migrations.OpCreateConstraintTypeCheck,
							Check:   ptr("(discount = 0) OR (price > 0)"),
							Up: map[string]string{
								"price":    "price",
								"discount": "discount",
							},
							Down: map[string]string{
								"price":    "price",
								"discount": "discount",
							},
						},
					},
				},
				{
					Name: "03_drop_check_constraint",
					Operations: migrations.Operations{
						&migrations.OpDropMultiColumnConstraint{
							Table: "products",
							Name:  "products_check_price_discount",
							Up: map[string]string{
								"price":    "price",
								"discount": "discount",
							},
							Down: map[string]string{
								"price":    "SELECT CASE price WHEN 0 THEN 100 ELSE 0 END",
								"discount": "discount",
							},
						},
					},
				},
			},
			afterStart: func(t *testing.T, db *sql.DB, schema string) {
				// Inserting a row into old schema that violates the check constraint fails.
				MustNotInsert(t, db, schema, "02_create_check_constraint", "products", map[string]string{
					"id":       "1",
					"name":     "apple",
					"price":    "0",
					"discount": "1",
				}, testutils.CheckViolationErrorCode)

				// Inserting a row into old schema that meets the check constraint succeeds.
				MustInsert(t, db, schema, "02_create_check_constraint", "products", map[string]string{
					"id":       "1",
					"name":     "apple",
					"price":    "1",
					"discount": "1",
				})

				// Inserting a row into the new schema that violates the check constraint succeeds.
				MustInsert(t, db, schema, "03_drop_check_constraint", "products", map[string]string{
					"id":       "2",
					"name":     "banana",
					"price":    "0",
					"discount": "1",
				})

				// The `products` table in the new schema has the expected data.
				rows := MustSelect(t, db, schema, "03_drop_check_constraint", "products")
				assert.Equal(t, []map[string]any{
					{"id": 1, "name": "apple", "price": 1, "discount": 1},
					{"id": 2, "name": "banana", "price": 0, "discount": 1},
				}, rows)

				// The `products` table in the old schema has the expected data.
				// The row that violated the check constraint was migrated using the down SQL.
				rows = MustSelect(t, db, schema, "02_create_check_constraint", "products")
				assert.Equal(t, []map[string]any{
					{"id": 1, "name": "apple", "price": 1, "discount": 1},
					{"id": 2, "name": "banana", "price": 100, "discount": 1},
				}, rows)
			},
			afterRollback: func(t *testing.T, db *sql.DB, schema string) {
				// The temporary columns no longer exist.
				ColumnMustNotExist(t, db, schema, "products", migrations.TemporaryName("price"))
				ColumnMustNotExist(t, db, schema, "products", migrations.TemporaryName("discount"))

				// The up functions no longer exist.
				FunctionMustNotExist(t, db, schema, migrations.TriggerFunctionName("products", "price"))
				FunctionMustNotExist(t, db, schema, migrations.TriggerFunctionName("products", "discount"))

				// The down functions no longer exist.
				FunctionMustNotExist(t, db, schema, migrations.TriggerFunctionName("products", migrations.TemporaryName("price")))
				FunctionMustNotExist(t, db, schema, migrations.TriggerFunctionName("products", migrations.TemporaryName("discount")))

				// The up triggers no longer exist.
				TriggerMustNotExist(t, db, schema, "products", migrations.TriggerName("products", "price"))
				TriggerMustNotExist(t, db, schema, "products", migrations.TriggerName("products", "discount"))

				// The down triggers no longer exist.
				TriggerMustNotExist(t, db, schema, "products", migrations.TriggerName("products", migrations.TemporaryName("price")))
				TriggerMustNotExist(t, db, schema, "products", migrations.TriggerName("products", migrations.TemporaryName("discount")))
			},
			afterComplete: func(t *testing.T, db *sql.DB, schema string) {
				// Inserting a row into the new schema that violates the check constraint succeeds.
				MustInsert(t, db, schema, "03_drop_check_constraint", "products", map[string]string{
					"id":       "3",
					"name":     "carrot",
					"price":    "0",
					"discount": "1",
				})

				// The `products` table in the new schema has the expected data.
				rows := MustSelect(t, db, schema, "03_drop_check_constraint", "products")
				assert.Equal(t, []map[string]any{
					{"id": 1, "name": "apple", "price": 1, "discount": 1},
					{"id": 2, "name": "banana", "price": 100, "discount": 1},
					{"id": 3, "name": "carrot", "price": 0, "discount": 1},
				}, rows)
			},
		},
		{
			name: "can drop a multi-column unique constraint",
			migrations: []migrations.Migration{
				{
					Name: "01_add_table",
					Operations: migrations.Operations{
						&migrations.OpCreateTable{
							Name: "products",
							Columns: []migrations.Column{
								{
									Name: "id",
									Type: "integer",
									Pk:   ptr(true),
								},
								{
									Name: "name",
									Type: "text",
								},
								{
									Name: "description",
									Type: "text",
								},
							},
						},
					},
				},
				{
					Name: "02_create_unique_constraint",
					Operations: migrations.Operations{
						&migrations.OpCreateConstraint{
							Table:   "products",
							Name:    "products_unique_name_description",
							Columns: []string{"name", "description"},
							Type:    migrations.OpCreateConstraintTypeUnique,
							Up: map[string]string{
								"name":        "name",
								"description": "description",
							},
							Down: map[string]string{
								"name":        "name",
								"description": "description",
							},
						},
					},
				},
				{
					Name: "03_drop_unique_constraint",
					Operations: migrations.Operations{
						&migrations.OpDropMultiColumnConstraint{
							Table: "products",
							Name:  "products_unique_name_description",
							Up: map[string]string{
								"name":        "name",
								"description": "description",
							},
							Down: map[string]string{
								"name":        "name || '-foo'",
								"description": "description",
							},
						},
					},
				},
			},
			afterStart: func(t *testing.T, db *sql.DB, schema string) {
				// Inserting an initial row into old schema succeeds.
				MustInsert(t, db, schema, "02_create_unique_constraint", "products", map[string]string{
					"id": "1", "name": "apple", "description": "red",
				})

				// Inserting a row into the old schema that violates the unique constraint fails.
				MustNotInsert(t, db, schema, "02_create_unique_constraint", "products", map[string]string{
					"id": "2", "name": "apple", "description": "red",
				}, testutils.UniqueViolationErrorCode)

				// Inserting a row into the new schema that violates the unique constraint succeeds.
				MustInsert(t, db, schema, "03_drop_unique_constraint", "products", map[string]string{
					"id": "2", "name": "apple", "description": "red",
				})

				// The `products` table in the new schema has the expected data.
				rows := MustSelect(t, db, schema, "03_drop_unique_constraint", "products")
				assert.Equal(t, []map[string]any{
					{"id": 1, "name": "apple", "description": "red"},
					{"id": 2, "name": "apple", "description": "red"},
				}, rows)

				// The `products` table in the old schema has the expected data.
				// The row that violated the unique constraint was migrated using the down SQL.
				rows = MustSelect(t, db, schema, "02_create_unique_constraint", "products")
				assert.Equal(t, []map[string]any{
					{"id": 1, "name": "apple", "description": "red"},
					{"id": 2, "name": "apple-foo", "description": "red"},
				}, rows)
			},
			afterRollback: func(t *testing.T, db *sql.DB, schema string) {
				// The temporary columns no longer exist.
				ColumnMustNotExist(t, db, schema, "products", migrations.TemporaryName("name"))
				ColumnMustNotExist(t, db, schema, "products", migrations.TemporaryName("description"))

				// The up functions no longer exist.
				FunctionMustNotExist(t, db, schema, migrations.TriggerFunctionName("products", "name"))
				FunctionMustNotExist(t, db, schema, migrations.TriggerFunctionName("products", "description"))

				// The down functions no longer exist.
				FunctionMustNotExist(t, db, schema, migrations.TriggerFunctionName("products", migrations.TemporaryName("name")))
				FunctionMustNotExist(t, db, schema, migrations.TriggerFunctionName("products", migrations.TemporaryName("description")))

				// The up triggers no longer exist.
				TriggerMustNotExist(t, db, schema, "products", migrations.TriggerName("products", "name"))
				TriggerMustNotExist(t, db, schema, "products", migrations.TriggerName("products", "description"))

				// The down triggers no longer exist.
				TriggerMustNotExist(t, db, schema, "products", migrations.TriggerName("products", migrations.TemporaryName("name")))
				TriggerMustNotExist(t, db, schema, "products", migrations.TriggerName("products", migrations.TemporaryName("description")))
			},
			afterComplete: func(t *testing.T, db *sql.DB, schema string) {
				// Inserting a row into the new schema that violates the unique constraint succeeds.
				MustInsert(t, db, schema, "03_drop_unique_constraint", "products", map[string]string{
					"id": "3", "name": "apple", "description": "red",
				})

				// The `products` table in the new schema has the expected data.
				rows := MustSelect(t, db, schema, "03_drop_unique_constraint", "products")
				assert.Equal(t, []map[string]any{
					{"id": 1, "name": "apple", "description": "red"},
					{"id": 2, "name": "apple-foo", "description": "red"},
					{"id": 3, "name": "apple", "description": "red"},
				}, rows)
			},
		},
		{
			name: "can drop a multi-column foreign key constraint",
			migrations: []migrations.Migration{
				{
					Name: "01_create_tables",
					Operations: migrations.Operations{
						&migrations.OpCreateTable{
							Name: "users",
							Columns: []migrations.Column{
								{
									Name: "id",
									Type: "integer",
									Pk:   ptr(true),
								},
								{
									Name:     "name",
									Type:     "varchar(255)",
									Nullable: ptr(false),
								},
								{
									Name: "zip",
									Type: "integer",
									Pk:   ptr(true),
								},
							},
						},
						&migrations.OpCreateTable{
							Name: "reports",
							Columns: []migrations.Column{
								{
									Name: "id",
									Type: "integer",
									Pk:   ptr(true),
								},
								{
									Name:     "description",
									Type:     "varchar(255)",
									Nullable: ptr(false),
								},
								{
									Name:     "user_id",
									Type:     "integer",
									Nullable: ptr(true),
								},
								{
									Name:     "user_zip",
									Type:     "integer",
									Nullable: ptr(true),
								},
							},
						},
					},
				},
				{
					Name: "02_create_fk_constraint",
					Operations: migrations.Operations{
						&migrations.OpCreateConstraint{
							Name:    "fk_users_reports",
							Table:   "reports",
							Type:    migrations.OpCreateConstraintTypeForeignKey,
							Columns: []string{"user_id", "user_zip"},
							References: &migrations.OpCreateConstraintReferences{
								Table:   "users",
								Columns: []string{"id", "zip"},
							},
							Up: map[string]string{
								"user_id":  "user_id",
								"user_zip": "user_zip",
							},
							Down: map[string]string{
								"user_id":  "user_id",
								"user_zip": "user_zip",
							},
						},
					},
				},
				{
					Name: "03_drop_fk_constraint",
					Operations: migrations.Operations{
						&migrations.OpDropMultiColumnConstraint{
							Table: "reports",
							Name:  "fk_users_reports",
							Up: map[string]string{
								"user_id":  "user_id",
								"user_zip": "user_zip",
							},
							Down: map[string]string{
								"user_id":  "1",
								"user_zip": "user_zip",
							},
						},
					},
				},
			},
			afterStart: func(t *testing.T, db *sql.DB, schema string) {
				// Insert a seed row into the `users` table.
				MustInsert(t, db, schema, "03_drop_fk_constraint", "users", map[string]string{
					"id": "1", "name": "alice", "zip": "12345",
				})

				// Inserting a row that meets the FK constraint into the old schema succeeds.
				MustInsert(t, db, schema, "02_create_fk_constraint", "reports", map[string]string{
					"id": "1", "description": "awesome", "user_id": "1", "user_zip": "12345",
				})

				// Inserting a row that violates the FK constraint into the old schema fails.
				MustNotInsert(t, db, schema, "02_create_fk_constraint", "reports", map[string]string{
					"id": "2", "description": "awesome", "user_id": "2", "user_zip": "12345",
				}, testutils.FKViolationErrorCode)

				// Inserting a row that violates the FK constraint into the new schema succeeds.
				MustInsert(t, db, schema, "03_drop_fk_constraint", "reports", map[string]string{
					"id": "2", "description": "better", "user_id": "2", "user_zip": "12345",
				})

				// The `reports` table in the new schema contains the expected rows
				rows := MustSelect(t, db, schema, "03_drop_fk_constraint", "reports")
				assert.Equal(t, []map[string]any{
					{"id": 1, "description": "awesome", "user_id": 1, "user_zip": 12345},
					{"id": 2, "description": "better", "user_id": 2, "user_zip": 12345},
				}, rows)

				// The `reports` table in the new schema contains the expected rows
				// The row that violated the FK constraint was migrated using the down SQL.
				rows = MustSelect(t, db, schema, "02_create_fk_constraint", "reports")
				assert.Equal(t, []map[string]any{
					{"id": 1, "description": "awesome", "user_id": 1, "user_zip": 12345},
					{"id": 2, "description": "better", "user_id": 1, "user_zip": 12345},
				}, rows)
			},
			afterRollback: func(t *testing.T, db *sql.DB, schema string) {
				// The temporary columns no longer exist.
				ColumnMustNotExist(t, db, schema, "reports", migrations.TemporaryName("user_id"))
				ColumnMustNotExist(t, db, schema, "reports", migrations.TemporaryName("user_zip"))

				// The up functions no longer exist.
				FunctionMustNotExist(t, db, schema, migrations.TriggerFunctionName("reports", "user_id"))
				FunctionMustNotExist(t, db, schema, migrations.TriggerFunctionName("reports", "user_zip"))

				// The down functions no longer exist.
				FunctionMustNotExist(t, db, schema, migrations.TriggerFunctionName("reports", migrations.TemporaryName("user_id")))
				FunctionMustNotExist(t, db, schema, migrations.TriggerFunctionName("reports", migrations.TemporaryName("user_zip")))

				// The up triggers no longer exist.
				TriggerMustNotExist(t, db, schema, "reports", migrations.TriggerName("reports", "user_id"))
				TriggerMustNotExist(t, db, schema, "reports", migrations.TriggerName("reports", "user_zip"))

				// The down triggers no longer exist.
				TriggerMustNotExist(t, db, schema, "reports", migrations.TriggerName("reports", migrations.TemporaryName("user_id")))
				TriggerMustNotExist(t, db, schema, "reports", migrations.TriggerName("reports", migrations.TemporaryName("user_zip")))
			},
			afterComplete: func(t *testing.T, db *sql.DB, schema string) {
				// Inserting a row that violates the FK constraint into the new schema succeeds.
				MustInsert(t, db, schema, "03_drop_fk_constraint", "reports", map[string]string{
					"id": "3", "description": "consistent", "user_id": "2", "user_zip": "12345",
				})

				// The `reports` table in the new schema contains the expected rows
				rows := MustSelect(t, db, schema, "03_drop_fk_constraint", "reports")
				assert.Equal(t, []map[string]any{
					{"id": 1, "description": "awesome", "user_id": 1, "user_zip": 12345},
					{"id": 2, "description": "better", "user_id": 1, "user_zip": 12345},
					{"id": 3, "description": "consistent", "user_id": 2, "user_zip": 12345},
				}, rows)
			},
		},
	})
}

func TestDropMultiColumnConstraintValidation(t *testing.T) {
	t.Parallel()

	createTableMigration := migrations.Migration{
		Name: "01_add_table",
		Operations: migrations.Operations{
			&migrations.OpCreateTable{
				Name: "posts",
				Columns: []migrations.Column{
					{
						Name: "id",
						Type: "serial",
						Pk:   ptr(true),
					},
					{
						Name: "title",
						Type: "text",
					},
				},
			},
		},
	}
	addCheckMigration := migrations.Migration{
		Name: "02_add_check_constraint",
		Operations: migrations.Operations{
			&migrations.OpAlterColumn{
				Table:  "posts",
				Column: "title",
				Check: &migrations.CheckConstraint{
					Name:       "check_title_length",
					Constraint: "length(title) > 3",
				},
				Up:   "SELECT CASE WHEN length(title) <= 3 THEN LPAD(title, 4, '-') ELSE title END",
				Down: "title",
			},
		},
	}

	ExecuteTests(t, TestCases{
		{
			name: "table must exist",
			migrations: []migrations.Migration{
				createTableMigration,
				addCheckMigration,
				{
					Name: "03_drop_check_constraint",
					Operations: migrations.Operations{
						&migrations.OpDropMultiColumnConstraint{
							Table: "doesntexist",
							Name:  "check_title_length",
							Up: map[string]string{
								"title": "title",
							},
							Down: map[string]string{
								"title": "SELECT CASE WHEN length(title) <= 3 THEN LPAD(title, 4, '-') ELSE title END",
							},
						},
					},
				},
			},
			wantStartErr: migrations.TableDoesNotExistError{Name: "doesntexist"},
		},
		{
			name: "constraint must exist",
			migrations: []migrations.Migration{
				createTableMigration,
				addCheckMigration,
				{
					Name: "03_drop_check_constraint",
					Operations: migrations.Operations{
						&migrations.OpDropMultiColumnConstraint{
							Table: "posts",
							Name:  "doesntexist",
							Up: map[string]string{
								"title": "title",
							},
							Down: map[string]string{
								"title": "SELECT CASE WHEN length(title) <= 3 THEN LPAD(title, 4, '-') ELSE title END",
							},
						},
					},
				},
			},
			wantStartErr: migrations.ConstraintDoesNotExistError{Table: "posts", Constraint: "doesntexist"},
		},
		{
			name: "name is mandatory",
			migrations: []migrations.Migration{
				createTableMigration,
				addCheckMigration,
				{
					Name: "03_drop_check_constraint",
					Operations: migrations.Operations{
						&migrations.OpDropMultiColumnConstraint{
							Table: "posts",
							Up: map[string]string{
								"title": "title",
							},
							Down: map[string]string{
								"title": "SELECT CASE WHEN length(title) <= 3 THEN LPAD(title, 4, '-') ELSE title END",
							},
						},
					},
				},
			},
			wantStartErr: migrations.FieldRequiredError{Name: "name"},
		},
		{
			name: "down SQL is mandatory",
			migrations: []migrations.Migration{
				createTableMigration,
				addCheckMigration,
				{
					Name: "03_drop_check_constraint",
					Operations: migrations.Operations{
						&migrations.OpDropMultiColumnConstraint{
							Table: "posts",
							Name:  "check_title_length",
							Up: map[string]string{
								"title": "title",
							},
						},
					},
				},
			},
			wantStartErr: migrations.FieldRequiredError{Name: "down"},
		},
		{
			name: "down SQL must be present for all columns covered by the constraint",
			migrations: []migrations.Migration{
				createTableMigration,
				addCheckMigration,
				{
					Name: "03_drop_check_constraint",
					Operations: migrations.Operations{
						&migrations.OpDropMultiColumnConstraint{
							Table: "posts",
							Name:  "check_title_length",
							Up: map[string]string{
								"title": "title",
							},
							Down: map[string]string{},
						},
					},
				},
			},
			wantStartErr: migrations.ColumnMigrationMissingError{Table: "posts", Name: "title"},
		},
		{
			name: "down SQL for columns not covered by the constraint is not allowed",
			migrations: []migrations.Migration{
				createTableMigration,
				addCheckMigration,
				{
					Name: "03_drop_check_constraint",
					Operations: migrations.Operations{
						&migrations.OpDropMultiColumnConstraint{
							Table: "posts",
							Name:  "check_title_length",
							Down: map[string]string{
								"title":       "SELECT CASE WHEN length(title) <= 3 THEN LPAD(title, 4, '-') ELSE title END",
								"not_covered": "not_covered",
							},
						},
					},
				},
			},
			wantStartErr: migrations.ColumnMigrationRedundantError{Table: "posts", Name: "not_covered"},
		},
		{
			name: "up SQL for columns not covered by the constraint is not allowed",
			migrations: []migrations.Migration{
				createTableMigration,
				addCheckMigration,
				{
					Name: "03_drop_check_constraint",
					Operations: migrations.Operations{
						&migrations.OpDropMultiColumnConstraint{
							Table: "posts",
							Name:  "check_title_length",
							Up: map[string]string{
								"not_covered": "not_covered",
							},
							Down: map[string]string{
								"title": "SELECT CASE WHEN length(title) <= 3 THEN LPAD(title, 4, '-') ELSE title END",
							},
						},
					},
				},
			},
			wantStartErr: migrations.ColumnMigrationRedundantError{Table: "posts", Name: "not_covered"},
		},
	})
}
