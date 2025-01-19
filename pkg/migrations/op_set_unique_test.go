// SPDX-License-Identifier: Apache-2.0

package migrations_test

import (
	"database/sql"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/xataio/pgroll/internal/testutils"
	"github.com/xataio/pgroll/pkg/migrations"
)

func TestSetColumnUnique(t *testing.T) {
	t.Parallel()

	ExecuteTests(t, TestCases{
		{
			name: "set unique with default down sql",
			migrations: []migrations.Migration{
				{
					Name: "01_add_table",
					Operations: migrations.Operations{
						&migrations.OpCreateTable{
							Name: "reviews",
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
								},
								{
									Name:     "product",
									Type:     "text",
									Nullable: false,
								},
								{
									Name:     "review",
									Type:     "text",
									Nullable: false,
								},
							},
						},
					},
				},
				{
					Name: "02_set_unique",
					Operations: migrations.Operations{
						&migrations.OpAlterColumn{
							Table:  "reviews",
							Column: "review",
							Unique: &migrations.UniqueConstraint{
								Name: "reviews_review_unique",
							},
							Up: "review || '-' || (random()*1000000)::integer",
						},
					},
				},
			},
			afterStart: func(t *testing.T, db *sql.DB, schema string) {
				// Inserting values into the old schema that violate uniqueness should succeed.
				MustInsert(t, db, schema, "01_add_table", "reviews", map[string]string{
					"username": "alice", "product": "apple", "review": "good",
				})
				MustInsert(t, db, schema, "01_add_table", "reviews", map[string]string{
					"username": "bob", "product": "banana", "review": "good",
				})

				// Inserting values into the new schema that violate uniqueness should fail.
				MustInsert(t, db, schema, "02_set_unique", "reviews", map[string]string{
					"username": "carl", "product": "carrot", "review": "bad",
				})
				MustNotInsert(t, db, schema, "02_set_unique", "reviews", map[string]string{
					"username": "dana", "product": "durian", "review": "bad",
				}, testutils.UniqueViolationErrorCode)
			},
			afterRollback: func(t *testing.T, db *sql.DB, schema string) {
				// The table is cleaned up; temporary columns, trigger functions and triggers no longer exist.
				TableMustBeCleanedUp(t, db, schema, "reviews", "review")
			},
			afterComplete: func(t *testing.T, db *sql.DB, schema string) {
				// The table is cleaned up; temporary columns, trigger functions and triggers no longer exist.
				TableMustBeCleanedUp(t, db, schema, "reviews", "review")

				// Inserting values into the new schema that violate uniqueness should fail.
				MustInsert(t, db, schema, "02_set_unique", "reviews", map[string]string{
					"username": "earl", "product": "elderberry", "review": "ok",
				})
				MustNotInsert(t, db, schema, "02_set_unique", "reviews", map[string]string{
					"username": "flora", "product": "fig", "review": "ok",
				}, testutils.UniqueViolationErrorCode)
			},
		},
		{
			name: "set unique with user supplied down sql",
			migrations: []migrations.Migration{
				{
					Name: "01_add_table",
					Operations: migrations.Operations{
						&migrations.OpCreateTable{
							Name: "reviews",
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
								},
								{
									Name:     "product",
									Type:     "text",
									Nullable: false,
								},
								{
									Name:     "review",
									Type:     "text",
									Nullable: false,
								},
							},
						},
					},
				},
				{
					Name: "02_set_unique",
					Operations: migrations.Operations{
						&migrations.OpAlterColumn{
							Table:  "reviews",
							Column: "review",
							Unique: &migrations.UniqueConstraint{
								Name: "reviews_review_unique",
							},
							Up:   "review || '-' || (random()*1000000)::integer",
							Down: "review || '!'",
						},
					},
				},
			},
			afterStart: func(t *testing.T, db *sql.DB, schema string) {
				// Inserting values into the new schema backfills the old column using the `down` SQL.
				MustInsert(t, db, schema, "02_set_unique", "reviews", map[string]string{
					"username": "carl", "product": "carrot", "review": "bad",
				})

				rows := MustSelect(t, db, schema, "01_add_table", "reviews")
				assert.Equal(t, []map[string]any{
					{"id": 1, "username": "carl", "product": "carrot", "review": "bad!"},
				}, rows)
			},
			afterRollback: func(t *testing.T, db *sql.DB, schema string) {
			},
			afterComplete: func(t *testing.T, db *sql.DB, schema string) {
			},
		},
		{
			name: "column defaults are preserved when adding a unique constraint",
			migrations: []migrations.Migration{
				{
					Name: "01_add_table",
					Operations: migrations.Operations{
						&migrations.OpCreateTable{
							Name: "reviews",
							Columns: []migrations.Column{
								{
									Name: "id",
									Type: "serial",
									Pk:   true,
								},
								{
									Name:    "username",
									Type:    "text",
									Default: ptr("'anonymous'"),
								},
								{
									Name: "product",
									Type: "text",
								},
								{
									Name: "review",
									Type: "text",
								},
							},
						},
					},
				},
				{
					Name: "02_set_unique",
					Operations: migrations.Operations{
						&migrations.OpAlterColumn{
							Table:  "reviews",
							Column: "username",
							Unique: &migrations.UniqueConstraint{
								Name: "reviews_username_unique",
							},
							Up:   "username",
							Down: "username",
						},
					},
				},
			},
			afterStart: func(t *testing.T, db *sql.DB, schema string) {
				// A row can be inserted into the new version of the table.
				MustInsert(t, db, schema, "02_set_unique", "reviews", map[string]string{
					"product": "apple", "review": "awesome",
				})

				// The newly inserted row respects the default value of the column.
				rows := MustSelect(t, db, schema, "02_set_unique", "reviews")
				assert.Equal(t, []map[string]any{
					{"id": 1, "username": "anonymous", "product": "apple", "review": "awesome"},
				}, rows)
			},
			afterRollback: func(t *testing.T, db *sql.DB, schema string) {
			},
			afterComplete: func(t *testing.T, db *sql.DB, schema string) {
				// Delete the row that was inserted in the `afterStart` hook to
				// ensure that another row with a default 'username' can be inserted
				// without violating the UNIQUE constraint on the column.
				MustDelete(t, db, schema, "02_set_unique", "reviews", map[string]string{
					"id": "1",
				})

				// A row can be inserted into the new version of the table.
				MustInsert(t, db, schema, "02_set_unique", "reviews", map[string]string{
					"product": "banana", "review": "bent",
				})

				// The newly inserted row respects the default value of the column.
				rows := MustSelect(t, db, schema, "02_set_unique", "reviews")
				assert.Equal(t, []map[string]any{
					{"id": 2, "username": "anonymous", "product": "banana", "review": "bent"},
				}, rows)
			},
		},
		{
			name: "foreign keys defined on the column are preserved when adding a unique constraint",
			migrations: []migrations.Migration{
				{
					Name: "01_add_departments_table",
					Operations: migrations.Operations{
						&migrations.OpCreateTable{
							Name: "departments",
							Columns: []migrations.Column{
								{
									Name: "id",
									Type: "serial",
									Pk:   true,
								},
								{
									Name:     "name",
									Type:     "text",
									Nullable: false,
								},
							},
						},
					},
				},
				{
					Name: "02_add_employees_table",
					Operations: migrations.Operations{
						&migrations.OpCreateTable{
							Name: "employees",
							Columns: []migrations.Column{
								{
									Name: "id",
									Type: "serial",
									Pk:   true,
								},
								{
									Name:     "name",
									Type:     "text",
									Nullable: false,
								},
								{
									Name:     "department_id",
									Type:     "integer",
									Nullable: true,
									References: &migrations.ForeignKeyReference{
										Name:     "fk_employee_department",
										Table:    "departments",
										Column:   "id",
										OnDelete: migrations.ForeignKeyReferenceOnDeleteSETNULL,
									},
								},
							},
						},
					},
				},
				{
					Name: "03_set_unique",
					Operations: migrations.Operations{
						&migrations.OpAlterColumn{
							Table:  "employees",
							Column: "department_id",
							Unique: &migrations.UniqueConstraint{
								Name: "employees_department_id_unique",
							},
							Up:   "department_id",
							Down: "department_id",
						},
					},
				},
			},
			afterStart: func(t *testing.T, db *sql.DB, schema string) {
				// A temporary FK constraint has been created on the temporary column
				ValidatedForeignKeyMustExist(t, db, schema, "employees", migrations.DuplicationName("fk_employee_department"), withOnDeleteSetNull())
			},
			afterRollback: func(t *testing.T, db *sql.DB, schema string) {
			},
			afterComplete: func(t *testing.T, db *sql.DB, schema string) {
				// The foreign key constraint still exists on the column
				ValidatedForeignKeyMustExist(t, db, schema, "employees", "fk_employee_department", withOnDeleteSetNull())
			},
		},
		{
			name: "check constraints are preserved when adding a unique constraint",
			migrations: []migrations.Migration{
				{
					Name: "01_add_table",
					Operations: migrations.Operations{
						&migrations.OpCreateTable{
							Name: "reviews",
							Columns: []migrations.Column{
								{
									Name: "id",
									Type: "serial",
									Pk:   true,
								},
								{
									Name: "username",
									Type: "text",
								},
								{
									Name: "review",
									Type: "text",
									Check: &migrations.CheckConstraint{
										Name:       "reviews_review_check",
										Constraint: "length(review) > 3",
									},
								},
							},
						},
					},
				},
				{
					Name: "02_set_unique",
					Operations: migrations.Operations{
						&migrations.OpAlterColumn{
							Table:  "reviews",
							Column: "username",
							Unique: &migrations.UniqueConstraint{
								Name: "reviews_username_unique",
							},
							Up:   "username",
							Down: "username",
						},
					},
				},
			},
			afterStart: func(t *testing.T, db *sql.DB, schema string) {
				// Inserting a row that violates the check constraint should fail.
				MustNotInsert(t, db, schema, "02_set_unique", "reviews", map[string]string{
					"username": "alice",
					"review":   "x",
				}, testutils.CheckViolationErrorCode)
			},
			afterRollback: func(t *testing.T, db *sql.DB, schema string) {
			},
			afterComplete: func(t *testing.T, db *sql.DB, schema string) {
				// Inserting a row that violates the check constraint should fail.
				MustNotInsert(t, db, schema, "02_set_unique", "reviews", map[string]string{
					"username": "bob",
					"review":   "y",
				}, testutils.CheckViolationErrorCode)
			},
		},
		{
			name: "not null is preserved when adding a unique constraint",
			migrations: []migrations.Migration{
				{
					Name: "01_add_table",
					Operations: migrations.Operations{
						&migrations.OpCreateTable{
							Name: "reviews",
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
								},
								{
									Name: "product",
									Type: "text",
								},
								{
									Name: "review",
									Type: "text",
								},
							},
						},
					},
				},
				{
					Name: "02_set_unique",
					Operations: migrations.Operations{
						&migrations.OpAlterColumn{
							Table:  "reviews",
							Column: "username",
							Unique: &migrations.UniqueConstraint{
								Name: "reviews_username_unique",
							},
							Up:   "username",
							Down: "username",
						},
					},
				},
			},
			afterStart: func(t *testing.T, db *sql.DB, schema string) {
				// Inserting a row that violates the NOT NULL constraint on `username` should fail.
				MustNotInsert(t, db, schema, "02_set_unique", "reviews", map[string]string{
					"product": "apple", "review": "awesome",
				}, testutils.NotNullViolationErrorCode)
			},
			afterRollback: func(t *testing.T, db *sql.DB, schema string) {
			},
			afterComplete: func(t *testing.T, db *sql.DB, schema string) {
				// Inserting a row that violates the NOT NULL constraint on `username` should fail.
				MustNotInsert(t, db, schema, "02_set_unique", "reviews", map[string]string{
					"product": "apple", "review": "awesome",
				}, testutils.NotNullViolationErrorCode)
			},
		},
		{
			name: "comments are preserved when adding a unique constraint",
			migrations: []migrations.Migration{
				{
					Name: "01_add_table",
					Operations: migrations.Operations{
						&migrations.OpCreateTable{
							Name: "reviews",
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
									Comment:  ptr("the name of the user"),
								},
								{
									Name: "product",
									Type: "text",
								},
								{
									Name: "review",
									Type: "text",
								},
							},
						},
					},
				},
				{
					Name: "02_set_unique",
					Operations: migrations.Operations{
						&migrations.OpAlterColumn{
							Table:  "reviews",
							Column: "username",
							Unique: &migrations.UniqueConstraint{
								Name: "reviews_username_unique",
							},
							Up:   "username",
							Down: "username",
						},
					},
				},
			},
			afterStart: func(t *testing.T, db *sql.DB, schema string) {
				// The duplicated column has a comment defined on it
				ColumnMustHaveComment(t, db, schema, "reviews", migrations.TemporaryName("username"), "the name of the user")
			},
			afterRollback: func(t *testing.T, db *sql.DB, schema string) {
			},
			afterComplete: func(t *testing.T, db *sql.DB, schema string) {
				// The final column has a comment defined on it
				ColumnMustHaveComment(t, db, schema, "reviews", "username", "the name of the user")
			},
		},
		// It should be possible to add multiple unique constraints to a column
		// once unique constraints covering multiple columns are supported.
		//
		// In that case it should be possible to test that existing unique constraints are
		// preserved when adding a new unique constraint covering the same column.
		{
			name: "validate that the constraint name is not already taken",
			migrations: []migrations.Migration{
				{
					Name: "01_add_table",
					Operations: migrations.Operations{
						&migrations.OpCreateTable{
							Name: "reviews",
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
								},
							},
						},
					},
				},
				{
					Name: "02_set_unique",
					Operations: migrations.Operations{
						&migrations.OpAlterColumn{
							Table:  "reviews",
							Column: "username",
							Unique: &migrations.UniqueConstraint{
								Name: "reviews_username_key",
							},
						},
					},
				},
				{
					Name: "03_set_unique_again",
					Operations: migrations.Operations{
						&migrations.OpAlterColumn{
							Table:  "reviews",
							Column: "username",
							Unique: &migrations.UniqueConstraint{
								Name: "reviews_username_key",
							},
						},
					},
				},
			},
			wantStartErr: migrations.ConstraintAlreadyExistsError{
				Table:      "reviews",
				Constraint: "reviews_username_key",
			},
			afterStart:    func(t *testing.T, db *sql.DB, schema string) {},
			afterRollback: func(t *testing.T, db *sql.DB, schema string) {},
			afterComplete: func(t *testing.T, db *sql.DB, schema string) {},
		},
	})
}

func TestSetUniqueInMultiOperationMigrations(t *testing.T) {
	t.Parallel()

	ExecuteTests(t, TestCases{
		{
			name: "rename table, set unique",
			migrations: []migrations.Migration{
				{
					Name: "01_create_table",
					Operations: migrations.Operations{
						&migrations.OpCreateTable{
							Name: "items",
							Columns: []migrations.Column{
								{
									Name: "id",
									Type: "serial",
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
						&migrations.OpAlterColumn{
							Table:  "products",
							Column: "name",
							Unique: &migrations.UniqueConstraint{
								Name: "products_name_unique",
							},
							Up:   "name || '-' || floor(random()*100000)::text",
							Down: "name",
						},
					},
				},
			},
			afterStart: func(t *testing.T, db *sql.DB, schema string) {
				// Can insert a row that meets the constraint into the new view
				MustInsert(t, db, schema, "02_multi_operation", "products", map[string]string{
					"name": "apple",
				})

				// Can't insert a row that violates the constraint into the new view
				MustNotInsert(t, db, schema, "02_multi_operation", "products", map[string]string{
					"name": "apple",
				}, testutils.UniqueViolationErrorCode)

				// Can insert a row that violates the constraint into the old view
				MustInsert(t, db, schema, "01_create_table", "items", map[string]string{
					"name": "apple",
				})
			},
			afterRollback: func(t *testing.T, db *sql.DB, schema string) {
				// The table has been cleaned up
				TableMustBeCleanedUp(t, db, schema, "items", "name")
			},
			afterComplete: func(t *testing.T, db *sql.DB, schema string) {
				// Can insert a row that meets the constraint into the new view
				MustInsert(t, db, schema, "02_multi_operation", "products", map[string]string{
					"name": "banana",
				})

				// Can't insert a row that violates the constraint into the new view
				MustNotInsert(t, db, schema, "02_multi_operation", "products", map[string]string{
					"name": "banana",
				}, testutils.UniqueViolationErrorCode)
			},
		},
		{
			name: "rename table, rename column, set unique",
			migrations: []migrations.Migration{
				{
					Name: "01_create_table",
					Operations: migrations.Operations{
						&migrations.OpCreateTable{
							Name: "items",
							Columns: []migrations.Column{
								{
									Name: "id",
									Type: "serial",
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
						&migrations.OpAlterColumn{
							Table:  "products",
							Column: "item_name",
							Unique: &migrations.UniqueConstraint{
								Name: "products_name_unique",
							},
							Up:   "item_name || '-' || floor(random()*100000)::text",
							Down: "item_name",
						},
					},
				},
			},
			afterStart: func(t *testing.T, db *sql.DB, schema string) {
				// Can insert a row that meets the constraint into the new view
				MustInsert(t, db, schema, "02_multi_operation", "products", map[string]string{
					"item_name": "apple",
				})

				// Can't insert a row that violates the constraint into the new view
				MustNotInsert(t, db, schema, "02_multi_operation", "products", map[string]string{
					"item_name": "apple",
				}, testutils.UniqueViolationErrorCode)

				// Can insert a row that violates the constraint into the old view
				MustInsert(t, db, schema, "01_create_table", "items", map[string]string{
					"name": "apple",
				})
			},
			afterRollback: func(t *testing.T, db *sql.DB, schema string) {
				// The table has been cleaned up
				TableMustBeCleanedUp(t, db, schema, "items", "name")
			},
			afterComplete: func(t *testing.T, db *sql.DB, schema string) {
				// Can insert a row that meets the constraint into the new view
				MustInsert(t, db, schema, "02_multi_operation", "products", map[string]string{
					"item_name": "banana",
				})

				// Can't insert a row that violates the constraint into the new view
				MustNotInsert(t, db, schema, "02_multi_operation", "products", map[string]string{
					"item_name": "banana",
				}, testutils.UniqueViolationErrorCode)
			},
		},
	})
}
