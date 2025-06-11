// SPDX-License-Identifier: Apache-2.0

package migrations_test

import (
	"database/sql"
	"strings"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"

	"github.com/xataio/pgroll/internal/testutils"
	"github.com/xataio/pgroll/pkg/migrations"
)

func TestCreateTable(t *testing.T) {
	t.Parallel()

	ExecuteTests(t, TestCases{
		{
			name: "create table",
			migrations: []migrations.Migration{
				{
					Name:          "01_create_table",
					VersionSchema: "create_table",
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
									Name:   "name",
									Type:   "varchar(255)",
									Unique: true,
								},
							},
						},
					},
				},
			},
			afterStart: func(t *testing.T, db *sql.DB, schema string) {
				// The new view exists in the new version schema.
				ViewMustExist(t, db, schema, "create_table", "users")

				// Data can be inserted into the new view.
				MustInsert(t, db, schema, "create_table", "users", map[string]string{
					"name": "Alice",
				})

				// Data can be retrieved from the new view.
				rows := MustSelect(t, db, schema, "create_table", "users")
				assert.Equal(t, []map[string]any{
					{"id": 1, "name": "Alice"},
				}, rows)
			},
			afterRollback: func(t *testing.T, db *sql.DB, schema string) {
				// The underlying table has been dropped.
				TableMustNotExist(t, db, schema, "users")
			},
			afterComplete: func(t *testing.T, db *sql.DB, schema string) {
				// The view still exists
				ViewMustExist(t, db, schema, "create_table", "users")

				// Data can be inserted into the new view.
				MustInsert(t, db, schema, "create_table", "users", map[string]string{
					"name": "Alice",
				})

				// Data can be retrieved from the new view.
				rows := MustSelect(t, db, schema, "create_table", "users")
				assert.Equal(t, []map[string]any{
					{"id": 1, "name": "Alice"},
				}, rows)
			},
		},
		{
			name: "create table with composite key",
			migrations: []migrations.Migration{
				{
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
									Name: "rand",
									Type: "varchar(255)",
									Pk:   true,
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
			afterStart: func(t *testing.T, db *sql.DB, schema string) {
				// The new view exists in the new version schema.
				ViewMustExist(t, db, schema, "01_create_table", "users")

				// Data can be inserted into the new view.
				MustInsert(t, db, schema, "01_create_table", "users", map[string]string{
					"rand": "123",
					"name": "Alice",
				})
				// New record with same keys cannot be inserted.
				MustNotInsert(t, db, schema, "01_create_table", "users", map[string]string{
					"id":   "1",
					"rand": "123",
					"name": "Malice",
				}, testutils.UniqueViolationErrorCode)

				// Data can be retrieved from the new view.
				rows := MustSelect(t, db, schema, "01_create_table", "users")
				assert.Equal(t, []map[string]any{
					{"id": 1, "rand": "123", "name": "Alice"},
				}, rows)
			},
			afterRollback: func(t *testing.T, db *sql.DB, schema string) {
				// The underlying table has been dropped.
				TableMustNotExist(t, db, schema, "users")
			},
			afterComplete: func(t *testing.T, db *sql.DB, schema string) {
				// The view still exists
				ViewMustExist(t, db, schema, "01_create_table", "users")

				// The columns are still primary keys.
				ColumnMustBePK(t, db, schema, "users", "id")
				ColumnMustBePK(t, db, schema, "users", "rand")

				// Data can be inserted into the new view.
				MustInsert(t, db, schema, "01_create_table", "users", map[string]string{
					"rand": "123",
					"name": "Alice",
				})

				// New record with same keys cannot be inserted.
				MustNotInsert(t, db, schema, "01_create_table", "users", map[string]string{
					"id":   "1",
					"rand": "123",
					"name": "Malice",
				}, testutils.UniqueViolationErrorCode)

				// Data can be retrieved from the new view.
				rows := MustSelect(t, db, schema, "01_create_table", "users")
				assert.Equal(t, []map[string]any{
					{"id": 1, "rand": "123", "name": "Alice"},
				}, rows)
			},
		},
		{
			name: "create table with foreign key with default ON DELETE NO ACTION",
			migrations: []migrations.Migration{
				{
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
									Name: "id",
									Type: "serial",
									Pk:   true,
								},
								{
									Name: "user_id",
									Type: "integer",
									References: &migrations.ForeignKeyReference{
										Column: "id",
										Name:   "fk_users_id",
										Table:  "users",
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
			afterStart: func(t *testing.T, db *sql.DB, schema string) {
				// The foreign key constraint exists on the new table.
				ValidatedForeignKeyMustExist(t, db, schema, "orders", "fk_users_id")

				// Inserting a row into the referenced table succeeds.
				MustInsert(t, db, schema, "01_create_table", "users", map[string]string{
					"name": "alice",
				})

				// Inserting a row into the referencing table succeeds as the referenced row exists.
				MustInsert(t, db, schema, "02_create_table_with_fk", "orders", map[string]string{
					"user_id":  "1",
					"quantity": "100",
				})

				// Inserting a row into the referencing table fails as the referenced row does not exist.
				MustNotInsert(t, db, schema, "02_create_table_with_fk", "orders", map[string]string{
					"user_id":  "2",
					"quantity": "200",
				}, testutils.FKViolationErrorCode)

				// Deleting a row in the referenced table fails as a referencing row exists.
				MustNotDelete(t, db, schema, "02_create_table_with_fk", "users", map[string]string{
					"name": "alice",
				}, testutils.FKViolationErrorCode)
			},
			afterRollback: func(t *testing.T, db *sql.DB, schema string) {
				// The table has been dropped, so the foreign key constraint is gone.
			},
			afterComplete: func(t *testing.T, db *sql.DB, schema string) {
				ValidatedForeignKeyMustExist(t, db, schema, "orders", "fk_users_id")

				// Inserting a row into the referenced table succeeds.
				MustInsert(t, db, schema, "02_create_table_with_fk", "users", map[string]string{
					"name": "bob",
				})

				// Inserting a row into the referencing table succeeds as the referenced row exists.
				MustInsert(t, db, schema, "02_create_table_with_fk", "orders", map[string]string{
					"user_id":  "2",
					"quantity": "200",
				})

				// Inserting a row into the referencing table fails as the referenced row does not exist.
				MustNotInsert(t, db, schema, "02_create_table_with_fk", "orders", map[string]string{
					"user_id":  "3",
					"quantity": "300",
				}, testutils.FKViolationErrorCode)

				// Deleting a row in the referenced table fails as a referencing row exists.
				MustNotDelete(t, db, schema, "02_create_table_with_fk", "users", map[string]string{
					"name": "bob",
				}, testutils.FKViolationErrorCode)
			},
		},
		{
			name: "create table with foreign key with ON DELETE CASCADE",
			migrations: []migrations.Migration{
				{
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
									Name: "id",
									Type: "serial",
									Pk:   true,
								},
								{
									Name: "user_id",
									Type: "integer",
									References: &migrations.ForeignKeyReference{
										Column:   "id",
										Name:     "fk_users_id",
										Table:    "users",
										OnDelete: migrations.ForeignKeyActionCASCADE,
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
			afterStart: func(t *testing.T, db *sql.DB, schema string) {
				// The foreign key constraint exists on the new table.
				ValidatedForeignKeyMustExistWithReferentialAction(
					t,
					db,
					schema,
					"orders",
					"fk_users_id",
					migrations.ForeignKeyActionCASCADE,
					migrations.ForeignKeyActionNOACTION)

				// Inserting a row into the referenced table succeeds.
				MustInsert(t, db, schema, "01_create_table", "users", map[string]string{
					"name": "alice",
				})

				// Inserting a row into the referencing table succeeds as the referenced row exists.
				MustInsert(t, db, schema, "02_create_table_with_fk", "orders", map[string]string{
					"user_id":  "1",
					"quantity": "100",
				})

				// Inserting a row into the referencing table fails as the referenced row does not exist.
				MustNotInsert(t, db, schema, "02_create_table_with_fk", "orders", map[string]string{
					"user_id":  "2",
					"quantity": "200",
				}, testutils.FKViolationErrorCode)

				// Deleting a row in the referenced table succeeds because of the ON DELETE CASCADE.
				MustDelete(t, db, schema, "02_create_table_with_fk", "users", map[string]string{
					"name": "alice",
				})

				// The row in the referencing table has been deleted by the ON DELETE CASCADE.
				rows := MustSelect(t, db, schema, "02_create_table_with_fk", "orders")
				assert.Empty(t, rows)
			},
			afterRollback: func(t *testing.T, db *sql.DB, schema string) {
				// The table has been dropped, so the foreign key constraint is gone.
			},
			afterComplete: func(t *testing.T, db *sql.DB, schema string) {
				ValidatedForeignKeyMustExistWithReferentialAction(
					t,
					db,
					schema,
					"orders",
					"fk_users_id",
					migrations.ForeignKeyActionCASCADE,
					migrations.ForeignKeyActionNOACTION)

				// Inserting a row into the referenced table succeeds.
				MustInsert(t, db, schema, "02_create_table_with_fk", "users", map[string]string{
					"name": "bob",
				})

				// Inserting a row into the referencing table succeeds as the referenced row exists.
				MustInsert(t, db, schema, "02_create_table_with_fk", "orders", map[string]string{
					"user_id":  "2",
					"quantity": "200",
				})

				// Inserting a row into the referencing table fails as the referenced row does not exist.
				MustNotInsert(t, db, schema, "02_create_table_with_fk", "orders", map[string]string{
					"user_id":  "3",
					"quantity": "300",
				}, testutils.FKViolationErrorCode)

				// Deleting a row in the referenced table succeeds because of the ON DELETE CASCADE.
				MustDelete(t, db, schema, "02_create_table_with_fk", "users", map[string]string{
					"name": "bob",
				})

				// The row in the referencing table has been deleted by the ON DELETE CASCADE.
				rows := MustSelect(t, db, schema, "02_create_table_with_fk", "orders")
				assert.Empty(t, rows)
			},
		},
		{
			name: "create table with foreign key with default ON UPDATE and ON UPDATE CASADE",
			migrations: []migrations.Migration{
				{
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
									Name: "id",
									Type: "serial",
									Pk:   true,
								},
								{
									Name: "user_id",
									Type: "integer",
									References: &migrations.ForeignKeyReference{
										Column:   "id",
										Name:     "fk_users_id",
										Table:    "users",
										OnDelete: migrations.ForeignKeyActionCASCADE,
										OnUpdate: migrations.ForeignKeyActionCASCADE,
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
			afterStart: func(t *testing.T, db *sql.DB, schema string) {
				// The foreign key constraint exists on the new table.
				ValidatedForeignKeyMustExistWithReferentialAction(
					t,
					db,
					schema,
					"orders",
					"fk_users_id",
					migrations.ForeignKeyActionCASCADE,
					migrations.ForeignKeyActionCASCADE)

				// Inserting a row into the referenced table succeeds.
				MustInsert(t, db, schema, "01_create_table", "users", map[string]string{
					"name": "alice",
				})

				// Inserting a row into the referencing table succeeds as the referenced row exists.
				MustInsert(t, db, schema, "02_create_table_with_fk", "orders", map[string]string{
					"user_id":  "1",
					"quantity": "100",
				})

				// Inserting a row into the referencing table fails as the referenced row does not exist.
				MustNotInsert(t, db, schema, "02_create_table_with_fk", "orders", map[string]string{
					"user_id":  "2",
					"quantity": "200",
				}, testutils.FKViolationErrorCode)

				// Deleting a row in the referenced table succeeds and cascades.
				MustDelete(t, db, schema, "02_create_table_with_fk", "users", map[string]string{
					"name": "alice",
				})
				rows := MustSelect(t, db, schema, "02_create_table_with_fk", "users")
				assert.Equal(t, []map[string]any{}, rows)
			},
			afterRollback: func(t *testing.T, db *sql.DB, schema string) {
				// The table has been dropped, so the foreign key constraint is gone.
			},
			afterComplete: func(t *testing.T, db *sql.DB, schema string) {
				ValidatedForeignKeyMustExistWithReferentialAction(
					t,
					db,
					schema,
					"orders",
					"fk_users_id",
					migrations.ForeignKeyActionCASCADE,
					migrations.ForeignKeyActionCASCADE)

				// Inserting a row into the referenced table succeeds.
				MustInsert(t, db, schema, "02_create_table_with_fk", "users", map[string]string{
					"name": "bob",
				})

				// Inserting a row into the referencing table succeeds as the referenced row exists.
				MustInsert(t, db, schema, "02_create_table_with_fk", "orders", map[string]string{
					"user_id":  "2",
					"quantity": "200",
				})

				// Inserting a row into the referencing table fails as the referenced row does not exist.
				MustNotInsert(t, db, schema, "02_create_table_with_fk", "orders", map[string]string{
					"user_id":  "3",
					"quantity": "300",
				}, testutils.FKViolationErrorCode)

				// Deleting a row in the referenced table succeeds and cascades.
				MustDelete(t, db, schema, "02_create_table_with_fk", "users", map[string]string{
					"name": "bob",
				})
				rows := MustSelect(t, db, schema, "02_create_table_with_fk", "users")
				assert.Equal(t, []map[string]any{}, rows)
			},
		},
		{
			name: "create table with a check constraint on column",
			migrations: []migrations.Migration{
				{
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
										Name:       "check_name_length",
										Constraint: "length(name) > 3",
										NoInherit:  true,
									},
								},
							},
						},
					},
				},
			},
			afterStart: func(t *testing.T, db *sql.DB, schema string) {
				// The check constraint exists on the new table.
				NotInheritableCheckConstraintMustExist(t, db, schema, "users", "check_name_length")

				// Inserting a row into the table succeeds when the check constraint is satisfied.
				MustInsert(t, db, schema, "01_create_table", "users", map[string]string{
					"name": "alice",
				})

				// Inserting a row into the table fails when the check constraint is not satisfied.
				MustNotInsert(t, db, schema, "01_create_table", "users", map[string]string{
					"name": "b",
				}, testutils.CheckViolationErrorCode)
			},
			afterRollback: func(t *testing.T, db *sql.DB, schema string) {
				// The table has been dropped, so the check constraint is gone.
			},
			afterComplete: func(t *testing.T, db *sql.DB, schema string) {
				// The check constraint exists on the new table.
				NotInheritableCheckConstraintMustExist(t, db, schema, "users", "check_name_length")

				// Inserting a row into the table succeeds when the check constraint is satisfied.
				MustInsert(t, db, schema, "01_create_table", "users", map[string]string{
					"name": "bobby",
				})

				// Inserting a row into the table fails when the check constraint is not satisfied.
				MustNotInsert(t, db, schema, "01_create_table", "users", map[string]string{
					"name": "c",
				}, testutils.CheckViolationErrorCode)
			},
		},
		{
			name: "create table with a table check constraint",
			migrations: []migrations.Migration{
				{
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
								},
							},
							Constraints: []migrations.Constraint{
								{
									Name:  "check_name_length",
									Type:  "check",
									Check: "length(name) > 3",
								},
							},
						},
					},
				},
			},
			afterStart: func(t *testing.T, db *sql.DB, schema string) {
				// The check constraint exists on the new table.
				CheckConstraintMustExist(t, db, schema, "users", "check_name_length")

				// Inserting a row into the table succeeds when the check constraint is satisfied.
				MustInsert(t, db, schema, "01_create_table", "users", map[string]string{
					"name": "alice",
				})

				// Inserting a row into the table fails when the check constraint is not satisfied.
				MustNotInsert(t, db, schema, "01_create_table", "users", map[string]string{
					"name": "b",
				}, testutils.CheckViolationErrorCode)
			},
			afterRollback: func(t *testing.T, db *sql.DB, schema string) {
				// The table has been dropped, so the check constraint is gone.
			},
			afterComplete: func(t *testing.T, db *sql.DB, schema string) {
				// The check constraint exists on the new table.
				CheckConstraintMustExist(t, db, schema, "users", "check_name_length")

				// Inserting a row into the table succeeds when the check constraint is satisfied.
				MustInsert(t, db, schema, "01_create_table", "users", map[string]string{
					"name": "bobby",
				})

				// Inserting a row into the table fails when the check constraint is not satisfied.
				MustNotInsert(t, db, schema, "01_create_table", "users", map[string]string{
					"name": "c",
				}, testutils.CheckViolationErrorCode)
			},
		},
		{
			name: "create table with column and table comments",
			migrations: []migrations.Migration{
				{
					Name: "01_create_table",
					Operations: migrations.Operations{
						&migrations.OpCreateTable{
							Name:    "users",
							Comment: ptr("the users table"),
							Columns: []migrations.Column{
								{
									Name: "id",
									Type: "serial",
									Pk:   true,
								},
								{
									Name:    "name",
									Type:    "varchar(255)",
									Unique:  true,
									Comment: ptr("the username"),
								},
							},
						},
					},
				},
			},
			afterStart: func(t *testing.T, db *sql.DB, schema string) {
				// The comment has been added to the underlying table.
				TableMustHaveComment(t, db, schema, "users", "the users table")
				// The comment has been added to the underlying column.
				ColumnMustHaveComment(t, db, schema, "users", "name", "the username")
			},
			afterRollback: func(t *testing.T, db *sql.DB, schema string) {
			},
			afterComplete: func(t *testing.T, db *sql.DB, schema string) {
				// The comment is still present on the underlying table.
				TableMustHaveComment(t, db, schema, "users", "the users table")
				// The comment is still present on the underlying column.
				ColumnMustHaveComment(t, db, schema, "users", "name", "the username")
			},
		},
		{
			name: "create table with a unique table constraint",
			migrations: []migrations.Migration{
				{
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
								},
							},
							Constraints: []migrations.Constraint{
								{
									Name: "unique_name",
									Type: migrations.ConstraintTypeUnique,
									Columns: []string{
										"name",
									},
								},
							},
						},
					},
				},
			},
			afterStart: func(t *testing.T, db *sql.DB, schema string) {
				// The unique constraint exists on the new table.
				UniqueConstraintMustExist(t, db, schema, "users", "unique_name")

				// Inserting a row into the table succeeds when the unique constraint is satisfied.
				MustInsert(t, db, schema, "01_create_table", "users", map[string]string{
					"name": "alice",
				})

				// Inserting a row into the table fails when the unique constraint is not satisfied.
				MustNotInsert(t, db, schema, "01_create_table", "users", map[string]string{
					"name": "alice",
				}, testutils.UniqueViolationErrorCode)
			},
			afterRollback: func(t *testing.T, db *sql.DB, schema string) {
				// The table has been dropped, so the unique constraint is gone.
			},
			afterComplete: func(t *testing.T, db *sql.DB, schema string) {
				// The check constraint exists on the new table.
				UniqueConstraintMustExist(t, db, schema, "users", "unique_name")

				// Inserting a row into the table succeeds when the unique constraint is satisfied.
				MustInsert(t, db, schema, "01_create_table", "users", map[string]string{
					"name": "bobby",
				})

				// Inserting a row into the table fails when the unique constraint is not satisfied.
				MustNotInsert(t, db, schema, "01_create_table", "users", map[string]string{
					"name": "bobby",
				}, testutils.UniqueViolationErrorCode)
			},
		},
		{
			name: "create table with composite primary key and migrate it to test backfilling",
			migrations: []migrations.Migration{
				{
					Name: "01_create_table",
					Operations: migrations.Operations{
						&migrations.OpCreateTable{
							Name: "users",
							Columns: []migrations.Column{
								{
									Name: "id",
									Type: "uuid",
									Pk:   true,
								},
								{
									Name: "name",
									Type: "text",
									Pk:   true,
								},
								{
									Name:     "city",
									Type:     "text",
									Nullable: true,
								},
							},
						},
					},
				},
				{
					Name: "02_add_constraint",
					Operations: migrations.Operations{
						&migrations.OpCreateConstraint{
							Name:    "nowhere_forbidden",
							Table:   "users",
							Columns: []string{"city"},
							Type:    migrations.OpCreateConstraintTypeCheck,
							Check:   ptr("city != 'nowhere'"),
							Up: migrations.MultiColumnUpSQL{
								"city": "'chicago'",
							},
							Down: migrations.MultiColumnDownSQL{
								"city": "city",
							},
						},
					},
				},
			},
			afterStart: func(t *testing.T, db *sql.DB, schema string) {
				// Inserting a row into the table succeeds when the unique constraint is satisfied.
				MustInsert(t, db, schema, "01_create_table", "users", map[string]string{
					"id":   "00000000-0000-0000-0000-000000000001",
					"name": "alice",
					"city": "new york",
				})
				MustInsert(t, db, schema, "01_create_table", "users", map[string]string{
					"id":   "00000000-0000-0000-0000-000000000002",
					"name": "bob",
					"city": "new york",
				})
				MustInsert(t, db, schema, "01_create_table", "users", map[string]string{
					"id":   "00000000-0000-0000-0000-000000000003",
					"name": "carol",
				})
				id1, _ := uuid.MustParse("00000000-0000-0000-0000-000000000001").MarshalText()
				id2, _ := uuid.MustParse("00000000-0000-0000-0000-000000000002").MarshalText()
				id3, _ := uuid.MustParse("00000000-0000-0000-0000-000000000003").MarshalText()
				rows := MustSelect(t, db, schema, "01_create_table", "users")
				assert.Equal(t, []map[string]any{
					{"id": id1, "name": "alice", "city": "new york"},
					{"id": id2, "name": "bob", "city": "new york"},
					{"id": id3, "name": "carol", "city": nil},
				}, rows)
			},
			afterRollback: func(t *testing.T, db *sql.DB, schema string) {},
			afterComplete: func(t *testing.T, db *sql.DB, schema string) {
				id1, _ := uuid.MustParse("00000000-0000-0000-0000-000000000001").MarshalText()
				id2, _ := uuid.MustParse("00000000-0000-0000-0000-000000000002").MarshalText()
				id3, _ := uuid.MustParse("00000000-0000-0000-0000-000000000003").MarshalText()
				rows := MustSelect(t, db, schema, "02_add_constraint", "users")
				assert.Equal(t, []map[string]any{
					{"id": id1, "name": "alice", "city": "chicago"},
					{"id": id2, "name": "bob", "city": "chicago"},
					{"id": id3, "name": "carol", "city": "chicago"},
				}, rows)
			},
		},
		{
			name: "create table with primary key constraint",
			migrations: []migrations.Migration{
				{
					Name: "01_create_table",
					Operations: migrations.Operations{
						&migrations.OpCreateTable{
							Name: "users",
							Columns: []migrations.Column{
								{
									Name: "id",
									Type: "int",
								},
								{
									Name: "name",
									Type: "text",
								},
							},
							Constraints: []migrations.Constraint{
								{
									Name:    "pk_users",
									Type:    migrations.ConstraintTypePrimaryKey,
									Columns: []string{"id"},
								},
							},
						},
					},
				},
			},
			afterStart: func(t *testing.T, db *sql.DB, schema string) {
				// The PK constraint exists on the new table.
				PrimaryKeyConstraintMustExist(t, db, schema, "users", "pk_users")

				// Inserting a row into the table succeeds when the PK constraint is satisfied.
				MustInsert(t, db, schema, "01_create_table", "users", map[string]string{
					"id":   "1",
					"name": "alice",
				})

				// Inserting a row into the table fails when the PK constraint is not satisfied.
				MustNotInsert(t, db, schema, "01_create_table", "users", map[string]string{
					"id":   "1",
					"name": "b",
				}, testutils.UniqueViolationErrorCode)
			},
			afterRollback: func(t *testing.T, db *sql.DB, schema string) {
				// The table has been dropped, so the PK constraint is gone.
			},
			afterComplete: func(t *testing.T, db *sql.DB, schema string) {
				// The PK constraint exists on the new table.
				PrimaryKeyConstraintMustExist(t, db, schema, "users", "pk_users")

				// Inserting a row into the table succeeds when the PK constraint is satisfied.
				MustInsert(t, db, schema, "01_create_table", "users", map[string]string{
					"id":   "2",
					"name": "bobby",
				})

				// Inserting a row into the table fails when the PK constraint is not satisfied.
				MustNotInsert(t, db, schema, "01_create_table", "users", map[string]string{
					"id":   "2",
					"name": "c",
				}, testutils.UniqueViolationErrorCode)
			},
		},
		{
			name: "create table with foreign key constraint",
			migrations: []migrations.Migration{
				{
					Name: "01_create_referenced_table",
					Operations: migrations.Operations{
						&migrations.OpCreateTable{
							Name: "owners",
							Columns: []migrations.Column{
								{
									Name: "id",
									Type: "int",
								},
								{
									Name: "name",
									Type: "text",
								},
								{
									Name: "city",
									Type: "text",
								},
							},
							Constraints: []migrations.Constraint{
								{
									Name:    "pk_owners",
									Type:    migrations.ConstraintTypePrimaryKey,
									Columns: []string{"id"},
								},
							},
						},
					},
				},
				{
					Name: "02_create_referencing_table",
					Operations: migrations.Operations{
						&migrations.OpCreateTable{
							Name: "pets",
							Columns: []migrations.Column{
								{
									Name: "id",
									Type: "int",
								},
								{
									Name: "owner_id",
									Type: "int",
								},
								{
									Name: "name",
									Type: "text",
								},
							},
							Constraints: []migrations.Constraint{
								{
									Name:    "fk_owners",
									Type:    migrations.ConstraintTypeForeignKey,
									Columns: []string{"owner_id"},
									References: &migrations.TableForeignKeyReference{
										Table:    "owners",
										Columns:  []string{"id"},
										OnDelete: migrations.ForeignKeyActionCASCADE,
									},
								},
							},
						},
					},
				},
			},
			afterStart: func(t *testing.T, db *sql.DB, schema string) {
				// Table level FK exists on the new table.
				TableForeignKeyMustExist(t, db, schema, "pets", "fk_owners", false, false)

				MustInsert(t, db, schema, "01_create_referenced_table", "owners", map[string]string{
					"id":   "1",
					"name": "alice",
					"city": "new york",
				})

				// Inserting a row into the referencing table succeeds as the referenced row exists.
				MustInsert(t, db, schema, "02_create_referencing_table", "pets", map[string]string{
					"id":       "1",
					"owner_id": "1",
					"name":     "good boy",
				})

				// Inserting a row into the referencing table fails as the referenced row does not exist.
				MustNotInsert(t, db, schema, "02_create_referencing_table", "pets", map[string]string{
					"id":       "1",
					"owner_id": "0",
					"name":     "spider pig",
				}, testutils.FKViolationErrorCode)
			},
			afterRollback: func(t *testing.T, db *sql.DB, schema string) {
				// The table has been dropped, so the FK constraint is gone.
			},
			afterComplete: func(t *testing.T, db *sql.DB, schema string) {
				// Table level FK exists on the new table.
				TableForeignKeyMustExist(t, db, schema, "pets", "fk_owners", false, false)

				// Inserting a row into the table succeeds when the FK constraint is satisfied.
				MustInsert(t, db, schema, "02_create_referencing_table", "pets", map[string]string{
					"id":       "1",
					"owner_id": "1",
					"name":     "cutie pie",
				})

				// Inserting a row into the table fails when the FK constraint is not satisfied.
				MustNotInsert(t, db, schema, "02_create_referencing_table", "pets", map[string]string{
					"id":       "2",
					"owner_id": "0",
					"name":     "bobby",
				}, testutils.FKViolationErrorCode)

				// Deleting the row from the referenced table cascades to the referencing table.
				MustDelete(t, db, schema, "02_create_referencing_table", "owners", map[string]string{
					"id": "1",
				})

				rows := MustSelect(t, db, schema, "02_create_referencing_table", "pets")
				assert.Equal(t, []map[string]any{}, rows)
			},
		},
		{
			name:              "create table with multi-column foreign key constraint",
			minPgMajorVersion: 15,
			migrations: []migrations.Migration{
				{
					Name: "01_create_referenced_table",
					Operations: migrations.Operations{
						&migrations.OpCreateTable{
							Name: "owners",
							Columns: []migrations.Column{
								{
									Name: "id",
									Type: "int",
								},
								{
									Name: "name",
									Type: "text",
								},
								{
									Name: "city",
									Type: "text",
								},
							},
							Constraints: []migrations.Constraint{
								{
									Name:    "pk_owners",
									Type:    migrations.ConstraintTypePrimaryKey,
									Columns: []string{"id", "city"},
								},
							},
						},
					},
				},
				{
					Name: "02_create_referencing_table",
					Operations: migrations.Operations{
						&migrations.OpCreateTable{
							Name: "pets",
							Columns: []migrations.Column{
								{
									Name: "id",
									Type: "int",
									Pk:   true,
								},
								{
									Name:     "owner_id",
									Nullable: true,
									Type:     "int",
								},
								{
									Name:    "owner_city_id",
									Type:    "text",
									Default: ptr("'chicago'"),
								},
								{
									Name: "name",
									Type: "text",
								},
							},
							Constraints: []migrations.Constraint{
								{
									Name:       "fk_owners",
									Type:       migrations.ConstraintTypeForeignKey,
									Columns:    []string{"owner_id", "owner_city_id"},
									Deferrable: true,
									References: &migrations.TableForeignKeyReference{
										Table:              "owners",
										Columns:            []string{"id", "city"},
										OnDelete:           migrations.ForeignKeyActionSETDEFAULT,
										OnDeleteSetColumns: []string{"owner_id", "owner_city_id"},
									},
								},
							},
						},
					},
				},
			},
			afterStart: func(t *testing.T, db *sql.DB, schema string) {
				// Table level FK exists on the new table.
				TableForeignKeyMustExist(t, db, schema, "pets", "fk_owners", true, false)

				MustInsert(t, db, schema, "01_create_referenced_table", "owners", map[string]string{
					"id":   "1",
					"name": "alice",
					"city": "new york",
				})

				// Inserting a row into the referencing table succeeds as the referenced row exists.
				MustInsert(t, db, schema, "02_create_referencing_table", "pets", map[string]string{
					"id":            "1",
					"owner_id":      "1",
					"owner_city_id": "new york",
					"name":          "good boy",
				})

				// Inserting a row into the referencing table fails as the referenced row does not exist.
				MustNotInsert(t, db, schema, "02_create_referencing_table", "pets", map[string]string{
					"id":            "2",
					"owner_id":      "0",
					"owner_city_id": "sacramento",
					"name":          "spider pig",
				}, testutils.FKViolationErrorCode)
			},
			afterRollback: func(t *testing.T, db *sql.DB, schema string) {
				// The table has been dropped, so the FK constraint is gone.
			},
			afterComplete: func(t *testing.T, db *sql.DB, schema string) {
				// Table level FK exists on the new table.
				TableForeignKeyMustExist(t, db, schema, "pets", "fk_owners", true, false)

				// Inserting a row into the table succeeds when the FK constraint is satisfied.
				MustInsert(t, db, schema, "02_create_referencing_table", "pets", map[string]string{
					"id":            "1",
					"owner_id":      "1",
					"owner_city_id": "new york",
					"name":          "cutie pie",
				})

				// Inserting a row into the table fails when the FK constraint is not satisfied.
				MustNotInsert(t, db, schema, "02_create_referencing_table", "pets", map[string]string{
					"id":            "2",
					"owner_id":      "0",
					"owner_city_id": "sacramento",
					"name":          "bobby",
				}, testutils.FKViolationErrorCode)

				// Deleting the row from the referenced table cascades to the referencing table.
				MustDelete(t, db, schema, "02_create_referencing_table", "owners", map[string]string{
					"id": "1",
				})

				// deleting owner does not cascade to pets and default values are set.
				rows := MustSelect(t, db, schema, "02_create_referencing_table", "pets")
				assert.Equal(t, []map[string]any{
					{"id": 1, "owner_id": nil, "owner_city_id": "chicago", "name": "cutie pie"},
				}, rows)
			},
		},
		{
			name: "create table with a simple exclude constraint mimicking unique constraint",
			migrations: []migrations.Migration{
				{
					Name: "01_create_table",
					Operations: migrations.Operations{
						&migrations.OpCreateTable{
							Name: "users",
							Columns: []migrations.Column{
								{
									Name: "id",
									Type: "int",
								},
								{
									Name: "name",
									Type: "text",
								},
							},
							Constraints: []migrations.Constraint{
								{
									Name: "exclude_id",
									Type: migrations.ConstraintTypeExclude,
									Exclude: &migrations.ConstraintExclude{
										IndexMethod: "btree",
										Elements:    "id WITH =",
									},
								},
							},
						},
					},
				},
			},
			afterStart: func(t *testing.T, db *sql.DB, schema string) {
				// The exclusion constraint exists on the new table.
				ExcludeConstraintMustExist(t, db, schema, "users", "exclude_id")

				// Inserting a row into the table succeeds when the exclude constraint is satisfied.
				// This is the first row, so the constraint is satisfied.
				MustInsert(t, db, schema, "01_create_table", "users", map[string]string{
					"id":   "1",
					"name": "alice",
				})

				// Inserting a row into the table fails because there is already a row with id 1.
				MustNotInsert(t, db, schema, "01_create_table", "users", map[string]string{
					"id":   "1",
					"name": "b",
				}, testutils.ExclusionViolationErrorCode)
			},
			afterRollback: func(t *testing.T, db *sql.DB, schema string) {
				// The table has been dropped, so the constraint is gone.
			},
			afterComplete: func(t *testing.T, db *sql.DB, schema string) {
				// The exclusion constraint exists on the new table.
				ExcludeConstraintMustExist(t, db, schema, "users", "exclude_id")

				// Inserting a row into the table succeeds when the exclude constraint is satisfied.
				// This is the first row, so the constraint is satisfied.
				MustInsert(t, db, schema, "01_create_table", "users", map[string]string{
					"id":   "1",
					"name": "alice",
				})

				// Inserting a row into the table fails because there is already a row with id 1.
				MustNotInsert(t, db, schema, "01_create_table", "users", map[string]string{
					"id":   "1",
					"name": "b",
				}, testutils.ExclusionViolationErrorCode)
			},
		},
	})
}

func TestCreateTableValidation(t *testing.T) {
	t.Parallel()

	invalidName := strings.Repeat("x", 64)
	ExecuteTests(t, TestCases{
		{
			name: "foreign key validity",
			migrations: []migrations.Migration{
				{
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
									Name: "id",
									Type: "serial",
									Pk:   true,
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
		},
		{
			name: "invalid name",
			migrations: []migrations.Migration{
				{
					Name: "01_create_table",
					Operations: migrations.Operations{
						&migrations.OpCreateTable{
							Name: invalidName,
							Columns: []migrations.Column{
								{
									Name: "id",
									Type: "serial",
									Pk:   true,
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
			wantStartErr: migrations.InvalidIdentifierLengthError{Name: invalidName},
		},
		{
			name: "column definition missing a name",
			migrations: []migrations.Migration{
				{
					Name: "01_create_table",
					Operations: migrations.Operations{
						&migrations.OpCreateTable{
							Name: "orders",
							Columns: []migrations.Column{
								{
									Name: "id",
									Type: "serial",
									Pk:   true,
								},
								{
									// missing name
									Type: "varchar(255)",
								},
							},
						},
					},
				},
			},
			wantStartErr: migrations.ColumnIsInvalidError{Table: "orders", Name: ""},
		},
		{
			name: "column definition missing a type",
			migrations: []migrations.Migration{
				{
					Name: "01_create_table",
					Operations: migrations.Operations{
						&migrations.OpCreateTable{
							Name: "orders",
							Columns: []migrations.Column{
								{
									Name: "id",
									Type: "serial",
									Pk:   true,
								},
								{
									Name: "name",
									// missing type
								},
							},
						},
					},
				},
			},
			wantStartErr: migrations.ColumnIsInvalidError{Table: "orders", Name: "name"},
		},
		{
			name: "invalid column name",
			migrations: []migrations.Migration{
				{
					Name: "01_create_table",
					Operations: migrations.Operations{
						&migrations.OpCreateTable{
							Name: "table1",
							Columns: []migrations.Column{
								{
									Name: "id",
									Type: "serial",
									Pk:   true,
								},
								{
									Name:   invalidName,
									Type:   "varchar(255)",
									Unique: true,
								},
							},
						},
					},
				},
			},
			wantStartErr: migrations.InvalidIdentifierLengthError{Name: invalidName},
		},
		{
			name: "missing column list in unique constraint",
			migrations: []migrations.Migration{
				{
					Name: "01_create_table",
					Operations: migrations.Operations{
						&migrations.OpCreateTable{
							Name: "table1",
							Columns: []migrations.Column{
								{
									Name: "id",
									Type: "serial",
									Pk:   true,
								},
								{
									Name:   "name",
									Type:   "varchar(255)",
									Unique: true,
								},
							},
							Constraints: []migrations.Constraint{
								{
									Name: "unique_name",
									Type: migrations.ConstraintTypeUnique,
								},
							},
						},
					},
				},
			},
			wantStartErr: migrations.FieldRequiredError{Name: "columns"},
		},
		{
			name: "check constraint missing expression",
			migrations: []migrations.Migration{
				{
					Name: "01_create_table",
					Operations: migrations.Operations{
						&migrations.OpCreateTable{
							Name: "table1",
							Columns: []migrations.Column{
								{
									Name: "id",
									Type: "serial",
									Pk:   true,
								},
								{
									Name:   "name",
									Type:   "varchar(255)",
									Unique: true,
								},
							},
							Constraints: []migrations.Constraint{
								{
									Name: "check_name",
									Type: migrations.ConstraintTypeCheck,
								},
							},
						},
					},
				},
			},
			wantStartErr: migrations.FieldRequiredError{Name: "check"},
		},
		{
			name: "multiple primary key definitions",
			migrations: []migrations.Migration{
				{
					Name: "01_create_table",
					Operations: migrations.Operations{
						&migrations.OpCreateTable{
							Name: "table1",
							Columns: []migrations.Column{
								{
									Name: "id",
									Type: "serial",
									Pk:   true,
								},
								{
									Name:   "name",
									Type:   "varchar(255)",
									Unique: true,
								},
							},
							Constraints: []migrations.Constraint{
								{
									Name:    "my_pk",
									Type:    migrations.ConstraintTypePrimaryKey,
									Columns: []string{"id"},
								},
							},
						},
					},
				},
			},
			wantStartErr: migrations.PrimaryKeysAreAlreadySetError{Table: "table1"},
		},
	})
}

func TestCreateTableValidationInMultiOperationMigrations(t *testing.T) {
	t.Parallel()

	ExecuteTests(t, TestCases{
		{
			name: "create table with a name matching a name used in a previous operation",
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
									Name: "name",
									Type: "varchar(255)",
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
						&migrations.OpCreateTable{
							Name: "products",
							Columns: []migrations.Column{
								{
									Name: "id",
									Type: "serial",
									Pk:   true,
								},
							},
						},
					},
				},
			},
			wantStartErr: migrations.TableAlreadyExistsError{Name: "products"},
		},
	})
}
