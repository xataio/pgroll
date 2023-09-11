package migrations_test

import (
	"database/sql"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/xataio/pg-roll/pkg/migrations"
)

func TestSetNotNull(t *testing.T) {
	t.Parallel()

	ExecuteTests(t, TestCases{
		{
			name: "set not null with default down sql",
			migrations: []migrations.Migration{
				{
					Name: "01_add_table",
					Operations: migrations.Operations{
						&migrations.OpCreateTable{
							Name: "reviews",
							Columns: []migrations.Column{
								{
									Name:       "id",
									Type:       "serial",
									PrimaryKey: true,
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
									Nullable: true,
								},
							},
						},
					},
				},
				{
					Name: "02_set_not_null",
					Operations: migrations.Operations{
						&migrations.OpSetNotNull{
							Table:  "reviews",
							Column: "review",
							Up:     "(SELECT CASE WHEN review IS NULL THEN product || ' is good' ELSE review END)",
						},
					},
				},
			},
			afterStart: func(t *testing.T, db *sql.DB) {
				// The new (temporary) `review` column should exist on the underlying table.
				ColumnMustExist(t, db, "public", "reviews", migrations.TemporaryName("review"))

				// Inserting a NULL into the new `review` column should fail
				MustNotInsert(t, db, "public", "02_set_not_null", "reviews", map[string]string{
					"username": "alice",
					"product":  "apple",
				})

				// Inserting a non-NULL value into the new `review` column should succeed
				MustInsert(t, db, "public", "02_set_not_null", "reviews", map[string]string{
					"username": "alice",
					"product":  "apple",
					"review":   "amazing",
				})

				// The value inserted into the new `review` column has been backfilled into the
				// old `review` column.
				rows := MustSelect(t, db, "public", "01_add_table", "reviews")
				assert.Equal(t, []map[string]any{
					{"id": 2, "username": "alice", "product": "apple", "review": "amazing"},
				}, rows)

				// Inserting a NULL value into the old `review` column should succeed
				MustInsert(t, db, "public", "01_add_table", "reviews", map[string]string{
					"username": "bob",
					"product":  "banana",
				})

				// The NULL value inserted into the old `review` column has been written into
				// the new `review` column using the `up` SQL.
				rows = MustSelect(t, db, "public", "02_set_not_null", "reviews")
				assert.Equal(t, []map[string]any{
					{"id": 2, "username": "alice", "product": "apple", "review": "amazing"},
					{"id": 3, "username": "bob", "product": "banana", "review": "banana is good"},
				}, rows)

				// Inserting a non-NULL value into the old `review` column should succeed
				MustInsert(t, db, "public", "01_add_table", "reviews", map[string]string{
					"username": "carl",
					"product":  "carrot",
					"review":   "crunchy",
				})

				// The non-NULL value inserted into the old `review` column has been copied
				// unchanged into the new `review` column.
				rows = MustSelect(t, db, "public", "02_set_not_null", "reviews")
				assert.Equal(t, []map[string]any{
					{"id": 2, "username": "alice", "product": "apple", "review": "amazing"},
					{"id": 3, "username": "bob", "product": "banana", "review": "banana is good"},
					{"id": 4, "username": "carl", "product": "carrot", "review": "crunchy"},
				}, rows)
			},
			afterRollback: func(t *testing.T, db *sql.DB) {
				// The new (temporary) `review` column should not exist on the underlying table.
				ColumnMustNotExist(t, db, "public", "reviews", migrations.TemporaryName("review"))

				// The up function no longer exists.
				FunctionMustNotExist(t, db, "public", migrations.TriggerFunctionName("reviews", "review"))
				// The down function no longer exists.
				FunctionMustNotExist(t, db, "public", migrations.TriggerFunctionName("reviews", migrations.TemporaryName("review")))

				// The up trigger no longer exists.
				TriggerMustNotExist(t, db, "public", "reviews", migrations.TriggerName("reviews", "review"))
				// The down trigger no longer exists.
				TriggerMustNotExist(t, db, "public", "reviews", migrations.TriggerName("reviews", migrations.TemporaryName("review")))
			},
			afterComplete: func(t *testing.T, db *sql.DB) {
				// The new (temporary) `review` column should not exist on the underlying table.
				ColumnMustNotExist(t, db, "public", "reviews", migrations.TemporaryName("review"))

				// Selecting from the `reviews` view should succeed.
				rows := MustSelect(t, db, "public", "02_set_not_null", "reviews")
				assert.Equal(t, []map[string]any{
					{"id": 2, "username": "alice", "product": "apple", "review": "amazing"},
					{"id": 3, "username": "bob", "product": "banana", "review": "banana is good"},
					{"id": 4, "username": "carl", "product": "carrot", "review": "crunchy"},
				}, rows)

				// Writing NULL reviews into the `review` column should fail.
				MustNotInsert(t, db, "public", "02_set_not_null", "reviews", map[string]string{
					"username": "daisy",
					"product":  "durian",
				})

				// The up function no longer exists.
				FunctionMustNotExist(t, db, "public", migrations.TriggerFunctionName("reviews", "review"))
				// The down function no longer exists.
				FunctionMustNotExist(t, db, "public", migrations.TriggerFunctionName("reviews", migrations.TemporaryName("review")))

				// The up trigger no longer exists.
				TriggerMustNotExist(t, db, "public", "reviews", migrations.TriggerName("reviews", "review"))
				// The down trigger no longer exists.
				TriggerMustNotExist(t, db, "public", "reviews", migrations.TriggerName("reviews", migrations.TemporaryName("review")))
			},
		},
		{
			name: "set not null with user-supplied down sql",
			migrations: []migrations.Migration{
				{
					Name: "01_add_table",
					Operations: migrations.Operations{
						&migrations.OpCreateTable{
							Name: "reviews",
							Columns: []migrations.Column{
								{
									Name:       "id",
									Type:       "serial",
									PrimaryKey: true,
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
									Nullable: true,
								},
							},
						},
					},
				},
				{
					Name: "02_set_not_null",
					Operations: migrations.Operations{
						&migrations.OpSetNotNull{
							Table:  "reviews",
							Column: "review",
							Up:     "(SELECT CASE WHEN review IS NULL THEN product || ' is good' ELSE review END)",
							Down:   "review || ' (from new column)'",
						},
					},
				},
			},
			afterStart: func(t *testing.T, db *sql.DB) {
				// Inserting a non-NULL value into the new `review` column should succeed
				MustInsert(t, db, "public", "02_set_not_null", "reviews", map[string]string{
					"username": "alice",
					"product":  "apple",
					"review":   "amazing",
				})

				// The value inserted into the new `review` column has been backfilled into the
				// old `review` column using the user-supplied `down` SQL.
				rows := MustSelect(t, db, "public", "01_add_table", "reviews")
				assert.Equal(t, []map[string]any{
					{"id": 1, "username": "alice", "product": "apple", "review": "amazing (from new column)"},
				}, rows)
			},
			afterRollback: func(t *testing.T, db *sql.DB) {
			},
			afterComplete: func(t *testing.T, db *sql.DB) {
			},
		},
	})
}

func TestSetNotNullValidation(t *testing.T) {
	t.Parallel()

	createTableMigration := migrations.Migration{
		Name: "01_add_table",
		Operations: migrations.Operations{
			&migrations.OpCreateTable{
				Name: "reviews",
				Columns: []migrations.Column{
					{
						Name:       "id",
						Type:       "serial",
						PrimaryKey: true,
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
						Nullable: true,
					},
				},
			},
		},
	}

	ExecuteTests(t, TestCases{
		{
			name: "up SQL is mandatory",
			migrations: []migrations.Migration{
				createTableMigration,
				{
					Name: "02_set_not_null",
					Operations: migrations.Operations{
						&migrations.OpSetNotNull{
							Table:  "reviews",
							Column: "review",
							Down:   "review",
						},
					},
				},
			},
			wantStartErr: migrations.FieldRequiredError{Name: "up"},
		},
		{
			name: "table must exist",
			migrations: []migrations.Migration{
				createTableMigration,
				{
					Name: "02_set_not_null",
					Operations: migrations.Operations{
						&migrations.OpSetNotNull{
							Table:  "doesntexist",
							Column: "review",
							Up:     "(SELECT CASE WHEN review IS NULL THEN product || ' is good' ELSE review END)",
							Down:   "review",
						},
					},
				},
			},
			wantStartErr: migrations.TableDoesNotExistError{Name: "doesntexist"},
		},
		{
			name: "column must exist",
			migrations: []migrations.Migration{
				createTableMigration,
				{
					Name: "02_set_not_null",
					Operations: migrations.Operations{
						&migrations.OpSetNotNull{
							Table:  "reviews",
							Column: "doesntexist",
							Up:     "(SELECT CASE WHEN review IS NULL THEN product || ' is good' ELSE review END)",
							Down:   "review",
						},
					},
				},
			},
			wantStartErr: migrations.ColumnDoesNotExistError{Table: "reviews", Name: "doesntexist"},
		},
		{
			name: "column is nullable",
			migrations: []migrations.Migration{
				{
					Name: "01_add_table",
					Operations: migrations.Operations{
						&migrations.OpCreateTable{
							Name: "reviews",
							Columns: []migrations.Column{
								{
									Name:       "id",
									Type:       "serial",
									PrimaryKey: true,
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
					Name: "02_set_not_null",
					Operations: migrations.Operations{
						&migrations.OpSetNotNull{
							Table:  "reviews",
							Column: "review",
							Up:     "(SELECT CASE WHEN review IS NULL THEN product || ' is good' ELSE review END)",
							Down:   "review",
						},
					},
				},
			},
			wantStartErr: migrations.ColumnIsNotNullableError{Table: "reviews", Name: "review"},
		},
	})
}
