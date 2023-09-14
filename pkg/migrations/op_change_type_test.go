package migrations_test

import (
	"database/sql"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/xataio/pg-roll/pkg/migrations"
	"github.com/xataio/pg-roll/pkg/roll"
)

func TestChangeColumnType(t *testing.T) {
	t.Parallel()

	ExecuteTests(t, TestCases{{
		name: "change column type",
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
								Name: "username",
								Type: "text",
							},
							{
								Name: "product",
								Type: "text",
							},
							{
								Name:    "rating",
								Type:    "text",
								Default: ptr("0"),
							},
						},
					},
				},
			},
			{
				Name: "02_change_type",
				Operations: migrations.Operations{
					&migrations.OpAlterColumn{
						Table:  "reviews",
						Column: "rating",
						Type:   "integer",
						Up:     "CAST (rating AS integer)",
						Down:   "CAST (rating AS text)",
					},
				},
			},
		},
		afterStart: func(t *testing.T, db *sql.DB) {
			newVersionSchema := roll.VersionedSchemaName("public", "02_change_type")

			// The new (temporary) `rating` column should exist on the underlying table.
			ColumnMustExist(t, db, "public", "reviews", migrations.TemporaryName("rating"))

			// The `rating` column in the new view must have the correct type.
			ColumnMustHaveType(t, db, newVersionSchema, "reviews", "rating", "integer")

			// Inserting into the new `rating` column should work.
			MustInsert(t, db, "public", "02_change_type", "reviews", map[string]string{
				"username": "alice",
				"product":  "apple",
				"rating":   "5",
			})

			// The value inserted into the new `rating` column has been backfilled into
			// the old `rating` column.
			rows := MustSelect(t, db, "public", "01_add_table", "reviews")
			assert.Equal(t, []map[string]any{
				{"id": 1, "username": "alice", "product": "apple", "rating": "5"},
			}, rows)

			// Inserting into the old `rating` column should work.
			MustInsert(t, db, "public", "01_add_table", "reviews", map[string]string{
				"username": "bob",
				"product":  "banana",
				"rating":   "8",
			})

			// The value inserted into the old `rating` column has been backfilled into
			// the new `rating` column.
			rows = MustSelect(t, db, "public", "02_change_type", "reviews")
			assert.Equal(t, []map[string]any{
				{"id": 1, "username": "alice", "product": "apple", "rating": 5},
				{"id": 2, "username": "bob", "product": "banana", "rating": 8},
			}, rows)
		},
		afterRollback: func(t *testing.T, db *sql.DB) {
			// The new (temporary) `rating` column should not exist on the underlying table.
			ColumnMustNotExist(t, db, "public", "reviews", migrations.TemporaryName("rating"))

			// The up function no longer exists.
			FunctionMustNotExist(t, db, "public", migrations.TriggerFunctionName("reviews", "rating"))
			// The down function no longer exists.
			FunctionMustNotExist(t, db, "public", migrations.TriggerFunctionName("reviews", migrations.TemporaryName("rating")))

			// The up trigger no longer exists.
			TriggerMustNotExist(t, db, "public", "reviews", migrations.TriggerName("reviews", "rating"))
			// The down trigger no longer exists.
			TriggerMustNotExist(t, db, "public", "reviews", migrations.TriggerName("reviews", migrations.TemporaryName("rating")))
		},
		afterComplete: func(t *testing.T, db *sql.DB) {
			newVersionSchema := roll.VersionedSchemaName("public", "02_change_type")

			// The new (temporary) `rating` column should not exist on the underlying table.
			ColumnMustNotExist(t, db, "public", "reviews", migrations.TemporaryName("rating"))

			// The `rating` column in the new view must have the correct type.
			ColumnMustHaveType(t, db, newVersionSchema, "reviews", "rating", "integer")

			// Inserting into the new view should work.
			MustInsert(t, db, "public", "02_change_type", "reviews", map[string]string{
				"username": "carl",
				"product":  "carrot",
				"rating":   "3",
			})

			// Selecting from the new view should succeed.
			rows := MustSelect(t, db, "public", "02_change_type", "reviews")
			assert.Equal(t, []map[string]any{
				{"id": 1, "username": "alice", "product": "apple", "rating": 5},
				{"id": 2, "username": "bob", "product": "banana", "rating": 8},
				{"id": 3, "username": "carl", "product": "carrot", "rating": 3},
			}, rows)

			// The up function no longer exists.
			FunctionMustNotExist(t, db, "public", migrations.TriggerFunctionName("reviews", "rating"))
			// The down function no longer exists.
			FunctionMustNotExist(t, db, "public", migrations.TriggerFunctionName("reviews", migrations.TemporaryName("rating")))

			// The up trigger no longer exists.
			TriggerMustNotExist(t, db, "public", "reviews", migrations.TriggerName("reviews", "rating"))
			// The down trigger no longer exists.
			TriggerMustNotExist(t, db, "public", "reviews", migrations.TriggerName("reviews", migrations.TemporaryName("rating")))
		},
	}})
}

func TestChangeColumnTypeValidation(t *testing.T) {
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
						Name: "username",
						Type: "text",
					},
					{
						Name: "product",
						Type: "text",
					},
					{
						Name: "rating",
						Type: "text",
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
					Name: "02_change_type",
					Operations: migrations.Operations{
						&migrations.OpAlterColumn{
							Table:  "reviews",
							Column: "rating",
							Type:   "integer",
							Down:   "CAST (rating AS text)",
						},
					},
				},
			},
			wantStartErr: migrations.FieldRequiredError{Name: "up"},
		},
		{
			name: "down SQL is mandatory",
			migrations: []migrations.Migration{
				createTableMigration,
				{
					Name: "02_change_type",
					Operations: migrations.Operations{
						&migrations.OpAlterColumn{
							Table:  "reviews",
							Column: "rating",
							Type:   "integer",
							Up:     "CAST (rating AS integer)",
						},
					},
				},
			},
			wantStartErr: migrations.FieldRequiredError{Name: "down"},
		},
	})
}
