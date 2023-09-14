package migrations_test

import (
	"database/sql"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/xataio/pg-roll/pkg/migrations"
)

func TestSetCheckConstraint(t *testing.T) {
	t.Parallel()

	ExecuteTests(t, TestCases{{
		name: "add check constraint",
		migrations: []migrations.Migration{
			{
				Name: "01_add_table",
				Operations: migrations.Operations{
					&migrations.OpCreateTable{
						Name: "posts",
						Columns: []migrations.Column{
							{
								Name:       "id",
								Type:       "serial",
								PrimaryKey: true,
							},
							{
								Name: "title",
								Type: "text",
							},
						},
					},
				},
			},
			{
				Name: "02_add_check_constraint",
				Operations: migrations.Operations{
					&migrations.OpAlterColumn{
						Table:  "posts",
						Column: "title",
						Check:  "length(title) > 3",
						Up:     "(SELECT CASE WHEN length(title) <= 3 THEN LPAD(title, 4, '-') ELSE title END)",
						Down:   "title",
					},
				},
			},
		},
		afterStart: func(t *testing.T, db *sql.DB) {
			// The new (temporary) `title` column should exist on the underlying table.
			ColumnMustExist(t, db, "public", "posts", migrations.TemporaryName("title"))

			// Inserting a row that meets the check constraint into the old view works.
			MustInsert(t, db, "public", "01_add_table", "posts", map[string]string{
				"title": "post by alice",
			})

			// Inserting a row that does not meet the check constraint into the old view also works.
			MustInsert(t, db, "public", "01_add_table", "posts", map[string]string{
				"title": "b",
			})

			// Both rows have been backfilled into the new view; the short title has
			// been rewritten using `up` SQL to meet the length constraint.
			rows := MustSelect(t, db, "public", "02_add_check_constraint", "posts")
			assert.Equal(t, []map[string]any{
				{"id": 1, "title": "post by alice"},
				{"id": 2, "title": "---b"},
			}, rows)

			// Inserting a row that meets the check constraint into the new view works.
			MustInsert(t, db, "public", "02_add_check_constraint", "posts", map[string]string{
				"title": "post by carl",
			})

			// Inserting a row that does not meet the check constraint into the new view fails.
			MustNotInsert(t, db, "public", "02_add_check_constraint", "posts", map[string]string{
				"title": "d",
			})

			// The row that was inserted into the new view has been backfilled into the old view.
			rows = MustSelect(t, db, "public", "01_add_table", "posts")
			assert.Equal(t, []map[string]any{
				{"id": 1, "title": "post by alice"},
				{"id": 2, "title": "b"},
				{"id": 3, "title": "post by carl"},
			}, rows)
		},
		afterRollback: func(t *testing.T, db *sql.DB) {
			// The new (temporary) `title` column should not exist on the underlying table.
			ColumnMustNotExist(t, db, "public", "posts", migrations.TemporaryName("title"))

			// The up function no longer exists.
			FunctionMustNotExist(t, db, "public", migrations.TriggerFunctionName("posts", "title"))
			// The down function no longer exists.
			FunctionMustNotExist(t, db, "public", migrations.TriggerFunctionName("posts", migrations.TemporaryName("title")))

			// The up trigger no longer exists.
			TriggerMustNotExist(t, db, "public", "posts", migrations.TriggerName("posts", "title"))
			// The down trigger no longer exists.
			TriggerMustNotExist(t, db, "public", "posts", migrations.TriggerName("posts", migrations.TemporaryName("title")))
		},
		afterComplete: func(t *testing.T, db *sql.DB) {
			// Inserting a row that meets the check constraint into the new view works.
			MustInsert(t, db, "public", "02_add_check_constraint", "posts", map[string]string{
				"title": "post by dana",
			})

			// Inserting a row that does not meet the check constraint into the new view fails.
			MustNotInsert(t, db, "public", "02_add_check_constraint", "posts", map[string]string{
				"title": "e",
			})

			// The data in the new `posts` view is as expected.
			rows := MustSelect(t, db, "public", "02_add_check_constraint", "posts")
			assert.Equal(t, []map[string]any{
				{"id": 1, "title": "post by alice"},
				{"id": 2, "title": "---b"},
				{"id": 3, "title": "post by carl"},
				{"id": 5, "title": "post by dana"},
			}, rows)

			// The up function no longer exists.
			FunctionMustNotExist(t, db, "public", migrations.TriggerFunctionName("posts", "title"))
			// The down function no longer exists.
			FunctionMustNotExist(t, db, "public", migrations.TriggerFunctionName("posts", migrations.TemporaryName("title")))

			// The up trigger no longer exists.
			TriggerMustNotExist(t, db, "public", "posts", migrations.TriggerName("posts", "title"))
			// The down trigger no longer exists.
			TriggerMustNotExist(t, db, "public", "posts", migrations.TriggerName("posts", migrations.TemporaryName("title")))
		},
	}})
}

func TestSetCheckConstraintValidation(t *testing.T) {
	t.Parallel()

	createTableMigration := migrations.Migration{
		Name: "01_add_table",
		Operations: migrations.Operations{
			&migrations.OpCreateTable{
				Name: "posts",
				Columns: []migrations.Column{
					{
						Name:       "id",
						Type:       "serial",
						PrimaryKey: true,
					},
					{
						Name: "title",
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
					Name: "02_add_check_constraint",
					Operations: migrations.Operations{
						&migrations.OpAlterColumn{
							Table:  "posts",
							Column: "title",
							Check:  "length(title) > 3",
							Down:   "title",
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
					Name: "02_add_check_constraint",
					Operations: migrations.Operations{
						&migrations.OpAlterColumn{
							Table:  "posts",
							Column: "title",
							Check:  "length(title) > 3",
							Up:     "(SELECT CASE WHEN length(title) <= 3 THEN LPAD(title, 4, '-') ELSE title END)",
						},
					},
				},
			},
			wantStartErr: migrations.FieldRequiredError{Name: "down"},
		},
	})
}
