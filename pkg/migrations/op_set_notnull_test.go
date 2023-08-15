package migrations_test

import (
	"database/sql"
	"testing"

	"github.com/xataio/pg-roll/pkg/migrations"
)

func TestSetNotNull(t *testing.T) {
	t.Parallel()

	ptr := func(s string) *string { return &s }

	ExecuteTests(t, TestCases{{
		name: "set not null",
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
						Up:     ptr("product || ' is good'"),
					},
				},
			},
		},
		afterStart: func(t *testing.T, db *sql.DB) {
			// Inserting a null review through the old view works (due to `up` sql populating the column).
			MustInsert(t, db, "public", "01_add_table", "reviews", map[string]string{
				"username": "alice",
				"product":  "apple",
			})
			// Inserting a null description through the new view fails.
			MustNotInsert(t, db, "public", "02_set_not_null", "reviews", map[string]string{
				"username": "bob",
				"product":  "banana",
			})
		},
		afterRollback: func(t *testing.T, db *sql.DB) {
			// The trigger has been dropped.
			triggerName := migrations.TriggerName("reviews", "review")
			TriggerMustNotExist(t, db, "public", "reviews", triggerName)

			// the not null constraint has been dropped
			// TODO: check the constraint name here is the correct one.
			constraintName := migrations.NotNullConstraintName("review")
			ConstraintMustNotExist(t, db, "public", "reviews", constraintName)
		},
		afterComplete: func(t *testing.T, db *sql.DB) {
			// The trigger has been dropped.
			triggerName := migrations.TriggerName("reviews", "review")
			TriggerMustNotExist(t, db, "public", "reviews", triggerName)

			// the not null constraint has been dropped
			// TODO: check the constraint name here is the correct one.
			constraintName := migrations.NotNullConstraintName("review")
			ConstraintMustNotExist(t, db, "public", "reviews", constraintName)

			// Inserting a null description through the new view fails.
			MustNotInsert(t, db, "public", "02_set_not_null", "reviews", map[string]string{
				"username": "carl",
				"product":  "carrot",
			})
		},
	}})
}
