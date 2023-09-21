package migrations_test

import (
	"database/sql"
	"testing"

	"github.com/xataio/pg-roll/pkg/migrations"
)

func TestSetColumnUnique(t *testing.T) {
	t.Parallel()

	ExecuteTests(t, TestCases{{
		name: "set unique",
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
				Name: "02_set_unique",
				Operations: migrations.Operations{
					&migrations.OpAlterColumn{
						Table:  "reviews",
						Column: "review",
						Unique: &migrations.UniqueConstraint{
							Name: "reviews_review_unique",
						},
					},
				},
			},
		},
		afterStart: func(t *testing.T, db *sql.DB) {
			// The unique index has been created on the underlying table.
			IndexMustExist(t, db, "public", "reviews", "reviews_review_unique")

			// Inserting values into the old schema that violate uniqueness should fail.
			MustInsert(t, db, "public", "01_add_table", "reviews", map[string]string{
				"username": "alice", "product": "apple", "review": "good",
			})
			MustNotInsert(t, db, "public", "01_add_table", "reviews", map[string]string{
				"username": "bob", "product": "banana", "review": "good",
			})

			// Inserting values into the new schema that violate uniqueness should fail.
			MustInsert(t, db, "public", "02_set_unique", "reviews", map[string]string{
				"username": "carl", "product": "carrot", "review": "bad",
			})
			MustNotInsert(t, db, "public", "02_set_unique", "reviews", map[string]string{
				"username": "dana", "product": "durian", "review": "bad",
			})
		},
		afterRollback: func(t *testing.T, db *sql.DB) {
			// The unique index has been dropped from the the underlying table.
			IndexMustNotExist(t, db, "public", "reviews", "reviews_review_unique")
		},
		afterComplete: func(t *testing.T, db *sql.DB) {
			// The unique constraint has been created on the underlying table.
			ConstraintMustExist(t, db, "public", "reviews", "reviews_review_unique")

			// Inserting values into the new schema that violate uniqueness should fail.
			MustInsert(t, db, "public", "02_set_unique", "reviews", map[string]string{
				"username": "earl", "product": "elderberry", "review": "ok",
			})
			MustNotInsert(t, db, "public", "02_set_unique", "reviews", map[string]string{
				"username": "flora", "product": "fig", "review": "ok",
			})
		},
	}})
}
