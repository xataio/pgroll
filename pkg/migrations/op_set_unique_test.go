// SPDX-License-Identifier: Apache-2.0

package migrations_test

import (
	"database/sql"
	"testing"

	"github.com/stretchr/testify/assert"
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
			afterStart: func(t *testing.T, db *sql.DB) {
				// Inserting values into the old schema that violate uniqueness should succeed.
				MustInsert(t, db, "public", "01_add_table", "reviews", map[string]string{
					"username": "alice", "product": "apple", "review": "good",
				})
				MustInsert(t, db, "public", "01_add_table", "reviews", map[string]string{
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

				// The up function no longer exists.
				FunctionMustNotExist(t, db, "public", migrations.TriggerFunctionName("reviews", "review"))
				// The down function no longer exists.
				FunctionMustNotExist(t, db, "public", migrations.TriggerFunctionName("reviews", migrations.TemporaryName("review")))

				// The up trigger no longer exists.
				TriggerMustNotExist(t, db, "public", "reviews", migrations.TriggerName("reviews", "review"))
				// The down trigger no longer exists.
				TriggerMustNotExist(t, db, "public", "reviews", migrations.TriggerName("reviews", migrations.TemporaryName("review")))

				// Inserting values into the new schema that violate uniqueness should fail.
				MustInsert(t, db, "public", "02_set_unique", "reviews", map[string]string{
					"username": "earl", "product": "elderberry", "review": "ok",
				})
				MustNotInsert(t, db, "public", "02_set_unique", "reviews", map[string]string{
					"username": "flora", "product": "fig", "review": "ok",
				})
			},
		},
		{
			name: "set unique with default user supplied down sql",
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
			afterStart: func(t *testing.T, db *sql.DB) {
				// Inserting values into the new schema backfills the old column using the `down` SQL.
				MustInsert(t, db, "public", "02_set_unique", "reviews", map[string]string{
					"username": "carl", "product": "carrot", "review": "bad",
				})

				rows := MustSelect(t, db, "public", "01_add_table", "reviews")
				assert.Equal(t, []map[string]any{
					{"id": 1, "username": "carl", "product": "carrot", "review": "bad!"},
				}, rows)
			},
			afterRollback: func(t *testing.T, db *sql.DB) {
			},
			afterComplete: func(t *testing.T, db *sql.DB) {
			},
		},
	})
}
