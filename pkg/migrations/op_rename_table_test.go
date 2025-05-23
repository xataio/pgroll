// SPDX-License-Identifier: Apache-2.0

package migrations_test

import (
	"database/sql"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/xataio/pgroll/pkg/migrations"
)

func TestRenameTable(t *testing.T) {
	t.Parallel()

	invalidName := strings.Repeat("x", 64)
	ExecuteTests(t, TestCases{
		{
			name: "rename table",
			migrations: []migrations.Migration{
				{
					Name: "01_create_table",
					Operations: migrations.Operations{
						&migrations.OpCreateTable{
							Name: "test_table",
							Columns: []migrations.Column{
								{
									Name: "id",
									Type: "serial",
								},
								{
									Name: "name",
									Type: "text",
								},
							},
						},
					},
				},
				{
					Name: "02_rename_table",
					Operations: migrations.Operations{
						&migrations.OpRenameTable{
							From: "test_table",
							To:   "renamed_table",
						},
					},
				},
			},
			afterStart: func(t *testing.T, db *sql.DB, schema string) {
				// check that the table with the new name can be accessed
				ViewMustExist(t, db, schema, "01_create_table", "test_table")
				ViewMustExist(t, db, schema, "02_rename_table", "renamed_table")

				// inserts work
				MustInsert(t, db, schema, "01_create_table", "test_table", map[string]string{
					"name": "foo",
				})
				MustInsert(t, db, schema, "02_rename_table", "renamed_table", map[string]string{
					"name": "bar",
				})

				// selects work in both versions
				resNew := MustSelect(t, db, schema, "01_create_table", "test_table")
				resOld := MustSelect(t, db, schema, "02_rename_table", "renamed_table")

				assert.Equal(t, resOld, resNew)
				assert.Equal(t, []map[string]any{
					{
						"id":   1,
						"name": "foo",
					},
					{
						"id":   2,
						"name": "bar",
					},
				}, resNew)
			},
			afterComplete: func(t *testing.T, db *sql.DB, schema string) {
				// the table still exists with the new name
				ViewMustNotExist(t, db, schema, "02_rename_table", "testTable")
				ViewMustExist(t, db, schema, "02_rename_table", "renamed_table")

				// inserts & select work
				MustInsert(t, db, schema, "02_rename_table", "renamed_table", map[string]string{
					"name": "baz",
				})
				res := MustSelect(t, db, schema, "02_rename_table", "renamed_table")
				assert.Equal(t, []map[string]any{
					{
						"id":   1,
						"name": "foo",
					},
					{
						"id":   2,
						"name": "bar",
					},
					{
						"id":   3,
						"name": "baz",
					},
				}, res)
			},
		},
		{
			name: "rename table validation: already exists",
			migrations: []migrations.Migration{
				{
					Name: "01_create_table",
					Operations: migrations.Operations{
						&migrations.OpCreateTable{
							Name:    "test_table",
							Columns: []migrations.Column{},
						},
						&migrations.OpCreateTable{
							Name:    "other_table",
							Columns: []migrations.Column{},
						},
					},
				},
				{
					Name: "02_rename_table",
					Operations: migrations.Operations{
						&migrations.OpRenameTable{
							From: "test_table",
							To:   "other_table",
						},
					},
				},
			},
			wantValidateErr: migrations.TableAlreadyExistsError{Name: "other_table"},
		},
		{
			name: "rename table validation: invalid name",
			migrations: []migrations.Migration{
				{
					Name: "01_create_table",
					Operations: migrations.Operations{
						&migrations.OpCreateTable{
							Name:    "test_table",
							Columns: []migrations.Column{},
						},
						&migrations.OpCreateTable{
							Name:    "other_table",
							Columns: []migrations.Column{},
						},
					},
				},
				{
					Name: "02_rename_table",
					Operations: migrations.Operations{
						&migrations.OpRenameTable{
							From: "test_table",
							To:   invalidName,
						},
					},
				},
			},
			wantValidateErr: migrations.InvalidIdentifierLengthError{Name: invalidName},
		},
	})
}
