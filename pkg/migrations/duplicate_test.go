package migrations

import (
	"slices"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/xataio/pgroll/pkg/schema"
)

var (
	table = &schema.Table{
		Name: "test_table",
		Columns: map[string]schema.Column{
			"id":          {Name: "id", Type: "serial"},
			"name":        {Name: "name", Type: "text"},
			"nick":        {Name: "nick", Type: "text"},
			"age":         {Name: "age", Type: "integer"},
			"email":       {Name: "email", Type: "text"},
			"city":        {Name: "city", Type: "text"},
			"description": {Name: "description", Type: "text"},
		},
		UniqueConstraints: map[string]schema.UniqueConstraint{
			"unique_email":     {Name: "unique_email", Columns: []string{"email"}},
			"unique_name_nick": {Name: "unique_name_nick", Columns: []string{"name", "nick"}},
		},
		CheckConstraints: map[string]schema.CheckConstraint{
			"email_at":        {Name: "email_at", Columns: []string{"email"}, Definition: `"email" ~ '@'`},
			"adults":          {Name: "adults", Columns: []string{"age"}, Definition: `"age" > 18`},
			"new_york_adults": {Name: "new_york_adults", Columns: []string{"city", "age"}, Definition: `"city" = 'New York' AND "age" > 21`},
			"different_nick":  {Name: "different_nick", Columns: []string{"name", "nick"}, Definition: `"name" != "nick"`},
		},
	}
	withoutConstraint = ""
)

func TestDuplicateStmtBuilderCheckConstraints(t *testing.T) {
	d := &duplicatorStmtBuilder{table}
	for name, testCases := range map[string]struct {
		columns       []string
		expectedStmts []string
	}{
		"single column duplicated with no constraint": {
			columns:       []string{"description"},
			expectedStmts: []string{},
		},
		"single-column check constraint with single column duplicated": {
			columns:       []string{"email"},
			expectedStmts: []string{`ALTER TABLE "test_table" ADD CONSTRAINT "_pgroll_dup_email_at" "_pgroll_new_email" ~ '@' NOT VALID`},
		},
		"multiple multi and single column check constraint with single column duplicated": {
			columns: []string{"age"},
			expectedStmts: []string{
				`ALTER TABLE "test_table" ADD CONSTRAINT "_pgroll_dup_adults" "_pgroll_new_age" > 18 NOT VALID`,
				`ALTER TABLE "test_table" ADD CONSTRAINT "_pgroll_dup_new_york_adults" "city" = 'New York' AND "_pgroll_new_age" > 21 NOT VALID`,
			},
		},
		"multiple multi and single column check constraint with multiple column duplicated": {
			columns: []string{"age", "description"},
			expectedStmts: []string{
				`ALTER TABLE "test_table" ADD CONSTRAINT "_pgroll_dup_adults" "_pgroll_new_age" > 18 NOT VALID`,
				`ALTER TABLE "test_table" ADD CONSTRAINT "_pgroll_dup_new_york_adults" "city" = 'New York' AND "_pgroll_new_age" > 21 NOT VALID`,
			},
		},
		"multi-column check constraint with multiple columns with single column duplicated": {
			columns:       []string{"name"},
			expectedStmts: []string{`ALTER TABLE "test_table" ADD CONSTRAINT "_pgroll_dup_different_nick" "_pgroll_new_name" != "nick" NOT VALID`},
		},
		"multi-column check constraint with multiple columns duplicated": {
			columns:       []string{"name", "nick"},
			expectedStmts: []string{`ALTER TABLE "test_table" ADD CONSTRAINT "_pgroll_dup_different_nick" "_pgroll_new_name" != "_pgroll_new_nick" NOT VALID`},
		},
	} {
		t.Run(name, func(t *testing.T) {
			stmts := d.duplicateCheckConstraints(withoutConstraint, testCases.columns...)
			assert.Equal(t, len(testCases.expectedStmts), len(stmts))
			for _, stmt := range stmts {
				assert.True(t, slices.Contains(testCases.expectedStmts, stmt))
			}
		})
	}
}

func TestDuplicateStmtBuilderUniqueConstraints(t *testing.T) {
	d := &duplicatorStmtBuilder{table}
	for name, testCases := range map[string]struct {
		columns       []string
		expectedStmts []string
	}{
		"single column duplicated": {
			columns:       []string{"city"},
			expectedStmts: []string{},
		},
		"single-column constraint with single column duplicated": {
			columns:       []string{"email"},
			expectedStmts: []string{`CREATE UNIQUE INDEX CONCURRENTLY "_pgroll_dup_unique_email" ON "test_table" ("_pgroll_new_email")`},
		},
		"single-column constraint with multiple column duplicated": {
			columns:       []string{"email", "description"},
			expectedStmts: []string{`CREATE UNIQUE INDEX CONCURRENTLY "_pgroll_dup_unique_email" ON "test_table" ("_pgroll_new_email")`},
		},
		"multi-column constraint with single column duplicated": {
			columns:       []string{"name"},
			expectedStmts: []string{`CREATE UNIQUE INDEX CONCURRENTLY "_pgroll_dup_unique_name_nick" ON "test_table" ("_pgroll_new_name", "nick")`},
		},
		"multi-column constraint with multiple unrelated column duplicated": {
			columns:       []string{"name", "description"},
			expectedStmts: []string{`CREATE UNIQUE INDEX CONCURRENTLY "_pgroll_dup_unique_name_nick" ON "test_table" ("_pgroll_new_name", "nick")`},
		},
		"multi-column constraint with multiple columns": {
			columns:       []string{"name", "nick"},
			expectedStmts: []string{`CREATE UNIQUE INDEX CONCURRENTLY "_pgroll_dup_unique_name_nick" ON "test_table" ("_pgroll_new_name", "_pgroll_new_nick")`},
		},
	} {
		t.Run(name, func(t *testing.T) {
			stmts := d.duplicateUniqueConstraints(withoutConstraint, testCases.columns...)
			assert.Equal(t, len(testCases.expectedStmts), len(stmts))
			for _, stmt := range stmts {
				assert.True(t, slices.Contains(testCases.expectedStmts, stmt))
			}
		})
	}
}
