// SPDX-License-Identifier: Apache-2.0

package migrations

import (
	"slices"
	"testing"

	"github.com/stretchr/testify/assert"
	pgq "github.com/xataio/pg_query_go/v6"

	"github.com/xataio/pgroll/pkg/schema"
)

var table = &schema.Table{
	Name: "test_table",
	Columns: map[string]*schema.Column{
		"id":          {Name: "id", Type: "serial"},
		"name":        {Name: "name", Type: "text"},
		"nick":        {Name: "nick", Type: "text"},
		"age":         {Name: "age", Type: "integer"},
		"email":       {Name: "email", Type: "text"},
		"city":        {Name: "city", Type: "text"},
		"description": {Name: "description", Type: "text"},
	},
	UniqueConstraints: map[string]*schema.UniqueConstraint{
		"unique_email":     {Name: "unique_email", Columns: []string{"email"}},
		"unique_name_nick": {Name: "unique_name_nick", Columns: []string{"name", "nick"}},
	},
	CheckConstraints: map[string]*schema.CheckConstraint{
		"email_at":        {Name: "email_at", Columns: []string{"email"}, Definition: `"email" ~ '@'`, NoInherit: true},
		"adults":          {Name: "adults", Columns: []string{"age"}, Definition: `"age" > 18`},
		"new_york_adults": {Name: "new_york_adults", Columns: []string{"city", "age"}, Definition: `"city" = 'New York' AND "age" > 21`},
		"different_nick":  {Name: "different_nick", Columns: []string{"name", "nick"}, Definition: `"name" != "nick"`},
	},
	ForeignKeys: map[string]*schema.ForeignKey{
		"fk_city":      {Name: "fk_city", Columns: []string{"city"}, ReferencedTable: "cities", ReferencedColumns: []string{"id"}, OnDelete: "NO ACTION"},
		"fk_name_nick": {Name: "fk_name_nick", Columns: []string{"name", "nick"}, ReferencedTable: "users", ReferencedColumns: []string{"name", "nick"}, OnDelete: "CASCADE", OnUpdate: "CASCADE", MatchType: "FULL"},
	},
	Indexes: map[string]*schema.Index{
		"idx_no_kate": {
			Name:       "idx_no_kate",
			Columns:    []string{"name"},
			Definition: `CREATE INDEX "idx_name" ON "test_table" USING hash ("name") WITH (fillfactor = 70) WHERE "name" != 'Kate'`,
			Predicate:  ptr("name != 'Kate'"),
			Method:     "hash",
		},
		"idx_email": {
			Name:    "idx_email",
			Columns: []string{"email"},
		},
		"idx_name_city": {
			Name:    "idx_name_city",
			Columns: []string{"name", "city"},
		},
	},
}

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
			expectedStmts: []string{`ALTER TABLE "test_table" ADD CONSTRAINT "_pgroll_dup_email_at" CHECK ("_pgroll_new_email" ~ '@') NO INHERIT NOT VALID`},
		},
		"multiple multi and single column check constraint with single column duplicated": {
			columns: []string{"age"},
			expectedStmts: []string{
				`ALTER TABLE "test_table" ADD CONSTRAINT "_pgroll_dup_adults" CHECK ("_pgroll_new_age" > 18) NOT VALID`,
				`ALTER TABLE "test_table" ADD CONSTRAINT "_pgroll_dup_new_york_adults" CHECK ("city" = 'New York' AND "_pgroll_new_age" > 21) NOT VALID`,
			},
		},
		"multiple multi and single column check constraint with multiple column duplicated": {
			columns: []string{"age", "description"},
			expectedStmts: []string{
				`ALTER TABLE "test_table" ADD CONSTRAINT "_pgroll_dup_adults" CHECK ("_pgroll_new_age" > 18) NOT VALID`,
				`ALTER TABLE "test_table" ADD CONSTRAINT "_pgroll_dup_new_york_adults" CHECK ("city" = 'New York' AND "_pgroll_new_age" > 21) NOT VALID`,
			},
		},
		"multi-column check constraint with multiple columns with single column duplicated": {
			columns:       []string{"name"},
			expectedStmts: []string{`ALTER TABLE "test_table" ADD CONSTRAINT "_pgroll_dup_different_nick" CHECK ("_pgroll_new_name" != "nick") NOT VALID`},
		},
		"multi-column check constraint with multiple columns duplicated": {
			columns:       []string{"name", "nick"},
			expectedStmts: []string{`ALTER TABLE "test_table" ADD CONSTRAINT "_pgroll_dup_different_nick" CHECK ("_pgroll_new_name" != "_pgroll_new_nick") NOT VALID`},
		},
	} {
		t.Run(name, func(t *testing.T) {
			stmts := d.duplicateCheckConstraints(nil, testCases.columns...)
			assert.Equal(t, len(testCases.expectedStmts), len(stmts))
			for _, stmt := range stmts {
				_, err := pgq.Parse(stmt)
				assert.NoError(t, err)
				assert.True(t, slices.Contains(testCases.expectedStmts, stmt))
			}
		})
	}
}

func TestDuplicateStmtBuilderForeignKeyConstraints(t *testing.T) {
	d := &duplicatorStmtBuilder{table}
	for name, testCases := range map[string]struct {
		columns       []string
		expectedStmts []string
	}{
		"duplicate single column with no FK constraint": {
			columns:       []string{"description"},
			expectedStmts: []string{},
		},
		"single-column FK with single column duplicated": {
			columns: []string{"city"},
			expectedStmts: []string{
				`ALTER TABLE "test_table" ADD CONSTRAINT "_pgroll_dup_fk_city" FOREIGN KEY ("_pgroll_new_city") REFERENCES "cities" ("id") MATCH SIMPLE ON DELETE NO ACTION ON UPDATE NO ACTION`,
			},
		},
		"single-column FK with multiple columns duplicated": {
			columns: []string{"city", "description"},
			expectedStmts: []string{
				`ALTER TABLE "test_table" ADD CONSTRAINT "_pgroll_dup_fk_city" FOREIGN KEY ("_pgroll_new_city") REFERENCES "cities" ("id") MATCH SIMPLE ON DELETE NO ACTION ON UPDATE NO ACTION`,
			},
		},
		"multi-column FK with single column duplicated": {
			columns: []string{"name"},
			expectedStmts: []string{
				`ALTER TABLE "test_table" ADD CONSTRAINT "_pgroll_dup_fk_name_nick" FOREIGN KEY ("_pgroll_new_name", "nick") REFERENCES "users" ("name", "nick") MATCH FULL ON DELETE CASCADE ON UPDATE CASCADE`,
			},
		},
		"multi-column FK with multiple unrelated column duplicated": {
			columns: []string{"name", "description"},
			expectedStmts: []string{
				`ALTER TABLE "test_table" ADD CONSTRAINT "_pgroll_dup_fk_name_nick" FOREIGN KEY ("_pgroll_new_name", "nick") REFERENCES "users" ("name", "nick") MATCH FULL ON DELETE CASCADE ON UPDATE CASCADE`,
			},
		},
		"multi-column FK with multiple columns": {
			columns:       []string{"name", "nick"},
			expectedStmts: []string{`ALTER TABLE "test_table" ADD CONSTRAINT "_pgroll_dup_fk_name_nick" FOREIGN KEY ("_pgroll_new_name", "_pgroll_new_nick") REFERENCES "users" ("name", "nick") MATCH FULL ON DELETE CASCADE ON UPDATE CASCADE`},
		},
	} {
		t.Run(name, func(t *testing.T) {
			stmts := d.duplicateForeignKeyConstraints(nil, testCases.columns...)
			assert.Equal(t, len(testCases.expectedStmts), len(stmts))
			for _, stmt := range stmts {
				assert.Contains(t, testCases.expectedStmts, stmt)
			}
		})
	}
}

func TestDuplicateStmtBuilderIndexes(t *testing.T) {
	d := &duplicatorStmtBuilder{table}
	for name, testCases := range map[string]struct {
		columns       []string
		expectedStmts []string
	}{
		"single column duplicated": {
			columns:       []string{"nick"},
			expectedStmts: []string{},
		},
		"single-column index with single column duplicated": {
			columns:       []string{"email"},
			expectedStmts: []string{`CREATE INDEX CONCURRENTLY "_pgroll_dup_idx_email" ON "test_table" ("_pgroll_new_email")`},
		},
		"single-column index with multiple column duplicated": {
			columns:       []string{"email", "description"},
			expectedStmts: []string{`CREATE INDEX CONCURRENTLY "_pgroll_dup_idx_email" ON "test_table" ("_pgroll_new_email")`},
		},
		"multi-column index with single column duplicated": {
			columns:       []string{"name"},
			expectedStmts: []string{`CREATE INDEX CONCURRENTLY "_pgroll_dup_idx_name_city" ON "test_table" ("_pgroll_new_name", "city")`, `CREATE INDEX CONCURRENTLY "_pgroll_dup_idx_no_kate" ON "test_table" USING hash ("_pgroll_new_name") WITH (fillfactor = 70) WHERE "_pgroll_new_name" != 'Kate'`},
		},
		"multi-column index with multiple unrelated column duplicated": {
			columns:       []string{"name", "description"},
			expectedStmts: []string{`CREATE INDEX CONCURRENTLY "_pgroll_dup_idx_name_city" ON "test_table" ("_pgroll_new_name", "city")`, `CREATE INDEX CONCURRENTLY "_pgroll_dup_idx_no_kate" ON "test_table" USING hash ("_pgroll_new_name") WITH (fillfactor = 70) WHERE "_pgroll_new_name" != 'Kate'`},
		},
		"multi-column index with multiple columns": {
			columns:       []string{"name", "city"},
			expectedStmts: []string{`CREATE INDEX CONCURRENTLY "_pgroll_dup_idx_name_city" ON "test_table" ("_pgroll_new_name", "_pgroll_new_city")`, `CREATE INDEX CONCURRENTLY "_pgroll_dup_idx_no_kate" ON "test_table" USING hash ("_pgroll_new_name") WITH (fillfactor = 70) WHERE "_pgroll_new_name" != 'Kate'`},
		},
	} {
		t.Run(name, func(t *testing.T) {
			stmts := d.duplicateIndexes(nil, testCases.columns...)
			assert.Equal(t, len(testCases.expectedStmts), len(stmts))
			for _, stmt := range stmts {
				assert.True(t, slices.Contains(testCases.expectedStmts, stmt))
			}
		})
	}
}

func TestCreateIndexConcurrentlySqlGeneration(t *testing.T) {
	for name, testCases := range map[string]struct {
		indexName    string
		schemaName   string
		tableName    string
		columns      []string
		expectedStmt string
	}{
		"single column with schemaname": {
			indexName:    "idx_email",
			schemaName:   "test_sch",
			tableName:    "test_table",
			columns:      []string{"email"},
			expectedStmt: `CREATE UNIQUE INDEX CONCURRENTLY IF NOT EXISTS "idx_email" ON "test_sch"."test_table" ("email")`,
		},
		"single column with no schema name": {
			indexName:    "idx_email",
			tableName:    "test_table",
			columns:      []string{"email"},
			expectedStmt: `CREATE UNIQUE INDEX CONCURRENTLY IF NOT EXISTS "idx_email" ON "test_table" ("email")`,
		},
		"multi-column with no schema name": {
			indexName:    "idx_name_city",
			tableName:    "test_table",
			columns:      []string{"name", "city"},
			expectedStmt: `CREATE UNIQUE INDEX CONCURRENTLY IF NOT EXISTS "idx_name_city" ON "test_table" ("name", "city")`,
		},
		"multi-column with schema name": {
			indexName:    "idx_name_city",
			schemaName:   "test_sch",
			tableName:    "test_table",
			columns:      []string{"id", "name", "city"},
			expectedStmt: `CREATE UNIQUE INDEX CONCURRENTLY IF NOT EXISTS "idx_name_city" ON "test_sch"."test_table" ("id", "name", "city")`,
		},
	} {
		t.Run(name, func(t *testing.T) {
			action := NewCreateUniqueIndexConcurrentlyAction(nil, testCases.schemaName, testCases.indexName, false, testCases.tableName, testCases.columns...)
			stmt := action.getCreateUniqueIndexConcurrentlySQL()
			assert.Equal(t, testCases.expectedStmt, stmt)
		})
	}
}

func ptr[T any](x T) *T { return &x }
