// SPDX-License-Identifier: Apache-2.0

package sql2pgroll_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/xataio/pgroll/pkg/migrations"
	"github.com/xataio/pgroll/pkg/sql2pgroll"
	"github.com/xataio/pgroll/pkg/sql2pgroll/expect"
)

func TestConvertCreateIndexStatements(t *testing.T) {
	t.Parallel()

	tests := []struct {
		sql        string
		expectedOp migrations.Operation
	}{
		{
			sql:        "CREATE INDEX idx_name ON foo (bar)",
			expectedOp: expect.CreateIndexOp1,
		},
		{
			sql:        "CREATE INDEX idx_name ON foo (bar ASC)",
			expectedOp: expect.CreateIndexOp1,
		},
		{
			sql:        "CREATE INDEX idx_name ON foo USING btree (bar)",
			expectedOp: expect.CreateIndexOp1,
		},
		{
			sql:        "CREATE INDEX idx_name ON foo USING brin (bar)",
			expectedOp: expect.CreateIndexOp1WithMethod("brin"),
		},
		{
			sql:        "CREATE INDEX idx_name ON foo USING gin (bar)",
			expectedOp: expect.CreateIndexOp1WithMethod("gin"),
		},
		{
			sql:        "CREATE INDEX idx_name ON foo USING gist (bar)",
			expectedOp: expect.CreateIndexOp1WithMethod("gist"),
		},
		{
			sql:        "CREATE INDEX idx_name ON foo USING hash (bar)",
			expectedOp: expect.CreateIndexOp1WithMethod("hash"),
		},
		{
			sql:        "CREATE INDEX idx_name ON foo USING spgist (bar)",
			expectedOp: expect.CreateIndexOp1WithMethod("spgist"),
		},
		{
			sql:        "CREATE INDEX CONCURRENTLY idx_name ON foo (bar)",
			expectedOp: expect.CreateIndexOp1,
		},
		{
			sql:        "CREATE INDEX idx_name ON schema.foo (bar)",
			expectedOp: expect.CreateIndexOp2,
		},
		{
			sql:        "CREATE INDEX idx_name ON foo (bar, baz)",
			expectedOp: expect.CreateIndexOp3,
		},
		{
			sql:        "CREATE UNIQUE INDEX idx_name ON foo (bar)",
			expectedOp: expect.CreateIndexOp4,
		},
		{
			sql:        "CREATE INDEX idx_name ON foo (bar) WHERE (foo > 0)",
			expectedOp: expect.CreateIndexOp5,
		},
		{
			sql:        "CREATE INDEX idx_name ON foo (bar) WHERE foo > 0",
			expectedOp: expect.CreateIndexOp5,
		},
		{
			sql:        "CREATE INDEX idx_name ON foo (bar) WITH (fillfactor = 70)",
			expectedOp: expect.CreateIndexOpWithStorageParam("fillfactor=70"),
		},
		{
			sql:        "CREATE INDEX idx_name ON foo (bar) WITH (deduplicate_items = true)",
			expectedOp: expect.CreateIndexOpWithStorageParam("deduplicate_items=true"),
		},
		{
			sql:        "CREATE INDEX idx_name ON foo (bar) WITH (buffering = ON)",
			expectedOp: expect.CreateIndexOpWithStorageParam("buffering=on"),
		},
		{
			sql:        "CREATE INDEX idx_name ON foo (bar) WITH (buffering = OFF)",
			expectedOp: expect.CreateIndexOpWithStorageParam("buffering=off"),
		},
		{
			sql:        "CREATE INDEX idx_name ON foo (bar) WITH (buffering = AUTO)",
			expectedOp: expect.CreateIndexOpWithStorageParam("buffering=auto"),
		},
		{
			sql:        "CREATE INDEX idx_name ON foo (bar) WITH (fastupdate = true)",
			expectedOp: expect.CreateIndexOpWithStorageParam("fastupdate=true"),
		},
		{
			sql:        "CREATE INDEX idx_name ON foo (bar) WITH (pages_per_range = 100)",
			expectedOp: expect.CreateIndexOpWithStorageParam("pages_per_range=100"),
		},
		{
			sql:        "CREATE INDEX idx_name ON foo (bar) WITH (autosummarize = true)",
			expectedOp: expect.CreateIndexOpWithStorageParam("autosummarize=true"),
		},
		{
			sql:        "CREATE INDEX idx_name ON foo (bar) WITH (fillfactor = 70, deduplicate_items = true)",
			expectedOp: expect.CreateIndexOpWithStorageParam("fillfactor=70, deduplicate_items=true"),
		},
	}

	for _, tc := range tests {
		t.Run(tc.sql, func(t *testing.T) {
			ops, err := sql2pgroll.Convert(tc.sql)
			require.NoError(t, err)

			require.Len(t, ops, 1)

			assert.Equal(t, tc.expectedOp, ops[0])
		})
	}
}

func TestUnconvertableCreateIndexStatements(t *testing.T) {
	t.Parallel()

	tests := []string{
		// Tablespaces are not supported
		"CREATE INDEX idx_name ON foo (bar) TABLESPACE baz",
		// Index collations are not supported
		"CREATE INDEX idx_name ON foo (bar COLLATE en_US)",
		// Index ordering other than the default ASC is not supported
		"CREATE INDEX idx_name ON foo (bar DESC)",
		// Index nulls ordering is not supported
		"CREATE INDEX idx_name ON foo (bar NULLS FIRST)",
		"CREATE INDEX idx_name ON foo (bar NULLS LAST)",
		// Included columns are not supported
		"CREATE INDEX idx_name ON foo (bar) INCLUDE (baz)",
		// opclasses with or without options are not supported
		"CREATE INDEX idx_name ON foo (bar opclass (test = test))",
		"CREATE INDEX idx_name ON foo (bar opclass)",
		// Indexes created with ONLY are not supported
		"CREATE INDEX idx_name ON ONLY foo (bar)",
		// Indexes with NULLS NOT DISTINCT are not supported
		"CREATE INDEX idx_name ON foo(a) NULLS NOT DISTINCT",
		// IF NOT EXISTS is unsupported
		"CREATE INDEX IF NOT EXISTS idx_name ON foo(a)",
		// Indexes defined on expressions are not supported
		"CREATE INDEX idx_name ON foo(LOWER(a))",
		"CREATE INDEX idx_name ON foo(a, LOWER(b))",
	}

	for _, sql := range tests {
		t.Run(sql, func(t *testing.T) {
			ops, err := sql2pgroll.Convert(sql)
			require.NoError(t, err)

			require.Len(t, ops, 1)

			assert.Equal(t, expect.RawSQLOp(sql), ops[0])
		})
	}
}
