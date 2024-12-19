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
		// TODO: StorageParams
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
