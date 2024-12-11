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

func TestDropIndexStatements(t *testing.T) {
	t.Parallel()

	tests := []struct {
		sql        string
		expectedOp migrations.Operation
	}{
		{
			sql:        "DROP INDEX foo",
			expectedOp: expect.DropIndexOp1,
		},
		{
			sql:        "DROP INDEX myschema.foo",
			expectedOp: expect.DropIndexOp1,
		},
		{
			sql:        "DROP INDEX foo RESTRICT",
			expectedOp: expect.DropIndexOp1,
		},
		{
			sql:        "DROP INDEX IF EXISTS foo",
			expectedOp: expect.DropIndexOp1,
		},
		{
			sql:        "DROP INDEX CONCURRENTLY foo",
			expectedOp: expect.DropIndexOp1,
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

func TestUnconvertableDropIndexStatements(t *testing.T) {
	t.Parallel()

	tests := []string{
		"DROP INDEX foo CASCADE",
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
