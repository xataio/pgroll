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

func TestConvertRenameStatements(t *testing.T) {
	t.Parallel()

	tests := []struct {
		sql        string
		expectedOp migrations.Operation
	}{
		{
			sql:        "ALTER TABLE foo RENAME COLUMN a TO b",
			expectedOp: expect.AlterColumnOp4,
		},
		{
			sql:        "ALTER TABLE foo RENAME a TO b",
			expectedOp: expect.AlterColumnOp4,
		},
		{
			sql:        "ALTER TABLE foo RENAME TO bar",
			expectedOp: expect.RenameTableOp1,
		},
		{
			sql:        "ALTER TABLE foo RENAME CONSTRAINT bar TO baz",
			expectedOp: expect.RenameConstraintOp1,
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
