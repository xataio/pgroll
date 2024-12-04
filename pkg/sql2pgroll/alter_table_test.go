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

func TestConvertAlterTableStatements(t *testing.T) {
	t.Parallel()

	tests := []struct {
		sql        string
		expectedOp migrations.Operation
	}{
		{
			sql:        "ALTER TABLE foo ALTER COLUMN a SET NOT NULL",
			expectedOp: expect.AlterTableOp1,
		},
		{
			sql:        "ALTER TABLE foo ALTER COLUMN a DROP NOT NULL",
			expectedOp: expect.AlterTableOp2,
		},
		{
			sql:        "ALTER TABLE foo ALTER COLUMN a SET DATA TYPE text",
			expectedOp: expect.AlterTableOp3,
		},
		{
			sql:        "ALTER TABLE foo ALTER COLUMN a TYPE text",
			expectedOp: expect.AlterTableOp3,
		},
		{
			sql:        "ALTER TABLE foo ADD CONSTRAINT bar UNIQUE (a)",
			expectedOp: expect.AlterTableOp4,
		},
		{
			sql:        "ALTER TABLE foo ADD CONSTRAINT bar UNIQUE (a, b)",
			expectedOp: expect.AlterTableOp5,
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

func TestUnconvertableAlterTableAddConstraintStatements(t *testing.T) {
	t.Parallel()

	tests := []string{
		// UNIQUE constraints with various options that are not representable by
		// `OpCreateConstraint` operations
		"ALTER TABLE foo ADD CONSTRAINT bar UNIQUE NULLS NOT DISTINCT (a)",
		"ALTER TABLE foo ADD CONSTRAINT bar UNIQUE (a) INCLUDE (b)",
		"ALTER TABLE foo ADD CONSTRAINT bar UNIQUE (a) WITH (fillfactor=70)",
		"ALTER TABLE foo ADD CONSTRAINT bar UNIQUE (a) USING INDEX TABLESPACE baz",

		// COLLATE and USING clauses are not representable by `OpAlterColumn`
		// operations when changing data type.
		`ALTER TABLE foo ALTER COLUMN a SET DATA TYPE text COLLATE "en_US"`,
		"ALTER TABLE foo ALTER COLUMN a SET DATA TYPE text USING 'foo'",
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
