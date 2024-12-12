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
			expectedOp: expect.AlterColumnOp1,
		},
		{
			sql:        "ALTER TABLE foo ALTER COLUMN a DROP NOT NULL",
			expectedOp: expect.AlterColumnOp2,
		},
		{
			sql:        "ALTER TABLE foo ALTER COLUMN a SET DATA TYPE text",
			expectedOp: expect.AlterColumnOp3,
		},
		{
			sql:        "ALTER TABLE foo ALTER COLUMN a TYPE text",
			expectedOp: expect.AlterColumnOp3,
		},
		{
			sql:        "ALTER TABLE foo ALTER COLUMN bar SET DEFAULT 'baz'",
			expectedOp: expect.AlterColumnOp5,
		},
		{
			sql:        "ALTER TABLE foo ALTER COLUMN bar SET DEFAULT 123",
			expectedOp: expect.AlterColumnOp6,
		},
		{
			sql:        "ALTER TABLE foo ALTER COLUMN bar SET DEFAULT true",
			expectedOp: expect.AlterColumnOp9,
		},
		{
			sql:        "ALTER TABLE foo ALTER COLUMN bar SET DEFAULT B'0101'",
			expectedOp: expect.AlterColumnOp10,
		},
		{
			sql:        "ALTER TABLE foo ALTER COLUMN bar SET DEFAULT 123.456",
			expectedOp: expect.AlterColumnOp8,
		},
		{
			sql:        "ALTER TABLE foo ALTER COLUMN bar DROP DEFAULT",
			expectedOp: expect.AlterColumnOp7,
		},
		{
			sql:        "ALTER TABLE foo ALTER COLUMN bar SET DEFAULT null",
			expectedOp: expect.AlterColumnOp7,
		},
		{
			sql:        "ALTER TABLE foo ADD CONSTRAINT bar UNIQUE (a)",
			expectedOp: expect.CreateConstraintOp1,
		},
		{
			sql:        "ALTER TABLE foo ADD CONSTRAINT bar UNIQUE (a, b)",
			expectedOp: expect.CreateConstraintOp2,
		},
		{
			sql:        "ALTER TABLE foo DROP COLUMN bar",
			expectedOp: expect.DropColumnOp1,
		},
		{
			sql:        "ALTER TABLE foo DROP COLUMN bar RESTRICT ",
			expectedOp: expect.DropColumnOp1,
		},
		{
			sql:        "ALTER TABLE foo ADD CONSTRAINT fk_bar_cd FOREIGN KEY (a, b) REFERENCES bar (c, d);",
			expectedOp: expect.AddForeignKeyOp1,
		},
		{
			sql:        "ALTER TABLE foo ADD CONSTRAINT fk_bar_cd FOREIGN KEY (a, b) REFERENCES bar (c, d) ON DELETE NO ACTION;",
			expectedOp: expect.AddForeignKeyOp1,
		},
		{
			sql:        "ALTER TABLE foo ADD CONSTRAINT fk_bar_cd FOREIGN KEY (a, b) REFERENCES bar (c, d) ON DELETE RESTRICT;",
			expectedOp: expect.AddForeignKeyOp2,
		},
		{
			sql:        "ALTER TABLE foo ADD CONSTRAINT fk_bar_cd FOREIGN KEY (a, b) REFERENCES bar (c, d) ON DELETE SET DEFAULT ;",
			expectedOp: expect.AddForeignKeyOp3,
		},
		{
			sql:        "ALTER TABLE foo ADD CONSTRAINT fk_bar_cd FOREIGN KEY (a, b) REFERENCES bar (c, d) ON DELETE SET NULL;",
			expectedOp: expect.AddForeignKeyOp4,
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

func TestUnconvertableAlterTableStatements(t *testing.T) {
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

		// CASCADE and IF EXISTS clauses are not represented by OpDropColumn
		"ALTER TABLE foo DROP COLUMN bar CASCADE",
		"ALTER TABLE foo DROP COLUMN IF EXISTS bar",

		// Non literal default values
		"ALTER TABLE foo ALTER COLUMN bar SET DEFAULT now()",
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
