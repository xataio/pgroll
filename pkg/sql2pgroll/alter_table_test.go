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
			sql:        "ALTER TABLE foo ALTER COLUMN bar SET DEFAULT now()",
			expectedOp: expect.AlterColumnOp11,
		},
		{
			sql:        "ALTER TABLE foo ALTER COLUMN bar SET DEFAULT (first_name || ' ' || last_name)",
			expectedOp: expect.AlterColumnOp12,
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
			expectedOp: expect.AddForeignKeyOp1WithParams(migrations.ForeignKeyMatchTypeSIMPLE, migrations.ForeignKeyOnDeleteNOACTION, migrations.ForeignKeyOnDeleteNOACTION),
		},
		{
			sql:        "ALTER TABLE foo ADD CONSTRAINT fk_bar_cd FOREIGN KEY (a, b) REFERENCES bar (c, d) ON DELETE NO ACTION;",
			expectedOp: expect.AddForeignKeyOp1WithParams(migrations.ForeignKeyMatchTypeSIMPLE, migrations.ForeignKeyOnDeleteNOACTION, migrations.ForeignKeyOnDeleteNOACTION),
		},
		{
			sql:        "ALTER TABLE foo ADD CONSTRAINT fk_bar_cd FOREIGN KEY (a, b) REFERENCES bar (c, d) ON DELETE RESTRICT;",
			expectedOp: expect.AddForeignKeyOp1WithParams(migrations.ForeignKeyMatchTypeSIMPLE, migrations.ForeignKeyOnDeleteRESTRICT, migrations.ForeignKeyOnDeleteNOACTION),
		},
		{
			sql:        "ALTER TABLE foo ADD CONSTRAINT fk_bar_cd FOREIGN KEY (a, b) REFERENCES bar (c, d) ON DELETE SET DEFAULT ;",
			expectedOp: expect.AddForeignKeyOp1WithParams(migrations.ForeignKeyMatchTypeSIMPLE, migrations.ForeignKeyOnDeleteSETDEFAULT, migrations.ForeignKeyOnDeleteNOACTION),
		},
		{
			sql:        "ALTER TABLE foo ADD CONSTRAINT fk_bar_cd FOREIGN KEY (a, b) REFERENCES bar (c, d) ON DELETE SET NULL;",
			expectedOp: expect.AddForeignKeyOp1WithParams(migrations.ForeignKeyMatchTypeSIMPLE, migrations.ForeignKeyOnDeleteSETNULL, migrations.ForeignKeyOnDeleteNOACTION),
		},
		{
			sql:        "ALTER TABLE foo ADD CONSTRAINT fk_bar_cd FOREIGN KEY (a, b) REFERENCES bar (c, d) ON DELETE RESTRICT ON UPDATE SET DEFAULT;",
			expectedOp: expect.AddForeignKeyOp1WithParams(migrations.ForeignKeyMatchTypeSIMPLE, migrations.ForeignKeyOnDeleteRESTRICT, migrations.ForeignKeyOnDeleteSETDEFAULT),
		},
		{
			sql:        "ALTER TABLE foo ADD CONSTRAINT fk_bar_cd FOREIGN KEY (a, b) REFERENCES bar (c, d) ON DELETE SET DEFAULT ON UPDATE RESTRICT;",
			expectedOp: expect.AddForeignKeyOp1WithParams(migrations.ForeignKeyMatchTypeSIMPLE, migrations.ForeignKeyOnDeleteSETDEFAULT, migrations.ForeignKeyOnDeleteRESTRICT),
		},
		{
			sql:        "ALTER TABLE foo ADD CONSTRAINT fk_bar_cd FOREIGN KEY (a, b) REFERENCES bar (c, d) MATCH FULL ON UPDATE SET NULL;",
			expectedOp: expect.AddForeignKeyOp1WithParams(migrations.ForeignKeyMatchTypeFULL, migrations.ForeignKeyOnDeleteNOACTION, migrations.ForeignKeyOnDeleteSETNULL),
		},
		{
			sql:        "ALTER TABLE foo ADD CONSTRAINT fk_bar_c FOREIGN KEY (a) REFERENCES bar (c);",
			expectedOp: expect.AddForeignKeyOp2,
		},
		{
			sql:        "ALTER TABLE schema_a.foo ADD CONSTRAINT fk_bar_c FOREIGN KEY (a) REFERENCES schema_a.bar (c);",
			expectedOp: expect.AddForeignKeyOp3,
		},
		{
			sql:        "ALTER TABLE foo DROP CONSTRAINT constraint_foo",
			expectedOp: expect.OpDropConstraintWithTable("foo"),
		},
		{
			sql:        "ALTER TABLE schema.foo DROP CONSTRAINT constraint_foo",
			expectedOp: expect.OpDropConstraintWithTable("schema.foo"),
		},
		{
			sql:        "ALTER TABLE foo DROP CONSTRAINT IF EXISTS constraint_foo",
			expectedOp: expect.OpDropConstraintWithTable("foo"),
		},
		{
			sql:        "ALTER TABLE foo DROP CONSTRAINT IF EXISTS constraint_foo RESTRICT",
			expectedOp: expect.OpDropConstraintWithTable("foo"),
		},
		{
			sql:        "ALTER TABLE foo ADD CONSTRAINT bar CHECK (age > 0)",
			expectedOp: expect.CreateConstraintOp3,
		},
		{
			sql:        "ALTER TABLE schema.foo ADD CONSTRAINT bar CHECK (age > 0)",
			expectedOp: expect.CreateConstraintOp4,
		},

		// Add column
		{
			sql:        "ALTER TABLE foo ADD COLUMN bar int",
			expectedOp: expect.AddColumnOp1,
		},
		{
			sql:        "ALTER TABLE foo ADD COLUMN bar int NOT NULL",
			expectedOp: expect.AddColumnOp8,
		},
		{
			sql:        "ALTER TABLE schema.foo ADD COLUMN bar int",
			expectedOp: expect.AddColumnOp2,
		},
		{
			sql:        "ALTER TABLE foo ADD COLUMN bar int DEFAULT 123",
			expectedOp: expect.AddColumnOp1WithDefault(ptr("123")),
		},
		{
			sql:        "ALTER TABLE foo ADD COLUMN bar int DEFAULT 'baz'",
			expectedOp: expect.AddColumnOp1WithDefault(ptr("'baz'")),
		},
		{
			sql:        "ALTER TABLE foo ADD COLUMN bar int DEFAULT null",
			expectedOp: expect.AddColumnOp1WithDefault(nil),
		},
		{
			sql:        "ALTER TABLE foo ADD COLUMN bar int NULL",
			expectedOp: expect.AddColumnOp3,
		},
		{
			sql:        "ALTER TABLE foo ADD COLUMN bar int UNIQUE",
			expectedOp: expect.AddColumnOp4,
		},
		{
			sql:        "ALTER TABLE foo ADD COLUMN bar int UNIQUE NOT DEFERRABLE",
			expectedOp: expect.AddColumnOp4,
		},
		{
			sql:        "ALTER TABLE foo ADD COLUMN bar int UNIQUE INITIALLY IMMEDIATE",
			expectedOp: expect.AddColumnOp4,
		},
		{
			sql:        "ALTER TABLE foo ADD COLUMN bar int PRIMARY KEY",
			expectedOp: expect.AddColumnOp5,
		},
		{
			sql:        "ALTER TABLE foo ADD COLUMN bar int CHECK (bar > 0)",
			expectedOp: expect.AddColumnOp6,
		},
		{
			sql:        "ALTER TABLE foo ADD COLUMN bar int CONSTRAINT check_bar CHECK (bar > 0)",
			expectedOp: expect.AddColumnOp7,
		},
		{
			sql:        "ALTER TABLE foo ADD COLUMN bar int CONSTRAINT fk_baz REFERENCES baz (bar) MATCH FULL",
			expectedOp: expect.AddColumnOp8WithOnDeleteAction(migrations.ForeignKeyOnDeleteNOACTION, migrations.ForeignKeyOnDeleteNOACTION, migrations.ForeignKeyMatchTypeFULL),
		},
		{
			sql:        "ALTER TABLE foo ADD COLUMN bar int CONSTRAINT fk_baz REFERENCES baz (bar) ON UPDATE NO ACTION",
			expectedOp: expect.AddColumnOp8WithOnDeleteAction(migrations.ForeignKeyOnDeleteNOACTION, migrations.ForeignKeyOnDeleteNOACTION, migrations.ForeignKeyMatchTypeSIMPLE),
		},
		{
			sql:        "ALTER TABLE foo ADD COLUMN bar int CONSTRAINT fk_baz REFERENCES baz (bar) ON DELETE NO ACTION",
			expectedOp: expect.AddColumnOp8WithOnDeleteAction(migrations.ForeignKeyOnDeleteNOACTION, migrations.ForeignKeyOnDeleteNOACTION, migrations.ForeignKeyMatchTypeSIMPLE),
		},
		{
			sql:        "ALTER TABLE foo ADD COLUMN bar int CONSTRAINT fk_baz REFERENCES baz (bar) ON DELETE RESTRICT",
			expectedOp: expect.AddColumnOp8WithOnDeleteAction(migrations.ForeignKeyOnDeleteRESTRICT, migrations.ForeignKeyOnDeleteNOACTION, migrations.ForeignKeyMatchTypeSIMPLE),
		},
		{
			sql:        "ALTER TABLE foo ADD COLUMN bar int CONSTRAINT fk_baz REFERENCES baz (bar) MATCH SIMPLE ON DELETE SET NULL ",
			expectedOp: expect.AddColumnOp8WithOnDeleteAction(migrations.ForeignKeyOnDeleteSETNULL, migrations.ForeignKeyOnDeleteNOACTION, migrations.ForeignKeyMatchTypeSIMPLE),
		},
		{
			sql:        "ALTER TABLE foo ADD COLUMN bar int CONSTRAINT fk_baz REFERENCES baz (bar) ON DELETE SET DEFAULT ON UPDATE NO ACTION",
			expectedOp: expect.AddColumnOp8WithOnDeleteAction(migrations.ForeignKeyOnDeleteSETDEFAULT, migrations.ForeignKeyOnDeleteNOACTION, migrations.ForeignKeyMatchTypeSIMPLE),
		},
		{
			sql:        "ALTER TABLE foo ADD COLUMN bar int CONSTRAINT fk_baz REFERENCES baz (bar) MATCH FULL ON DELETE CASCADE ON UPDATE CASCADE",
			expectedOp: expect.AddColumnOp8WithOnDeleteAction(migrations.ForeignKeyOnDeleteCASCADE, migrations.ForeignKeyOnDeleteCASCADE, migrations.ForeignKeyMatchTypeFULL),
		},
		{
			sql:        "ALTER TABLE foo ADD COLUMN bar int REFERENCES baz (bar) ON UPDATE RESTRICT",
			expectedOp: expect.AddColumnOp9WithOnDeleteActionUnnamed("foo_bar_fkey", migrations.ForeignKeyOnDeleteNOACTION, migrations.ForeignKeyOnDeleteRESTRICT, migrations.ForeignKeyMatchTypeSIMPLE),
		},
		{
			sql:        "ALTER TABLE foo ADD COLUMN bar int REFERENCES baz (bar) ON UPDATE CASCADE",
			expectedOp: expect.AddColumnOp9WithOnDeleteActionUnnamed("foo_bar_fkey", migrations.ForeignKeyOnDeleteNOACTION, migrations.ForeignKeyOnDeleteCASCADE, migrations.ForeignKeyMatchTypeSIMPLE),
		},
		{
			sql:        "ALTER TABLE foo ADD COLUMN bar int REFERENCES baz (bar) ON UPDATE SET NULL",
			expectedOp: expect.AddColumnOp9WithOnDeleteActionUnnamed("foo_bar_fkey", migrations.ForeignKeyOnDeleteNOACTION, migrations.ForeignKeyOnDeleteSETNULL, migrations.ForeignKeyMatchTypeSIMPLE),
		},
		{
			sql:        "ALTER TABLE foo ADD COLUMN bar int REFERENCES baz (bar) ON UPDATE SET DEFAULT",
			expectedOp: expect.AddColumnOp9WithOnDeleteActionUnnamed("foo_bar_fkey", migrations.ForeignKeyOnDeleteNOACTION, migrations.ForeignKeyOnDeleteSETDEFAULT, migrations.ForeignKeyMatchTypeSIMPLE),
		},
		{
			sql:        "ALTER TABLE foo ADD CONSTRAINT fk_baz FOREIGN KEY (a, b) REFERENCES bar (c, d) ON UPDATE RESTRICT",
			expectedOp: expect.AddConstraintOp10ForeignKey(migrations.ForeignKeyOnDeleteNOACTION, migrations.ForeignKeyOnDeleteRESTRICT, migrations.ForeignKeyMatchTypeSIMPLE),
		},
		{
			sql:        "ALTER TABLE foo ADD CONSTRAINT fk_baz FOREIGN KEY (a, b) REFERENCES bar (c, d) ON UPDATE CASCADE",
			expectedOp: expect.AddConstraintOp10ForeignKey(migrations.ForeignKeyOnDeleteNOACTION, migrations.ForeignKeyOnDeleteCASCADE, migrations.ForeignKeyMatchTypeSIMPLE),
		},
		{
			sql:        "ALTER TABLE foo ADD CONSTRAINT fk_baz FOREIGN KEY (a, b) REFERENCES bar (c, d) ON UPDATE SET NULL",
			expectedOp: expect.AddConstraintOp10ForeignKey(migrations.ForeignKeyOnDeleteNOACTION, migrations.ForeignKeyOnDeleteSETNULL, migrations.ForeignKeyMatchTypeSIMPLE),
		},
		{
			sql:        "ALTER TABLE foo ADD CONSTRAINT fk_baz FOREIGN KEY (a, b) REFERENCES bar (c, d) ON UPDATE SET DEFAULT",
			expectedOp: expect.AddConstraintOp10ForeignKey(migrations.ForeignKeyOnDeleteNOACTION, migrations.ForeignKeyOnDeleteSETDEFAULT, migrations.ForeignKeyMatchTypeSIMPLE),
		},
		{
			sql:        "ALTER TABLE foo ADD CONSTRAINT fk_baz FOREIGN KEY (a, b) REFERENCES bar (c, d) MATCH FULL",
			expectedOp: expect.AddConstraintOp10ForeignKey(migrations.ForeignKeyOnDeleteNOACTION, migrations.ForeignKeyOnDeleteNOACTION, migrations.ForeignKeyMatchTypeFULL),
		},
		{
			sql:        "ALTER TABLE foo ADD COLUMN bar int GENERATED BY DEFAULT AS IDENTITY ",
			expectedOp: expect.AddColumnOp9,
		},
		{
			sql:        "ALTER TABLE foo ADD COLUMN bar int GENERATED ALWAYS AS ( 123 ) STORED",
			expectedOp: expect.AddColumnOp10,
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

		// Unsupported foreign key statements
		"ALTER TABLE foo ADD CONSTRAINT fk_bar_cd FOREIGN KEY (a, b) REFERENCES bar (c, d) NOT VALID",
		// MATCH PARTIAL is not implemented in the actual parser yet
		//"ALTER TABLE foo ADD CONSTRAINT fk_bar_cd FOREIGN KEY (a, b) REFERENCES bar (c, d) MATCH PARTIAL;",

		// Drop constraint with CASCADE
		"ALTER TABLE foo DROP CONSTRAINT bar CASCADE",

		// NO INHERIT and NOT VALID options on CHECK constraints are not
		// representable by `OpCreateConstraint`
		"ALTER TABLE foo ADD CONSTRAINT bar CHECK (age > 0) NO INHERIT",
		"ALTER TABLE foo ADD CONSTRAINT bar CHECK (age > 0) NOT VALID",

		// ADD COLUMN cases not yet covered
		"ALTER TABLE foo ADD COLUMN IF NOT EXISTS bar int",
		"ALTER TABLE foo ADD COLUMN bar int UNIQUE DEFERRABLE",
		"ALTER TABLE foo ADD COLUMN bar int UNIQUE INITIALLY DEFERRED",
		"ALTER TABLE foo ADD COLUMN bar int COLLATE en_US",
		"ALTER TABLE foo ADD COLUMN bar int COMPRESSION pglz",
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

func ptr[T any](v T) *T {
	return &v
}
