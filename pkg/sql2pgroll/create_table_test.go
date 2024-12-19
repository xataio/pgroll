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

func TestConvertCreateTableStatements(t *testing.T) {
	t.Parallel()

	tests := []struct {
		sql        string
		expectedOp migrations.Operation
	}{
		{
			sql:        "CREATE TABLE foo(a int)",
			expectedOp: expect.CreateTableOp1,
		},
		{
			sql:        "CREATE TABLE foo(a int NULL)",
			expectedOp: expect.CreateTableOp1,
		},
		{
			sql:        "CREATE TABLE foo(a int NOT NULL)",
			expectedOp: expect.CreateTableOp2,
		},
		{
			sql:        "CREATE TABLE foo(a int UNIQUE)",
			expectedOp: expect.CreateTableOp5,
		},
		{
			sql:        "CREATE TABLE foo(a int PRIMARY KEY)",
			expectedOp: expect.CreateTableOp6,
		},
		{
			sql:        "CREATE TABLE foo(a int CHECK (a > 0))",
			expectedOp: expect.CreateTableOp10,
		},
		{
			sql:        "CREATE TABLE foo(a timestamptz DEFAULT now())",
			expectedOp: expect.CreateTableOp11,
		},
		{
			sql:        "CREATE TABLE foo(a int REFERENCES bar(b))",
			expectedOp: expect.CreateTableOp12,
		},
		{
			sql:        "CREATE TABLE foo(a int REFERENCES bar(b) ON UPDATE NO ACTION)",
			expectedOp: expect.CreateTableOp12,
		},
		{
			sql:        "CREATE TABLE foo(a int REFERENCES bar(b) ON DELETE NO ACTION)",
			expectedOp: expect.CreateTableOp12,
		},
		{
			sql:        "CREATE TABLE foo(a int REFERENCES bar(b) ON DELETE RESTRICT)",
			expectedOp: expect.CreateTableOp13,
		},
		{
			sql:        "CREATE TABLE foo(a int REFERENCES bar(b) ON DELETE SET NULL)",
			expectedOp: expect.CreateTableOp14,
		},
		{
			sql:        "CREATE TABLE foo(a int REFERENCES bar(b) ON DELETE SET DEFAULT)",
			expectedOp: expect.CreateTableOp15,
		},
		{
			sql:        "CREATE TABLE foo(a int REFERENCES bar(b) ON DELETE CASCADE)",
			expectedOp: expect.CreateTableOp16,
		},
		{
			sql:        "CREATE TABLE foo(a varchar(255))",
			expectedOp: expect.CreateTableOp3,
		},
		{
			sql:        "CREATE TABLE foo(a numeric(10, 2))",
			expectedOp: expect.CreateTableOp4,
		},
		{
			sql:        "CREATE TABLE foo(a text[])",
			expectedOp: expect.CreateTableOp7,
		},
		{
			sql:        "CREATE TABLE foo(a text[5])",
			expectedOp: expect.CreateTableOp8,
		},
		{
			sql:        "CREATE TABLE foo(a text[5][3])",
			expectedOp: expect.CreateTableOp9,
		},
	}

	for _, tc := range tests {
		t.Run(tc.sql, func(t *testing.T) {
			ops, err := sql2pgroll.Convert(tc.sql)
			require.NoError(t, err)

			require.Len(t, ops, 1)

			createTableOp, ok := ops[0].(*migrations.OpCreateTable)
			require.True(t, ok)

			assert.Equal(t, tc.expectedOp, createTableOp)
		})
	}
}

func TestUnconvertableCreateTableStatements(t *testing.T) {
	t.Parallel()

	tests := []string{
		// Temporary and unlogged tables are not supported
		"CREATE TEMPORARY TABLE foo(a int)",
		"CREATE UNLOGGED TABLE foo(a int)",

		// The IF NOT EXISTS clause is not supported
		"CREATE TABLE IF NOT EXISTS foo(a int)",

		// Table inheritance is not supported
		"CREATE TABLE foo(a int) INHERITS (bar)",

		// Any kind of partitioning is not supported
		"CREATE TABLE foo(a int) PARTITION BY RANGE (a)",
		"CREATE TABLE foo(a int) PARTITION BY LIST (a)",
		"CREATE TABLE foo PARTITION OF bar FOR VALUES FROM (1) to (10)",

		// Specifying a table access method is not supported
		"CREATE TABLE foo(a int) USING bar",

		// Specifying storage options is not supported
		"CREATE TABLE foo(a int) WITH (fillfactor=70)",

		// ON COMMMIT options are not supported. These options are syntactically
		// valid for all tables, but Postgres will reject them for non-temporary
		// tables. We err on the side of caution and reject them for all tables.
		"CREATE TABLE foo(a int) ON COMMIT DROP",

		// Specifying a tablespace is not supported
		"CREATE TABLE foo(a int) TABLESPACE bar",

		// CREATE TABLE OF type_name is not supported
		"CREATE TABLE foo OF type_bar",

		// The LIKE clause is not supported
		"CREATE TABLE foo(a int, LIKE bar)",
		"CREATE TABLE foo(LIKE bar)",

		// Column `STORAGE` options are not supported
		"CREATE TABLE foo(a int STORAGE PLAIN)",

		// Column compression options are not supported
		"CREATE TABLE foo(a text COMPRESSION pglz)",

		// Column collation is not supported
		"CREATE TABLE foo(a text COLLATE en_US)",

		// Table constraints, named and unnamed, are not supported
		"CREATE TABLE foo(a int, CONSTRAINT foo_check CHECK (a > 0))",
		"CREATE TABLE foo(a int, CONSTRAINT foo_unique UNIQUE (a))",
		"CREATE TABLE foo(a int, CONSTRAINT foo_pk PRIMARY KEY (a))",
		"CREATE TABLE foo(a int, CONSTRAINT foo_fk FOREIGN KEY (a) REFERENCES bar(b))",
		"CREATE TABLE foo(a int, CHECK (a > 0))",
		"CREATE TABLE foo(a int, UNIQUE (a))",
		"CREATE TABLE foo(a int, PRIMARY KEY (a))",
		"CREATE TABLE foo(a int, FOREIGN KEY (a) REFERENCES bar(b))",

		// Primary key constraint options are not supported
		"CREATE TABLE foo(a int PRIMARY KEY USING INDEX TABLESPACE bar)",
		"CREATE TABLE foo(a int PRIMARY KEY WITH (fillfactor=70))",

		// CHECK constraint NO INHERIT option is not supported
		"CREATE TABLE foo(a int CHECK (a > 0) NO INHERIT)",

		// Options on UNIQUE constraints are not supported
		"CREATE TABLE foo(a int UNIQUE NULLS NOT DISTINCT)",
		"CREATE TABLE foo(a int UNIQUE WITH (fillfactor=70))",
		"CREATE TABLE foo(a int UNIQUE USING INDEX TABLESPACE baz)",

		// Some options on FOREIGN KEY constraints are not supported
		"CREATE TABLE foo(a int REFERENCES bar (b) ON UPDATE RESTRICT)",
		"CREATE TABLE foo(a int REFERENCES bar (b) ON UPDATE CASCADE)",
		"CREATE TABLE foo(a int REFERENCES bar (b) ON UPDATE SET NULL)",
		"CREATE TABLE foo(a int REFERENCES bar (b) ON UPDATE SET DEFAULT)",
		"CREATE TABLE foo(a int REFERENCES bar (b) MATCH FULL)",

		// Named inline constraints are not supported
		"CREATE TABLE foo(a int CONSTRAINT foo_check CHECK (a > 0))",
		"CREATE TABLE foo(a int CONSTRAINT foo_unique UNIQUE)",
		"CREATE TABLE foo(a int CONSTRAINT foo_pk PRIMARY KEY)",
		"CREATE TABLE foo(a int CONSTRAINT foo_fk REFERENCES bar(b))",
		"CREATE TABLE foo(a int CONSTRAINT foo_default DEFAULT 0)",
		"CREATE TABLE foo(a int CONSTRAINT foo_null NULL)",
		"CREATE TABLE foo(a int CONSTRAINT foo_notnull NOT NULL)",
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
