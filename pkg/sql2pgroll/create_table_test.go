// SPDX-License-Identifier: Apache-2.0

package sql2pgroll_test

import (
	"fmt"
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
			sql:        "CREATE TABLE schema.foo(a int)",
			expectedOp: expect.CreateTableOp17,
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
			sql:        "CREATE TABLE foo(a int UNIQUE NOT DEFERRABLE)",
			expectedOp: expect.CreateTableOp5,
		},
		{
			sql:        "CREATE TABLE foo(a int UNIQUE INITIALLY IMMEDIATE)",
			expectedOp: expect.CreateTableOp5,
		},
		{
			sql:        "CREATE TABLE foo(a int PRIMARY KEY)",
			expectedOp: expect.CreateTableOp6,
		},
		{
			sql:        "CREATE TABLE foo(a int PRIMARY KEY NOT DEFERRABLE)",
			expectedOp: expect.CreateTableOp6,
		},
		{
			sql:        "CREATE TABLE foo(a int PRIMARY KEY INITIALLY IMMEDIATE)",
			expectedOp: expect.CreateTableOp6,
		},
		{
			sql:        "CREATE TABLE foo(a int CHECK (a > 0))",
			expectedOp: expect.CreateTableOp10,
		},
		{
			sql:        "CREATE TABLE foo(a int CONSTRAINT my_check CHECK (a > 0))",
			expectedOp: expect.CreateTableOp18,
		},
		{
			sql:        "CREATE TABLE foo(a timestamptz DEFAULT now())",
			expectedOp: expect.CreateTableOp11,
		},
		{
			sql:        "CREATE TABLE foo(a int DEFAULT NULL)",
			expectedOp: expect.CreateTableOp20,
		},
		{
			sql:        "CREATE TABLE foo(a int CONSTRAINT my_fk REFERENCES bar(b) MATCH FULL)",
			expectedOp: expect.CreateTableOp19,
		},
		{
			sql:        "CREATE TABLE foo(a int REFERENCES bar(b))",
			expectedOp: expect.CreateTableOp12,
		},
		{
			sql:        "CREATE TABLE foo(a int REFERENCES bar(b) NOT DEFERRABLE)",
			expectedOp: expect.CreateTableOp12,
		},
		{
			sql:        "CREATE TABLE foo(a int REFERENCES bar(b) INITIALLY IMMEDIATE)",
			expectedOp: expect.CreateTableOp12,
		},
		{
			sql:        "CREATE TABLE foo(a int REFERENCES bar(b) ON UPDATE NO ACTION)",
			expectedOp: expect.CreateTableOp12,
		},
		{
			sql:        "CREATE TABLE foo(a int REFERENCES bar(b) ON UPDATE NO ACTION NOT DEFERRABLE)",
			expectedOp: expect.CreateTableOp12,
		},
		{
			sql:        "CREATE TABLE foo(a int REFERENCES bar(b) ON DELETE NO ACTION)",
			expectedOp: expect.CreateTableOp12,
		},
		{
			sql:        "CREATE TABLE foo(a int REFERENCES bar(b) ON DELETE NO ACTION NOT DEFERRABLE)",
			expectedOp: expect.CreateTableOp12,
		},
		{
			sql:        "CREATE TABLE foo(a int REFERENCES bar(b) ON DELETE RESTRICT)",
			expectedOp: expect.CreateTableOp13,
		},
		{
			sql:        "CREATE TABLE foo(a int REFERENCES bar(b) ON DELETE RESTRICT NOT DEFERRABLE)",
			expectedOp: expect.CreateTableOp13,
		},
		{
			sql:        "CREATE TABLE foo(a int REFERENCES bar(b) ON DELETE SET NULL)",
			expectedOp: expect.CreateTableOp14,
		},
		{
			sql:        "CREATE TABLE foo(a int REFERENCES bar(b) ON DELETE SET NULL NOT DEFERRABLE)",
			expectedOp: expect.CreateTableOp14,
		},
		{
			sql:        "CREATE TABLE foo(a int REFERENCES bar(b) ON DELETE SET DEFAULT)",
			expectedOp: expect.CreateTableOp15,
		},
		{
			sql:        "CREATE TABLE foo(a int REFERENCES bar(b) ON DELETE SET DEFAULT NOT DEFERRABLE)",
			expectedOp: expect.CreateTableOp15,
		},
		{
			sql:        "CREATE TABLE foo(a int REFERENCES bar(b) ON DELETE CASCADE)",
			expectedOp: expect.CreateTableOp16,
		},
		{
			sql:        "CREATE TABLE foo(a int REFERENCES bar(b) ON DELETE CASCADE NOT DEFERRABLE)",
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
		{
			sql:        "CREATE TABLE foo(a serial PRIMARY KEY, b int DEFAULT 100 CHECK (b > 0), c text NOT NULL UNIQUE)",
			expectedOp: expect.CreateTableOp21,
		},
		{
			sql:        "CREATE TABLE foo(a serial PRIMARY KEY, b text, c text, UNIQUE (b, c))",
			expectedOp: expect.CreateTableOp22,
		},
		{
			sql:        "CREATE TABLE foo(b text, c text, UNIQUE (b) INCLUDE (c) WITH (fillfactor = 70) USING INDEX TABLESPACE my_tablespace)",
			expectedOp: expect.CreateTableOp23,
		},
		{
			sql:        "CREATE TABLE foo(a int, CHECK (a>0))",
			expectedOp: expect.CreateTableOp24,
		},
		{
			sql:        "CREATE TABLE foo(b text, c text, CHECK (b=c) NO INHERIT)",
			expectedOp: expect.CreateTableOp25,
		},
		{
			sql:        "CREATE TABLE foo(b text, c text, PRIMARY KEY (b) DEFERRABLE)",
			expectedOp: expect.CreateTableOp26,
		},
		{
			sql:        "CREATE TABLE foo(b bigint GENERATED ALWAYS AS IDENTITY (START WITH 1 INCREMENT BY 1 MINVALUE 1 MAXVALUE 1000 CYCLE CACHE 1) PRIMARY KEY)",
			expectedOp: expect.CreateTableOp27,
		},
		{
			sql:        "CREATE TABLE foo(a text, b text GENERATED ALWAYS AS (upper(a)) STORED)",
			expectedOp: expect.CreateTableOp28,
		},
		{
			sql:        "CREATE TABLE foo(a int, CONSTRAINT foo_fk FOREIGN KEY (a) REFERENCES bar(b))",
			expectedOp: expect.CreateTableOp29,
		},
		{
			sql:        "CREATE TABLE foo(a int, FOREIGN KEY (a) REFERENCES bar(b) ON DELETE SET NULL ON UPDATE CASCADE)",
			expectedOp: expect.CreateTableOp30,
		},
		{
			sql:        "CREATE TABLE foo(a int, b int, FOREIGN KEY (a, b) REFERENCES bar(c, d) ON DELETE SET NULL (b))",
			expectedOp: expect.CreateTableOp31,
		},
		{
			sql:        "CREATE TABLE foo(a int, b int, c int, FOREIGN KEY (a, b, c) REFERENCES bar(d, e, f) ON DELETE SET NULL ON UPDATE CASCADE)",
			expectedOp: expect.CreateTableOp32,
		},
		{
			sql:        "CREATE TABLE foo(a int, b int, c int, FOREIGN KEY (a, b, c) REFERENCES bar(d, e, f) MATCH FULL)",
			expectedOp: expect.CreateTableOp33,
		},
		{
			sql:        "CREATE TABLE foo(a int REFERENCES bar (b) ON UPDATE RESTRICT)",
			expectedOp: expect.CreateTableOp34(migrations.ForeignKeyMatchTypeSIMPLE, migrations.ForeignKeyActionNOACTION, migrations.ForeignKeyActionRESTRICT),
		},
		{
			sql:        "CREATE TABLE foo(a int REFERENCES bar (b) ON UPDATE CASCADE)",
			expectedOp: expect.CreateTableOp34(migrations.ForeignKeyMatchTypeSIMPLE, migrations.ForeignKeyActionNOACTION, migrations.ForeignKeyActionCASCADE),
		},
		{
			sql:        "CREATE TABLE foo(a int REFERENCES bar (b) ON UPDATE SET NULL)",
			expectedOp: expect.CreateTableOp34(migrations.ForeignKeyMatchTypeSIMPLE, migrations.ForeignKeyActionNOACTION, migrations.ForeignKeyActionSETNULL),
		},
		{
			sql:        "CREATE TABLE foo(a int REFERENCES bar (b) ON UPDATE SET DEFAULT)",
			expectedOp: expect.CreateTableOp34(migrations.ForeignKeyMatchTypeSIMPLE, migrations.ForeignKeyActionNOACTION, migrations.ForeignKeyActionSETDEFAULT),
		},
		{
			sql:        "CREATE TABLE foo(a int REFERENCES bar (b) MATCH FULL)",
			expectedOp: expect.CreateTableOp34(migrations.ForeignKeyMatchTypeFULL, migrations.ForeignKeyActionNOACTION, migrations.ForeignKeyActionNOACTION),
		},
		{
			sql:        "CREATE TABLE foo(a int REFERENCES bar (b) ON DELETE CASCADE ON UPDATE RESTRICT)",
			expectedOp: expect.CreateTableOp34(migrations.ForeignKeyMatchTypeSIMPLE, migrations.ForeignKeyActionCASCADE, migrations.ForeignKeyActionRESTRICT),
		},
		{
			sql:        "CREATE TABLE foo(a int REFERENCES bar (b) ON DELETE SET NULL ON UPDATE CASCADE)",
			expectedOp: expect.CreateTableOp34(migrations.ForeignKeyMatchTypeSIMPLE, migrations.ForeignKeyActionSETNULL, migrations.ForeignKeyActionCASCADE),
		},
		{
			sql:        "CREATE TABLE foo(a int REFERENCES bar (b) MATCH FULL ON DELETE SET DEFAULT ON UPDATE SET DEFAULT)",
			expectedOp: expect.CreateTableOp34(migrations.ForeignKeyMatchTypeFULL, migrations.ForeignKeyActionSETDEFAULT, migrations.ForeignKeyActionSETDEFAULT),
		},
		{
			sql:        "CREATE TABLE foo(id integer, s timestamptz, e timestamptz, canceled boolean DEFAULT false, EXCLUDE USING gist (id WITH =, tstzrange(s, e) WITH &&) WHERE (not canceled))",
			expectedOp: expect.CreateTableOp35,
		},
		{
			sql:        "CREATE TABLE foo(id integer, b text, EXCLUDE USING btree (id WITH =) DEFERRABLE INITIALLY DEFERRED)",
			expectedOp: expect.CreateTableOp36,
		},
	}

	for _, tc := range tests {
		t.Run(tc.sql, func(t *testing.T) {
			ops, err := sql2pgroll.Convert(tc.sql)
			require.NoError(t, err)

			fmt.Printf("%+v\n", ops[0])
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

		// Named inline constraints are not supported for DEFAULT, NULL, NOT NULL,
		// UNIQUE or PRIMARY KEY constraints
		"CREATE TABLE foo(a int CONSTRAINT foo_default DEFAULT 0)",
		"CREATE TABLE foo(a int CONSTRAINT foo_null NULL)",
		"CREATE TABLE foo(a int CONSTRAINT foo_notnull NOT NULL)",
		"CREATE TABLE foo(a int CONSTRAINT foo_unique UNIQUE)",
		"CREATE TABLE foo(a int CONSTRAINT foo_pk PRIMARY KEY)",

		// Deferrable constraints are not supported
		"CREATE TABLE foo(a int UNIQUE DEFERRABLE)",
		"CREATE TABLE foo(a int PRIMARY KEY DEFERRABLE)",
		"CREATE TABLE foo(a int REFERENCES bar(b) DEFERRABLE)",
		"CREATE TABLE foo(a int UNIQUE DEFERRABLE INITIALLY IMMEDIATE)",
		"CREATE TABLE foo(a int PRIMARY KEY DEFERRABLE INITIALLY IMMEDIATE)",
		"CREATE TABLE foo(a int REFERENCES bar(b) DEFERRABLE INITIALLY IMMEDIATE)",
		"CREATE TABLE foo(a int UNIQUE DEFERRABLE INITIALLY DEFERRED)",
		"CREATE TABLE foo(a int PRIMARY KEY DEFERRABLE INITIALLY DEFERRED)",
		"CREATE TABLE foo(a int REFERENCES bar(b) DEFERRABLE INITIALLY DEFERRED)",
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
