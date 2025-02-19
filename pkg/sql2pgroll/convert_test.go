// SPDX-License-Identifier: Apache-2.0

package sql2pgroll_test

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/xataio/pgroll/pkg/migrations"
	"github.com/xataio/pgroll/pkg/sql2pgroll"
)

func TestConvertToMigration(t *testing.T) {
	tests := map[string]struct {
		sql         string
		expectedOps migrations.Operations
		expectedErr bool
	}{
		"empty SQL statement": {
			sql: "",
		},
		"single SQL statement": {
			sql: "DROP TYPE t1;",
			expectedOps: migrations.Operations{
				&migrations.OpRawSQL{
					Up: "DROP TYPE t1",
				},
			},
		},
		"single multiline statement with comments": {
			sql: `CREATE TABLE t1 (
id INT, -- my id column
name TEXT -- my name column
);
`,
			expectedOps: migrations.Operations{
				&migrations.OpCreateTable{
					Name: "t1",
					Columns: []migrations.Column{
						{
							Name:     "id",
							Type:     "int",
							Nullable: true,
						},
						{
							Name:     "name",
							Type:     "text",
							Nullable: true,
						},
					},
				},
			},
		},
		"single function definition multiline with comments": {
			sql: `CREATE OR REPLACE FUNCTION check_password(uname TEXT, pass TEXT)
RETURNS BOOLEAN AS $$  -- check password for username
DECLARE passed BOOLEAN;
BEGIN
         SELECT  (pwd = $2) INTO passed
         FROM    pwds  -- from passwords table
         WHERE   username = $1; -- select password for username
         RETURN passed;
END; $$  LANGUAGE plpgsql
SECURITY DEFINER`,
			expectedOps: migrations.Operations{
				&migrations.OpRawSQL{
					Up: `CREATE OR REPLACE FUNCTION check_password(uname TEXT, pass TEXT)
RETURNS BOOLEAN AS $$  -- check password for username
DECLARE passed BOOLEAN;
BEGIN
         SELECT  (pwd = $2) INTO passed
         FROM    pwds  -- from passwords table
         WHERE   username = $1; -- select password for username
         RETURN passed;
END; $$  LANGUAGE plpgsql
SECURITY DEFINER`,
				},
			},
		},
		"multiple SQL raw migration statements": {
			sql: "DROP TYPE t1; DROP TYPE t2;",
			expectedOps: migrations.Operations{
				&migrations.OpRawSQL{
					Up: "DROP TYPE t1",
				},
				&migrations.OpRawSQL{
					Up: "DROP TYPE t2",
				},
			},
		},
		"multiple SQL migrations to raw and regular pgroll operations": {
			sql: "CREATE TABLE t1 (id INT); DROP INDEX idx1; DROP TYPE t1; ALTER TABLE t1 ADD COLUMN name TEXT;",
			expectedOps: migrations.Operations{
				&migrations.OpCreateTable{
					Name: "t1",
					Columns: []migrations.Column{
						{
							Name:     "id",
							Type:     "int",
							Nullable: true,
						},
					},
				},
				&migrations.OpDropIndex{
					Name: "idx1",
				},
				&migrations.OpRawSQL{
					Up: "DROP TYPE t1",
				},
				&migrations.OpAddColumn{
					Table: "t1",
					Column: migrations.Column{
						Name:     "name",
						Type:     "text",
						Nullable: true,
					},
					Up: sql2pgroll.PlaceHolderSQL,
				},
			},
		},
		"multiple unknown DDL statements": {
			sql: "CREATE TYPE t1 AS ENUM ('a', 'b'); CREATE DOMAIN d1 AS TEXT; CREATE SCHEMA s1; CREATE EXTENSION e1;",
			expectedOps: migrations.Operations{
				&migrations.OpRawSQL{
					Up: "CREATE TYPE t1 AS ENUM ('a', 'b')",
				},
				&migrations.OpRawSQL{
					Up: "CREATE DOMAIN d1 AS TEXT",
				},
				&migrations.OpRawSQL{
					Up: "CREATE SCHEMA s1",
				},
				&migrations.OpRawSQL{
					Up: "CREATE EXTENSION e1",
				},
			},
		},
		"multiple empty SQL statements": {
			sql: ";;",
		},
		"multiple statements with empty SQL statement": {
			sql: "CREATE TABLE t1 (id INT);; DROP TYPE t1;;",
			expectedOps: migrations.Operations{
				&migrations.OpCreateTable{
					Name: "t1",
					Columns: []migrations.Column{
						{
							Name:     "id",
							Type:     "int",
							Nullable: true,
						},
					},
				},
				&migrations.OpRawSQL{
					Up: "DROP TYPE t1",
				},
			},
		},
		"multiple multiline statments with comments": {
			sql: `DROP TYPE t1; -- drop type t1
DROP INDEX ixd1; -- drop my index
`,
			expectedOps: migrations.Operations{
				&migrations.OpRawSQL{
					Up: "DROP TYPE t1",
				},
				&migrations.OpDropIndex{
					Name: "ixd1",
				},
			},
		},
		"multiple statements with function definition multiline with comments": {
			sql: `DROP TABLE t1; DROP INDEX idx2; CREATE OR REPLACE FUNCTION check_password(uname TEXT, pass TEXT)
RETURNS BOOLEAN AS $$  -- check password for username
DECLARE passed BOOLEAN;
BEGIN
         SELECT  (pwd = $2) INTO passed
         FROM    pwds  -- from passwords table
         WHERE   username = $1; -- select password for username
         RETURN passed;
END; $$  LANGUAGE plpgsql
SECURITY DEFINER;
CREATE INDEX idx1 ON t1 (id);
CREATE TYPE t1;`,
			expectedOps: migrations.Operations{
				&migrations.OpDropTable{
					Name: "t1",
				},
				&migrations.OpDropIndex{
					Name: "idx2",
				},
				&migrations.OpRawSQL{
					Up: `CREATE OR REPLACE FUNCTION check_password(uname TEXT, pass TEXT)
RETURNS BOOLEAN AS $$  -- check password for username
DECLARE passed BOOLEAN;
BEGIN
         SELECT  (pwd = $2) INTO passed
         FROM    pwds  -- from passwords table
         WHERE   username = $1; -- select password for username
         RETURN passed;
END; $$  LANGUAGE plpgsql
SECURITY DEFINER`,
				},
				&migrations.OpCreateIndex{
					Name:    "idx1",
					Table:   "t1",
					Columns: []string{"id"},
					Method:  "btree",
				},
				&migrations.OpRawSQL{
					Up: "CREATE TYPE t1",
				},
			},
		},
		"syntax error in second statement": {
			sql:         "DROP INDEX idx1; DROP INDX idx2",
			expectedErr: true,
		},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			ops, err := sql2pgroll.Convert(tc.sql)
			if tc.expectedErr {
				assert.NotNil(t, err)
			} else {
				assert.Nil(t, err)
			}
			assert.Len(t, ops, len(tc.expectedOps))
			assert.Equal(t, tc.expectedOps, ops)
		})
	}
}
