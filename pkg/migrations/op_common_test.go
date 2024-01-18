// SPDX-License-Identifier: Apache-2.0

package migrations_test

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"testing"

	"github.com/lib/pq"
	"github.com/xataio/pgroll/pkg/migrations"
	"github.com/xataio/pgroll/pkg/roll"
	"github.com/xataio/pgroll/pkg/testutils"
	"golang.org/x/exp/maps"
	"golang.org/x/exp/slices"
)

type TestCase struct {
	name          string
	migrations    []migrations.Migration
	wantStartErr  error
	afterStart    func(t *testing.T, db *sql.DB)
	afterComplete func(t *testing.T, db *sql.DB)
	afterRollback func(t *testing.T, db *sql.DB)
}

type TestCases []TestCase

func TestMain(m *testing.M) {
	testutils.SharedTestMain(m)
}

func ExecuteTests(t *testing.T, tests TestCases) {
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			testutils.WithMigratorAndConnectionToContainer(t, func(mig *roll.Roll, db *sql.DB) {
				ctx := context.Background()

				// run all migrations except the last one
				for i := 0; i < len(tt.migrations)-1; i++ {
					if err := mig.Start(ctx, &tt.migrations[i]); err != nil {
						t.Fatalf("Failed to start migration: %v", err)
					}

					if err := mig.Complete(ctx); err != nil {
						t.Fatalf("Failed to complete migration: %v", err)
					}
				}

				// start the last migration
				err := mig.Start(ctx, &tt.migrations[len(tt.migrations)-1])
				if tt.wantStartErr != nil {
					if !errors.Is(err, tt.wantStartErr) {
						t.Fatalf("Expected error %q, got %q", tt.wantStartErr, err)
					}
					return
				}
				if err != nil {
					t.Fatalf("Failed to start migration: %v", err)
				}

				// run the afterStart hook
				if tt.afterStart != nil {
					tt.afterStart(t, db)
				}

				// roll back the migration
				if err := mig.Rollback(ctx); err != nil {
					t.Fatalf("Failed to roll back migration: %v", err)
				}

				// run the afterRollback hook
				if tt.afterRollback != nil {
					tt.afterRollback(t, db)
				}

				// re-start the last migration
				if err := mig.Start(ctx, &tt.migrations[len(tt.migrations)-1]); err != nil {
					t.Fatalf("Failed to start migration: %v", err)
				}

				// complete the last migration
				if err := mig.Complete(ctx); err != nil {
					t.Fatalf("Failed to complete migration: %v", err)
				}

				// run the afterComplete hook
				if tt.afterComplete != nil {
					tt.afterComplete(t, db)
				}
			})
		})
	}
}

// Common assertions

func ViewMustExist(t *testing.T, db *sql.DB, schema, version, view string) {
	t.Helper()
	if !viewExists(t, db, schema, version, view) {
		t.Fatalf("Expected view %q to exist", view)
	}
}

func ViewMustNotExist(t *testing.T, db *sql.DB, schema, version, view string) {
	t.Helper()
	if viewExists(t, db, schema, version, view) {
		t.Fatalf("Expected view %q to not exist", view)
	}
}

func TableMustExist(t *testing.T, db *sql.DB, schema, table string) {
	t.Helper()
	if !tableExists(t, db, schema, table) {
		t.Fatalf("Expected table %q to exist", table)
	}
}

func TableMustNotExist(t *testing.T, db *sql.DB, schema, table string) {
	t.Helper()
	if tableExists(t, db, schema, table) {
		t.Fatalf("Expected table %q to not exist", table)
	}
}

func ColumnMustExist(t *testing.T, db *sql.DB, schema, table, column string) {
	t.Helper()
	if !columnExists(t, db, schema, table, column) {
		t.Fatalf("Expected column %q to exist", column)
	}
}

func ColumnMustNotExist(t *testing.T, db *sql.DB, schema, table, column string) {
	t.Helper()
	if columnExists(t, db, schema, table, column) {
		t.Fatalf("Expected column %q to not exist", column)
	}
}

func ColumnMustHaveType(t *testing.T, db *sql.DB, schema, table, column, expectedType string) {
	t.Helper()
	if !columnHasType(t, db, schema, table, column, expectedType) {
		t.Fatalf("Expected column %q to have type %q", column, expectedType)
	}
}

func ColumnMustHaveComment(t *testing.T, db *sql.DB, schema, table, column, expectedComment string) {
	t.Helper()
	if !columnHasComment(t, db, schema, table, column, expectedComment) {
		t.Fatalf("Expected column %q to have comment %q", column, expectedComment)
	}
}

func TableMustHaveComment(t *testing.T, db *sql.DB, schema, table, expectedComment string) {
	t.Helper()
	if !tableHasComment(t, db, schema, table, expectedComment) {
		t.Fatalf("Expected table %q to have comment %q", table, expectedComment)
	}
}

func TableMustHaveColumnCount(t *testing.T, db *sql.DB, schema, table string, n int) {
	t.Helper()
	if !tableMustHaveColumnCount(t, db, schema, table, n) {
		t.Fatalf("Expected table to have %d columns", n)
	}
}

func FunctionMustExist(t *testing.T, db *sql.DB, schema, function string) {
	t.Helper()
	if !functionExists(t, db, schema, function) {
		t.Fatalf("Expected function %q to exist", function)
	}
}

func FunctionMustNotExist(t *testing.T, db *sql.DB, schema, function string) {
	t.Helper()
	if functionExists(t, db, schema, function) {
		t.Fatalf("Expected function %q to not exist", function)
	}
}

func TriggerMustNotExist(t *testing.T, db *sql.DB, schema, table, trigger string) {
	t.Helper()
	if triggerExists(t, db, schema, table, trigger) {
		t.Fatalf("Expected trigger %q to not exist", trigger)
	}
}

func TriggerMustExist(t *testing.T, db *sql.DB, schema, table, trigger string) {
	t.Helper()
	if !triggerExists(t, db, schema, table, trigger) {
		t.Fatalf("Expected trigger %q to exist", trigger)
	}
}

func CheckConstraintMustNotExist(t *testing.T, db *sql.DB, schema, table, constraint string) {
	t.Helper()
	if checkConstraintExists(t, db, schema, table, constraint) {
		t.Fatalf("Expected constraint %q to not exist", constraint)
	}
}

func CheckConstraintMustExist(t *testing.T, db *sql.DB, schema, table, constraint string) {
	t.Helper()
	if !checkConstraintExists(t, db, schema, table, constraint) {
		t.Fatalf("Expected constraint %q to exist", constraint)
	}
}

func ValidatedForeignKeyMustExist(t *testing.T, db *sql.DB, schema, table, constraint string) {
	t.Helper()
	if !foreignKeyExists(t, db, schema, table, constraint, true) {
		t.Fatalf("Expected validated foreign key %q to exist", constraint)
	}
}

func NotValidatedForeignKeyMustExist(t *testing.T, db *sql.DB, schema, table, constraint string) {
	t.Helper()
	if !foreignKeyExists(t, db, schema, table, constraint, false) {
		t.Fatalf("Expected not validated foreign key %q to exist", constraint)
	}
}

func IndexMustExist(t *testing.T, db *sql.DB, schema, table, index string) {
	t.Helper()
	if !indexExists(t, db, schema, table, index) {
		t.Fatalf("Expected index %q to exist", index)
	}
}

func IndexMustNotExist(t *testing.T, db *sql.DB, schema, table, index string) {
	t.Helper()
	if indexExists(t, db, schema, table, index) {
		t.Fatalf("Expected index %q to not exist", index)
	}
}

func ReplicaIdentityMustBe(t *testing.T, db *sql.DB, schema, table, replicaIdentity string) {
	t.Helper()

	var actualReplicaIdentity string
	err := db.QueryRow(`
    SELECT c.relreplident
    FROM pg_class c
    JOIN pg_namespace n ON n.oid = c.relnamespace
    WHERE c.relkind = 'r' -- regular table
    AND n.nspname = $1
    AND c.relname = $2;
  `, schema, table).Scan(&actualReplicaIdentity)
	if err != nil {
		t.Fatal(err)
	}

	if replicaIdentity != actualReplicaIdentity {
		t.Fatalf("Expected replica identity to be %q, got %q", replicaIdentity, actualReplicaIdentity)
	}
}

func indexExists(t *testing.T, db *sql.DB, schema, table, index string) bool {
	t.Helper()

	var exists bool
	err := db.QueryRow(`
    SELECT EXISTS (
      SELECT 1
      FROM pg_indexes
      WHERE schemaname = $1
      AND tablename = $2
      AND indexname = $3
    )`,
		schema, table, index).Scan(&exists)
	if err != nil {
		t.Fatal(err)
	}

	return exists
}

func checkConstraintExists(t *testing.T, db *sql.DB, schema, table, constraint string) bool {
	t.Helper()

	var exists bool
	err := db.QueryRow(`
    SELECT EXISTS (
      SELECT 1
      FROM pg_catalog.pg_constraint
      WHERE conrelid = $1::regclass
      AND conname = $2
      AND contype = 'c'
    )`,
		fmt.Sprintf("%s.%s", schema, table), constraint).Scan(&exists)
	if err != nil {
		t.Fatal(err)
	}

	return exists
}

func foreignKeyExists(t *testing.T, db *sql.DB, schema, table, constraint string, validated bool) bool {
	t.Helper()

	var exists bool
	err := db.QueryRow(`
    SELECT EXISTS (
      SELECT 1
      FROM pg_catalog.pg_constraint
      WHERE conrelid = $1::regclass
      AND conname = $2
      AND contype = 'f'
      AND convalidated = $3
    )`,
		fmt.Sprintf("%s.%s", schema, table), constraint, validated).Scan(&exists)
	if err != nil {
		t.Fatal(err)
	}

	return exists
}

func triggerExists(t *testing.T, db *sql.DB, schema, table, trigger string) bool {
	t.Helper()

	var exists bool
	err := db.QueryRow(`
    SELECT EXISTS (
      SELECT 1
      FROM pg_catalog.pg_trigger
      WHERE tgrelid = $1::regclass
      AND tgname = $2
    )`,
		fmt.Sprintf("%s.%s", schema, table), trigger).Scan(&exists)
	if err != nil {
		t.Fatal(err)
	}

	return exists
}

func functionExists(t *testing.T, db *sql.DB, schema, functionName string) bool {
	t.Helper()

	var exists bool
	err := db.QueryRow(`
    SELECT EXISTS (
      SELECT 1
      FROM pg_catalog.pg_proc
      WHERE proname = $1
      AND pronamespace = $2::regnamespace
    )`,
		functionName, schema).Scan(&exists)
	if err != nil {
		t.Fatal(err)
	}

	return exists
}

func tableExists(t *testing.T, db *sql.DB, schema, table string) bool {
	t.Helper()

	var exists bool
	err := db.QueryRow(`
		SELECT EXISTS (
			SELECT 1
			FROM pg_catalog.pg_tables
			WHERE schemaname = $1
			AND tablename = $2
		)`,
		schema, table).Scan(&exists)
	if err != nil {
		t.Fatal(err)
	}

	return exists
}

func tableMustHaveColumnCount(t *testing.T, db *sql.DB, schema, table string, n int) bool {
	t.Helper()

	var count int
	err := db.QueryRow(`
    SELECT COUNT(*)
    FROM information_schema.columns
    WHERE table_schema = $1
    AND table_name = $2`,
		schema, table).Scan(&count)
	if err != nil {
		t.Fatal(err)
	}

	return count == n
}

func viewExists(t *testing.T, db *sql.DB, schema, version, view string) bool {
	t.Helper()
	versionSchema := roll.VersionedSchemaName(schema, version)
	var exists bool
	err := db.QueryRow(`
		SELECT EXISTS (
			SELECT 1
			FROM pg_catalog.pg_views
			WHERE schemaname = $1
			AND viewname = $2
		)`,
		versionSchema, view).Scan(&exists)
	if err != nil {
		t.Fatal(err)
	}
	return exists
}

func columnExists(t *testing.T, db *sql.DB, schema, table, column string) bool {
	t.Helper()

	var exists bool
	err := db.QueryRow(`
    SELECT EXISTS (
      SELECT 1
      FROM information_schema.columns
      WHERE table_schema = $1
      AND table_name = $2
      AND column_name = $3
    )`,
		schema, table, column).Scan(&exists)
	if err != nil {
		t.Fatal(err)
	}

	return exists
}

func columnHasType(t *testing.T, db *sql.DB, schema, table, column, expectedType string) bool {
	t.Helper()

	var actualType string
	err := db.QueryRow(`
    SELECT data_type
    FROM information_schema.columns
    WHERE table_schema = $1
    AND table_name = $2
    AND column_name = $3
  `,
		schema, table, column).Scan(&actualType)
	if err != nil {
		t.Fatal(err)
	}

	return expectedType == actualType
}

func columnHasComment(t *testing.T, db *sql.DB, schema, table, column, expectedComment string) bool {
	t.Helper()

	var actualComment string
	err := db.QueryRow(fmt.Sprintf(`
    SELECT col_description(
      %[1]s::regclass,
      (SELECT attnum FROM pg_attribute WHERE attname=%[2]s and attrelid=%[1]s::regclass)
    )`,
		pq.QuoteLiteral(fmt.Sprintf("%s.%s", schema, table)),
		pq.QuoteLiteral(column)),
	).Scan(&actualComment)
	if err != nil {
		t.Fatal(err)
	}

	return expectedComment == actualComment
}

func tableHasComment(t *testing.T, db *sql.DB, schema, table, expectedComment string) bool {
	t.Helper()

	var actualComment string
	err := db.QueryRow(fmt.Sprintf(`
    SELECT obj_description(%[1]s::regclass, 'pg_class')`,
		pq.QuoteLiteral(fmt.Sprintf("%s.%s", schema, table))),
	).Scan(&actualComment)
	if err != nil {
		t.Fatal(err)
	}

	return expectedComment == actualComment
}

func MustInsert(t *testing.T, db *sql.DB, schema, version, table string, record map[string]string) {
	t.Helper()

	if err := insert(t, db, schema, version, table, record); err != nil {
		t.Fatal(err)
	}
}

func MustNotInsert(t *testing.T, db *sql.DB, schema, version, table string, record map[string]string, errorCode string) {
	t.Helper()

	err := insert(t, db, schema, version, table, record)
	if err == nil {
		t.Fatal("Expected INSERT to fail")
	}

	var pqErr *pq.Error
	if ok := errors.As(err, &pqErr); ok {
		if pqErr.Code.Name() != errorCode {
			t.Fatalf("Expected INSERT to fail with %q, got %q", errorCode, pqErr.Code.Name())
		}
	} else {
		t.Fatalf("INSERT failed with unknown error: %v", err)
	}
}

func insert(t *testing.T, db *sql.DB, schema, version, table string, record map[string]string) error {
	t.Helper()
	versionSchema := roll.VersionedSchemaName(schema, version)

	mustSetSearchPath(t, db, versionSchema)

	cols := maps.Keys(record)
	slices.Sort(cols)

	recordStr := "("
	for i, c := range cols {
		if i > 0 {
			recordStr += ", "
		}
		recordStr += c
	}
	recordStr += ") VALUES ("
	for i, c := range cols {
		if i > 0 {
			recordStr += ", "
		}
		recordStr += fmt.Sprintf("'%s'", record[c])
	}
	recordStr += ")"

	//nolint:gosec // this is a test so we don't care about SQL injection
	stmt := fmt.Sprintf("INSERT INTO %s.%s %s", versionSchema, table, recordStr)

	_, err := db.Exec(stmt)
	return err
}

func MustDelete(t *testing.T, db *sql.DB, schema, version, table string, record map[string]string) {
	t.Helper()

	if err := delete(t, db, schema, version, table, record); err != nil {
		t.Fatal(err)
	}
}

func delete(t *testing.T, db *sql.DB, schema, version, table string, record map[string]string) error {
	t.Helper()
	versionSchema := roll.VersionedSchemaName(schema, version)

	cols := maps.Keys(record)
	slices.Sort(cols)

	recordStr := ""
	for i, c := range cols {
		if i > 0 {
			recordStr += " AND "
		}
		recordStr += fmt.Sprintf("%s = '%s'", c, record[c])
	}

	//nolint:gosec // this is a test so we don't care about SQL injection
	stmt := fmt.Sprintf("DELETE FROM %s.%s WHERE %s", versionSchema, table, recordStr)

	_, err := db.Exec(stmt)
	return err
}

func MustSelect(t *testing.T, db *sql.DB, schema, version, table string) []map[string]any {
	t.Helper()
	versionSchema := roll.VersionedSchemaName(schema, version)

	//nolint:gosec // this is a test so we don't care about SQL injection
	selectStmt := fmt.Sprintf("SELECT * FROM %s.%s", versionSchema, table)

	q, err := db.Query(selectStmt)
	if err != nil {
		t.Fatal(err)
	}

	res := make([]map[string]any, 0)

	for q.Next() {
		cols, err := q.Columns()
		if err != nil {
			t.Fatal(err)
		}
		values := make([]any, len(cols))
		valuesPtr := make([]any, len(cols))
		for i := range values {
			valuesPtr[i] = &values[i]
		}
		if err := q.Scan(valuesPtr...); err != nil {
			t.Fatal(err)
		}

		row := map[string]any{}
		for i, col := range cols {
			// avoid having to cast int literals to int64 in tests
			if v, ok := values[i].(int64); ok {
				values[i] = int(v)
			}
			row[col] = values[i]
		}

		res = append(res, row)
	}

	return res
}

func mustSetSearchPath(t *testing.T, db *sql.DB, schema string) {
	t.Helper()

	_, err := db.Exec(fmt.Sprintf("SET search_path = %s", pq.QuoteIdentifier(schema)))
	if err != nil {
		t.Fatal(err)
	}
}

func ptr[T any](x T) *T { return &x }
