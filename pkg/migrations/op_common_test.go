// SPDX-License-Identifier: Apache-2.0

package migrations_test

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"maps"
	"os"
	"slices"
	"strconv"
	"strings"
	"testing"

	"github.com/lib/pq"
	"github.com/stretchr/testify/assert"

	"github.com/xataio/pgroll/internal/testutils"
	"github.com/xataio/pgroll/pkg/backfill"
	"github.com/xataio/pgroll/pkg/migrations"
	"github.com/xataio/pgroll/pkg/roll"
)

type TestCase struct {
	name              string
	minPgMajorVersion int
	migrations        []migrations.Migration
	wantStartErr      error
	wantRollbackErr   error
	wantCompleteErr   error
	afterStart        func(t *testing.T, db *sql.DB, schema string)
	afterComplete     func(t *testing.T, db *sql.DB, schema string)
	afterRollback     func(t *testing.T, db *sql.DB, schema string)
}

type TestCases []TestCase

func TestMain(m *testing.M) {
	testutils.SharedTestMain(m)
}

func ExecuteTests(t *testing.T, tests TestCases, opts ...roll.Option) {
	testSchema := testutils.TestSchema()

	for _, tt := range tests {
		if isTestSkipped(t, tt.minPgMajorVersion) {
			t.Skipf("Skipping test %q for PostgreSQL version %s", tt.name, os.Getenv("POSTGRES_VERSION"))
		}

		t.Run(tt.name, func(t *testing.T) {
			testutils.WithMigratorInSchemaAndConnectionToContainerWithOptions(t, testSchema, opts, func(mig *roll.Roll, db *sql.DB) {
				ctx := context.Background()
				config := backfill.NewConfig()

				// run all migrations except the last one
				for i := 0; i < len(tt.migrations)-1; i++ {
					if err := mig.Start(ctx, &tt.migrations[i], config); err != nil {
						t.Fatalf("Failed to start migration: %v", err)
					}

					if err := mig.Complete(ctx); err != nil {
						t.Fatalf("Failed to complete migration: %v", err)
					}
				}

				// start the last migration
				err := mig.Start(ctx, &tt.migrations[len(tt.migrations)-1], config)
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
					tt.afterStart(t, db, testSchema)
				}

				// roll back the migration
				err = mig.Rollback(ctx)
				if tt.wantRollbackErr != nil {
					if !errors.Is(err, tt.wantRollbackErr) {
						t.Fatalf("Expected error %q, got %q", tt.wantRollbackErr, err)
					}
					return
				}
				if err != nil {
					t.Fatalf("Failed to roll back migration: %v", err)
				}

				// run the afterRollback hook
				if tt.afterRollback != nil {
					tt.afterRollback(t, db, testSchema)
				}

				// re-start the last migration
				if err := mig.Start(ctx, &tt.migrations[len(tt.migrations)-1], config); err != nil {
					t.Fatalf("Failed to start migration: %v", err)
				}

				// complete the last migration
				err = mig.Complete(ctx)
				if tt.wantCompleteErr != nil {
					if !errors.Is(err, tt.wantCompleteErr) {
						t.Fatalf("Expected error %q, got %q", tt.wantCompleteErr, err)
					}
					return
				}
				if err != nil {
					t.Fatalf("Failed to complete migration: %v", err)
				}

				// run the afterComplete hook
				if tt.afterComplete != nil {
					tt.afterComplete(t, db, testSchema)
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
	if !columnHasComment(t, db, schema, table, column, &expectedComment) {
		t.Fatalf("Expected column %q to have comment %q", column, expectedComment)
	}
}

func ColumnMustNotHaveComment(t *testing.T, db *sql.DB, schema, table, column string) {
	t.Helper()
	if !columnHasComment(t, db, schema, table, column, nil) {
		t.Fatalf("Expected column %q to have no comment", column)
	}
}

func ColumnMustHaveDefault(t *testing.T, db *sql.DB, schema, table, column, expectedDefault string) {
	t.Helper()
	if !columnHasDefault(t, db, schema, table, column, &expectedDefault) {
		t.Fatalf("Expected column %q to have default value %q", column, expectedDefault)
	}
}

func ColumnMustBePK(t *testing.T, db *sql.DB, schema, table, column string) {
	t.Helper()
	if !columnMustBePK(t, db, schema, table, column) {
		t.Fatalf("Expected column %q to be primary key", column)
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

func CheckConstraintMustNotExist(t *testing.T, db *sql.DB, schema, table, constraint string) {
	t.Helper()
	if checkConstraintExists(t, db, schema, table, constraint, false) {
		t.Fatalf("Expected constraint %q to not exist", constraint)
	}
}

func CheckConstraintMustExist(t *testing.T, db *sql.DB, schema, table, constraint string) {
	t.Helper()
	if !checkConstraintExists(t, db, schema, table, constraint, false) {
		t.Fatalf("Expected constraint %q to exist", constraint)
	}
}

func NotInheritableCheckConstraintMustExist(t *testing.T, db *sql.DB, schema, table, constraint string) {
	t.Helper()
	if !checkConstraintExists(t, db, schema, table, constraint, true) {
		t.Fatalf("Expected constraint %q to exist", constraint)
	}
}

func UniqueConstraintMustExist(t *testing.T, db *sql.DB, schema, table, constraint string) {
	t.Helper()
	if !uniqueConstraintExists(t, db, schema, table, constraint) {
		t.Fatalf("Expected unique constraint %q to exist", constraint)
	}
}

func ValidatedForeignKeyMustExist(t *testing.T, db *sql.DB, schema, table, constraint string) {
	t.Helper()
	if !foreignKeyExists(t, db, schema, table, constraint, true, migrations.ForeignKeyActionNOACTION, migrations.ForeignKeyActionNOACTION) {
		t.Fatalf("Expected validated foreign key %q to exist", constraint)
	}
}

func ValidatedForeignKeyMustExistWithReferentialAction(t *testing.T, db *sql.DB, schema, table, constraint string, onDelete, onUpdate migrations.ForeignKeyAction) {
	t.Helper()
	if !foreignKeyExists(t, db, schema, table, constraint, true, onDelete, onUpdate) {
		t.Fatalf("Expected validated foreign key %q to exist", constraint)
	}
}

func NotValidatedForeignKeyMustExist(t *testing.T, db *sql.DB, schema, table, constraint string) {
	t.Helper()
	if !foreignKeyExists(t, db, schema, table, constraint, false, migrations.ForeignKeyActionNOACTION, migrations.ForeignKeyActionNOACTION) {
		t.Fatalf("Expected not validated foreign key %q to exist", constraint)
	}
}

func NotValidatedForeignKeyMustExistWithReferentialAction(t *testing.T, db *sql.DB, schema, table, constraint string, onDelete, onUpdate migrations.ForeignKeyAction) {
	t.Helper()
	if !foreignKeyExists(t, db, schema, table, constraint, false, onDelete, onUpdate) {
		t.Fatalf("Expected not validated foreign key %q to exist", constraint)
	}
}

func TableForeignKeyMustExist(t *testing.T, db *sql.DB, schema, table, constraint string, deferrable, initiallyDeferred bool) {
	t.Helper()
	if !tableForeignKeyExists(t, db, schema, table, constraint, deferrable, initiallyDeferred) {
		t.Fatalf("Expected table foreign key %q to exist", constraint)
	}
}

func PrimaryKeyConstraintMustExist(t *testing.T, db *sql.DB, schema, table, constraint string) {
	t.Helper()
	if !primaryKeyConstraintExists(t, db, schema, table, constraint) {
		t.Fatalf("Expected constraint %q to exist", constraint)
	}
}

func ExcludeConstraintMustExist(t *testing.T, db *sql.DB, schema, table, constraint string) {
	t.Helper()
	if !excludeConstraintExists(t, db, schema, table, constraint) {
		t.Fatalf("Expected constraint %q to exist", constraint)
	}
}

func IndexMustExist(t *testing.T, db *sql.DB, schema, table, index string) {
	t.Helper()
	if !indexExists(t, db, schema, table, index) {
		t.Fatalf("Expected index %q to exist", index)
	}
}

func IndexDescendingMustExist(t *testing.T, db *sql.DB, schema, table, index string, columnIdx int) {
	t.Helper()
	if !indexDescendingExists(t, db, schema, table, index, columnIdx) {
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

func indexDescendingExists(t *testing.T, db *sql.DB, schema, table, index string, columnIdx int) bool {
	t.Helper()

	var flags []uint8
	err := db.QueryRow(`
    SELECT indoption
    FROM pg_index
    WHERE indrelid = $1::regclass
    AND indexrelid = $2::regclass`,
		fmt.Sprintf("%s.%s", schema, table), fmt.Sprintf("%s.%s", schema, index)).Scan(&flags)
	if err != nil {
		t.Fatal(err)
	}

	// check if index is descending using the 1st bit of the flags
	indoptionDesc := uint8(1)
	return flags[columnIdx]&indoptionDesc == 1
}

func CheckIndexDefinition(t *testing.T, db *sql.DB, schema, table, index, expectedDefinition string) {
	t.Helper()

	var actualDef string
	err := db.QueryRow(`
      SELECT indexdef
      FROM pg_indexes
      WHERE schemaname = $1
      AND tablename = $2
      AND indexname = $3
    `,
		schema, table, index).Scan(&actualDef)
	if err != nil {
		t.Fatal(err)
	}

	if expectedDefinition != actualDef {
		t.Fatalf("Expected index %q to have definition %q, got %q", index, expectedDefinition, actualDef)
	}
}

func checkConstraintExists(t *testing.T, db *sql.DB, schema, table, constraint string, noInherit bool) bool {
	t.Helper()

	var exists bool
	err := db.QueryRow(`
    SELECT EXISTS (
      SELECT 1
      FROM pg_catalog.pg_constraint
      WHERE conrelid = $1::regclass
      AND conname = $2
      AND contype = 'c'
      AND connoinherit = $3
    )`,
		fmt.Sprintf("%s.%s", schema, table), constraint, noInherit).Scan(&exists)
	if err != nil {
		t.Fatal(err)
	}

	return exists
}

func uniqueConstraintExists(t *testing.T, db *sql.DB, schema, table, constraint string) bool {
	t.Helper()

	var exists bool
	err := db.QueryRow(`
    SELECT EXISTS (
      SELECT 1
      FROM pg_catalog.pg_constraint
      WHERE conrelid = $1::regclass
      AND conname = $2
      AND contype = 'u'
    )`,
		fmt.Sprintf("%s.%s", schema, table), constraint).Scan(&exists)
	if err != nil {
		t.Fatal(err)
	}

	return exists
}

func referentialAction(a migrations.ForeignKeyAction) string {
	switch a {
	case migrations.ForeignKeyActionNOACTION:
		return "a"
	case migrations.ForeignKeyActionRESTRICT:
		return "r"
	case migrations.ForeignKeyActionSETNULL:
		return "n"
	case migrations.ForeignKeyActionSETDEFAULT:
		return "d"
	case migrations.ForeignKeyActionCASCADE:
		return "c"
	default:
		return "a"
	}
}

func foreignKeyExists(t *testing.T, db *sql.DB, schema, table, constraint string, validated bool, onDeleteAction, onUpdateAction migrations.ForeignKeyAction) bool {
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
      AND confdeltype = $4
      AND confupdtype = $5
    )`,
		fmt.Sprintf("%s.%s", schema, table), constraint, validated, referentialAction(onDeleteAction), referentialAction(onUpdateAction)).Scan(&exists)
	if err != nil {
		t.Fatal(err)
	}

	return exists
}

func tableForeignKeyExists(t *testing.T, db *sql.DB, schema, table, constraint string, deferrable, initiallyDeferred bool) bool {
	t.Helper()

	deferrableStr := "NO"
	if deferrable {
		deferrableStr = "YES"
	}
	initiallyDeferredStr := "NO"
	if initiallyDeferred {
		initiallyDeferredStr = "YES"
	}

	var exists bool
	err := db.QueryRow(`
    SELECT EXISTS (
      SELECT 1
      FROM information_schema.table_constraints
      WHERE table_schema = $1
      AND table_name = $2
      AND constraint_name = $3
      AND constraint_type = 'FOREIGN KEY'
      AND is_deferrable = $4
      AND initially_deferred = $5
    )`,
		schema, table, constraint, deferrableStr, initiallyDeferredStr).Scan(&exists)
	if err != nil {
		t.Fatal(err)
	}

	return exists
}

func primaryKeyConstraintExists(t *testing.T, db *sql.DB, schema, table, constraint string) bool {
	t.Helper()

	var exists bool
	err := db.QueryRow(`
    SELECT EXISTS (
      SELECT 1
      FROM pg_catalog.pg_constraint
      WHERE conrelid = $1::regclass
      AND conname = $2
      AND contype = 'p'
    )`,
		fmt.Sprintf("%s.%s", schema, table), constraint).Scan(&exists)
	if err != nil {
		t.Fatal(err)
	}

	return exists
}

func excludeConstraintExists(t *testing.T, db *sql.DB, schema, table, constraint string) bool {
	t.Helper()

	var exists bool
	err := db.QueryRow(`
    SELECT EXISTS (
      SELECT 1
      FROM pg_catalog.pg_constraint
      WHERE conrelid = $1::regclass
      AND conname = $2
      AND contype = 'x'
    )`,
		fmt.Sprintf("%s.%s", schema, table), constraint).Scan(&exists)
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
		fmt.Sprintf("%s.%s", pq.QuoteIdentifier(schema), pq.QuoteIdentifier(table)), trigger).Scan(&exists)
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

func columnHasComment(t *testing.T, db *sql.DB, schema, table, column string, expectedComment *string) bool {
	t.Helper()

	var actualComment *string
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

	if expectedComment == nil {
		return actualComment == nil
	}
	return actualComment != nil && *expectedComment == *actualComment
}

func columnHasDefault(t *testing.T, db *sql.DB, schema, table, column string, expectedDefault *string) bool {
	t.Helper()

	var actualDefault *string
	err := db.QueryRow(`
    SELECT column_default
    FROM information_schema.columns
    WHERE table_schema = $1
    AND table_name = $2
    AND column_name = $3
  `,
		schema, table, column).Scan(&actualDefault)
	if err != nil {
		t.Fatal(err)
	}
	if expectedDefault == nil {
		return actualDefault == nil
	}
	return actualDefault != nil && *expectedDefault == *actualDefault
}

func columnMustBePK(t *testing.T, db *sql.DB, schema, table, column string) bool {
	t.Helper()

	var exists bool
	err := db.QueryRow(fmt.Sprintf(`
    SELECT EXISTS (
	  SELECT a.attname
      FROM   pg_index i
      JOIN   pg_attribute a ON a.attrelid = i.indrelid
                      AND a.attnum = ANY(i.indkey)
      WHERE  i.indrelid = %[1]s::regclass AND i.indisprimary AND a.attname = %[2]s
    )`,
		pq.QuoteLiteral(fmt.Sprintf("%s.%s", schema, table)),
		pq.QuoteLiteral(column)),
	).Scan(&exists)
	if err != nil {
		t.Fatal(err)
	}

	return exists
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

func MustUpdate(t *testing.T, db *sql.DB, schema, version, table, column, value string, record map[string]string) {
	t.Helper()

	if err := update(t, db, schema, version, table, column, value, record); err != nil {
		t.Fatal(err)
	}
}

func update(t *testing.T, db *sql.DB, schema, version, table, column, value string, record map[string]string) error {
	t.Helper()
	versionSchema := roll.VersionedSchemaName(schema, version)

	mustSetSearchPath(t, db, versionSchema)

	cols := slices.Collect(maps.Keys(record))
	slices.Sort(cols)

	recordStr := "SET "
	for i, c := range cols {
		if i > 0 {
			recordStr += ", "
		}
		recordStr += c + "=" + record[c]
	}
	recordStr += " WHERE " + column + "=" + value

	//nolint:gosec // this is a test so we don't care about SQL injection
	stmt := fmt.Sprintf("UPDATE %s.%s %s", versionSchema, table, recordStr)

	_, err := db.Exec(stmt)
	return err
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

	cols := slices.Collect(maps.Keys(record))
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
		if record[c] == "NULL" {
			recordStr += record[c]
		} else {
			recordStr += fmt.Sprintf("'%s'", record[c])
		}
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

func MustNotDelete(t *testing.T, db *sql.DB, schema, version, table string, record map[string]string, errorCode string) {
	t.Helper()

	err := delete(t, db, schema, version, table, record)
	if err == nil {
		t.Fatal("Expected DELETE to fail")
	}

	var pqErr *pq.Error
	if ok := errors.As(err, &pqErr); ok {
		if pqErr.Code.Name() != errorCode {
			t.Fatalf("Expected DELETE to fail with %q, got %q", errorCode, pqErr.Code.Name())
		}
	} else {
		t.Fatalf("DELETE failed with unknown error: %v", err)
	}
}

func delete(t *testing.T, db *sql.DB, schema, version, table string, record map[string]string) error {
	t.Helper()
	versionSchema := roll.VersionedSchemaName(schema, version)

	cols := slices.Collect(maps.Keys(record))
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
	assert.NoError(t, q.Err())

	return res
}

// TableMustBeCleanedUp asserts that the `columns` on `table` in `schema` have
// been cleaned up after migration rollback or completion. This means:
// - The temporary columns should not exist on the underlying table.
// - The up functions for the columns no longer exist.
// - The down functions for the columns no longer exist.
// - The up triggers for the columns no longer exist.
// - The down triggers for the columns no longer exist.
// - The _pgroll_needs_backfill column should not exist on the table.
func TableMustBeCleanedUp(t *testing.T, db *sql.DB, schema, table string, columns ...string) {
	t.Helper()

	for _, column := range columns {
		// The temporary column should not exist on the underlying table.
		ColumnMustNotExist(t, db, schema, table, migrations.TemporaryName(column))

		// The _pgroll_needs_backfill column should not exist on the table.
		ColumnMustNotExist(t, db, schema, table, backfill.CNeedsBackfillColumn)

		// The up function for the column no longer exists.
		FunctionMustNotExist(t, db, schema, migrations.TriggerFunctionName(table, column))
		// The down function for the column no longer exists.
		FunctionMustNotExist(t, db, schema, migrations.TriggerFunctionName(table, migrations.TemporaryName(column)))

		// The up trigger for the column no longer exists.
		TriggerMustNotExist(t, db, schema, table, migrations.TriggerName(table, column))
		// The down trigger for the column no longer exists.
		TriggerMustNotExist(t, db, schema, table, migrations.TriggerName(table, migrations.TemporaryName(column)))
	}
}

func mustSetSearchPath(t *testing.T, db *sql.DB, schema string) {
	t.Helper()

	_, err := db.Exec(fmt.Sprintf("SET search_path = %s", pq.QuoteIdentifier(schema)))
	if err != nil {
		t.Fatal(err)
	}
}

func isTestSkipped(t *testing.T, minPgMajorVersion int) bool {
	if minPgMajorVersion == 0 || os.Getenv("POSTGRES_VERSION") == "" || os.Getenv("POSTGRES_VERSION") == "latest" {
		return false
	}
	pgMajorVersion := strings.Split(os.Getenv("POSTGRES_VERSION"), ".")[0]
	version, err := strconv.Atoi(pgMajorVersion)
	if err != nil {
		return false
	}
	return version < minPgMajorVersion
}

func ptr[T any](x T) *T { return &x }
