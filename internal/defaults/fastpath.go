// SPDX-License-Identifier: Apache-2.0

package defaults

import (
	"context"
	"fmt"

	"github.com/lib/pq"
	"github.com/xataio/pgroll/pkg/db"
)

const (
	cNewTableName  = "_pgroll_temp_fastpath_check"
	cNewColumnName = "_pgroll_fastpath_check_column"
)

// UsesFastPath returns true if [defaultExpr] will use the fast-path
// optimization added in Postgres 11 to avoid taking an `ACCESS_EXCLUSIVE` lock
// when adding a new column with a `DEFAULT` value.
//
// The implementation works by creating a schema-only copy of [tableName],
// adding a new column with a `DEFAULT` value of [defaultExpr] and checking
// system catalogs to see if the fast-path optimization was applied.
func UsesFastPath(ctx context.Context, conn db.DB, tableName, columnType, defaultExpr string) (bool, error) {
	// Check if we have a real connection or a fake one
	if _, ok := conn.(*db.FakeDB); ok {
		return true, nil
	}

	// Create a schema-only copy of the table
	_, err := conn.ExecContext(ctx, fmt.Sprintf("CREATE UNLOGGED TABLE %s AS SELECT * FROM %s WHERE false",
		pq.QuoteIdentifier(cNewTableName),
		pq.QuoteIdentifier(tableName)))
	if err != nil {
		return false, fmt.Errorf("failed to create schema-only copy of table: %w", err)
	}

	// Ensure that the schema-only copy is removed
	defer cleanup(ctx, conn)

	// Add a new column with the default value
	_, err = conn.ExecContext(ctx, fmt.Sprintf("ALTER TABLE %s ADD COLUMN %s %s DEFAULT %s",
		pq.QuoteIdentifier(cNewTableName),
		pq.QuoteIdentifier(cNewColumnName),
		columnType,
		defaultExpr))
	if err != nil {
		return false, fmt.Errorf("failed to add column to schema-only copy: %w", err)
	}

	// Inspect the system catalogs to see if the fast-path optimization was applied
	rows, err := conn.QueryContext(ctx,
		"SELECT atthasmissing FROM pg_attribute WHERE attrelid::regclass = $1::regclass AND attname = $2",
		cNewTableName,
		cNewColumnName)
	if err != nil {
		return false, fmt.Errorf("failed to query pg_attribute: %w", err)
	}
	defer rows.Close()

	// Read the `attmissing` column from the result to determine if the fast-path
	// optimization was applied
	var hasMissing bool
	if err := db.ScanFirstValue(rows, &hasMissing); err != nil {
		return false, fmt.Errorf("failed to read pg_attribute result: %w", err)
	}

	return hasMissing, nil
}

func cleanup(ctx context.Context, conn db.DB) {
	conn.ExecContext(ctx, fmt.Sprintf("DROP TABLE IF EXISTS %s", pq.QuoteIdentifier(cNewTableName)))
}
