// SPDX-License-Identifier: Apache-2.0

package migrations

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/lib/pq"

	"github.com/xataio/pgroll/pkg/db"
	"github.com/xataio/pgroll/pkg/schema"
)

// Backfill updates all rows in the given table, in batches, using the
// following algorithm:
// 1. Get the primary key column for the table.
// 2. Get the first batch of rows from the table, ordered by the primary key.
// 3. Update each row in the batch, setting the value of the primary key column to itself.
// 4. Repeat steps 2 and 3 until no more rows are returned.
func Backfill(ctx context.Context, conn db.DB, table *schema.Table, batchSize int, batchDelay time.Duration, cbs ...CallbackFn) error {
	// get the backfill column
	identityColumns := getIdentityColumns(table)
	if identityColumns == nil {
		return BackfillNotPossibleError{Table: table.Name}
	}

	total, err := getRowCount(ctx, conn, table.Name)
	if err != nil {
		return fmt.Errorf("get row count for %q: %w", table.Name, err)
	}

	// Create a batcher for the table.
	b := newBatcher(table, batchSize)

	// Update each batch of rows, invoking callbacks for each one.
	for batch := 0; ; batch++ {
		for _, cb := range cbs {
			cb(int64(batch*batchSize), total)
		}

		if err := b.updateBatch(ctx, conn); err != nil {
			if errors.Is(err, sql.ErrNoRows) {
				break
			}
			return err
		}

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(batchDelay):
		}
	}

	return nil
}

// getRowCount will attempt to get the row count for the given table. It first attempts to get an
// estimate and if that is zero, falls back to a full table scan.
func getRowCount(ctx context.Context, conn db.DB, tableName string) (int64, error) {
	// Try and get estimated row count
	var currentSchema string
	rows, err := conn.QueryContext(ctx, "select current_schema()")
	if err != nil {
		return 0, fmt.Errorf("getting current schema: %w", err)
	}
	if err := db.ScanFirstValue(rows, &currentSchema); err != nil {
		return 0, fmt.Errorf("scanning current schema: %w", err)
	}

	var total int64
	rows, err = conn.QueryContext(ctx, `
	  SELECT n_live_tup AS estimate
	  FROM pg_stat_user_tables
	  WHERE schemaname = $1 AND relname = $2`, currentSchema, tableName)
	if err != nil {
		return 0, fmt.Errorf("getting row count estimate for %q: %w", tableName, err)
	}
	if err := db.ScanFirstValue(rows, &total); err != nil {
		return 0, fmt.Errorf("scanning row count estimate for %q: %w", tableName, err)
	}
	if total > 0 {
		return total, nil
	}

	// If the estimate is zero, fall back to full count
	rows, err = conn.QueryContext(ctx, fmt.Sprintf(`SELECT count(*) from %s`, tableName))
	if err != nil {
		return 0, fmt.Errorf("getting row count for %q: %w", tableName, err)
	}
	if err := db.ScanFirstValue(rows, &total); err != nil {
		return 0, fmt.Errorf("scanning row count for %q: %w", tableName, err)
	}

	return total, nil
}

// checkBackfill will return an error if the backfill operation is not supported.
func checkBackfill(table *schema.Table) error {
	cols := getIdentityColumns(table)
	if cols == nil {
		return BackfillNotPossibleError{Table: table.Name}
	}

	return nil
}

// getIdentityColumn will return a column suitable for use in a backfill operation.
func getIdentityColumns(table *schema.Table) []string {
	pks := table.GetPrimaryKey()
	if len(pks) != 0 {
		pkNames := make([]string, len(pks))
		for i, pk := range pks {
			pkNames[i] = pk.Name
		}
		return pkNames
	}

	// If there is no primary key, look for a unique not null column
	for _, col := range table.Columns {
		if col.Unique && !col.Nullable {
			return []string{col.Name}
		}
	}

	// no suitable column found
	return nil
}

type batcher struct {
	statementBuilder *batchStatementBuilder
	lastValues       []string
}

func newBatcher(table *schema.Table, batchSize int) *batcher {
	return &batcher{
		statementBuilder: newBatchStatementBuilder(table.Name, getIdentityColumns(table), batchSize),
		lastValues:       make([]string, len(getIdentityColumns(table))),
	}
}

func (b *batcher) updateBatch(ctx context.Context, conn db.DB) error {
	return conn.WithRetryableTransaction(ctx, func(ctx context.Context, tx *sql.Tx) error {
		// Build the query to update the next batch of rows
		query := b.statementBuilder.buildQuery(b.lastValues)

		// Execute the query to update the next batch of rows and update the last PK
		// value for the next batch
		wrapper := make([]any, len(b.lastValues))
		for i := range b.lastValues {
			wrapper[i] = &b.lastValues[i]
		}
		err := tx.QueryRowContext(ctx, query).Scan(wrapper...)
		if err != nil {
			return err
		}

		return nil
	})
}

type batchStatementBuilder struct {
	tableName       string
	identityColumns []string
	batchSize       int
}

func newBatchStatementBuilder(tableName string, identityColumnNames []string, batchSize int) *batchStatementBuilder {
	quotedCols := make([]string, len(identityColumnNames))
	for i, col := range identityColumnNames {
		quotedCols[i] = pq.QuoteIdentifier(col)
	}
	return &batchStatementBuilder{
		tableName:       pq.QuoteIdentifier(tableName),
		identityColumns: quotedCols,
		batchSize:       batchSize,
	}
}

// buildQuery builds the query used to update the next batch of rows.
func (sb *batchStatementBuilder) buildQuery(lastValues []string) string {
	return fmt.Sprintf("WITH batch AS (%[1]s), update AS (%[2]s) %[3]s",
		sb.buildBatchSubQuery(lastValues),
		sb.buildUpdateBatchSubQuery(),
		sb.buildLastValueQuery())
}

// fetch the next batch of PK of rows to update
func (sb *batchStatementBuilder) buildBatchSubQuery(lastValues []string) string {
	whereClause := ""
	if len(lastValues) != 0 && lastValues[0] != "" {
		whereClause = fmt.Sprintf("WHERE (%s) > (%s)",
			strings.Join(sb.identityColumns, ", "), strings.Join(quoteLiteralList(lastValues), ", "))
	}

	return fmt.Sprintf("SELECT %[1]s FROM %[2]s %[3]s ORDER BY %[1]s LIMIT %[4]d FOR NO KEY UPDATE",
		strings.Join(sb.identityColumns, ", "), sb.tableName, whereClause, sb.batchSize)
}

func quoteLiteralList(l []string) []string {
	quoted := make([]string, len(l))
	for i, v := range l {
		quoted[i] = pq.QuoteLiteral(v)
	}
	return quoted
}

// update the rows in the batch
func (sb *batchStatementBuilder) buildUpdateBatchSubQuery() string {
	conditions := make([]string, len(sb.identityColumns))
	for i, col := range sb.identityColumns {
		conditions[i] = fmt.Sprintf("%[1]s.%[2]s = batch.%[2]s", sb.tableName, col)
	}
	updateWhereClause := "WHERE " + strings.Join(conditions, " AND ")

	setStmt := fmt.Sprintf("%[1]s = %[2]s.%[1]s", sb.identityColumns[0], sb.tableName)
	for i := 1; i < len(sb.identityColumns); i++ {
		setStmt += fmt.Sprintf(", %[1]s = %[2]s.%[1]s", sb.identityColumns[i], sb.tableName)
	}
	updateReturning := sb.tableName + "." + sb.identityColumns[0]
	for i := 1; i < len(sb.identityColumns); i++ {
		updateReturning += ", " + sb.tableName + "." + sb.identityColumns[i]
	}
	return fmt.Sprintf("UPDATE %[1]s SET %[2]s FROM batch %[3]s RETURNING %[4]s",
		sb.tableName, setStmt, updateWhereClause, updateReturning)
}

// fetch the last values of the PK column
func (sb *batchStatementBuilder) buildLastValueQuery() string {
	lastValues := make([]string, len(sb.identityColumns))
	for i, col := range sb.identityColumns {
		lastValues[i] = "LAST_VALUE(" + col + ") OVER()"
	}
	return fmt.Sprintf("SELECT %[1]s FROM update", strings.Join(lastValues, ", "))
}
