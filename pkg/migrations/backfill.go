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

	// Create a batcher for the table.
	b := newBatcher(table, batchSize)

	// Update each batch of rows, invoking callbacks for each one.
	for batch := 0; ; batch++ {
		for _, cb := range cbs {
			cb(int64(batch * batchSize))
		}

		if err := b.updateBatch(ctx, conn); err != nil {
			if errors.Is(err, sql.ErrNoRows) {
				break
			}
			return err
		}

		time.Sleep(batchDelay)
	}

	return nil
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
	lastValues       any
}

func newBatcher(table *schema.Table, batchSize int) *batcher {
	return &batcher{
		statementBuilder: newBatchStatementBuilder(table.Name, getIdentityColumns(table), batchSize),
		lastValues:       nil,
	}
}

func (b *batcher) updateBatch(ctx context.Context, conn db.DB) error {
	return conn.WithRetryableTransaction(ctx, func(ctx context.Context, tx *sql.Tx) error {
		// Build the query to update the next batch of rows
		query := b.statementBuilder.buildQuery(b.lastValues)

		// Execute the query to update the next batch of rows and update the last PK
		// value for the next batch
		err := tx.QueryRowContext(ctx, query).Scan(&b.lastValues)
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
func (sb *batchStatementBuilder) buildQuery(lastValues any) string {
	return fmt.Sprintf("WITH batch AS (%[1]s), update AS (%[2]s) %[3]s",
		sb.buildBatchSubQuery(lastValues),
		sb.buildUpdateBatchSubQuery(),
		sb.buildLastValueQuery())
}

// fetch the next batch of PK of rows to update
func (sb *batchStatementBuilder) buildBatchSubQuery(lastValues any) string {
	whereClause := ""
	if lastValues != nil {
		conditions := make([]string, len(sb.identityColumns))
		switch lastVals := lastValues.(type) {
		case []int64:
			for i, col := range sb.identityColumns {
				conditions[i] = fmt.Sprintf("%s > %d", col, lastVals[i])
			}
		case []string:
			for i, col := range sb.identityColumns {
				conditions[i] = fmt.Sprintf("%s > %s", col, pq.QuoteLiteral(lastVals[i]))
			}
		case []any:
			for i, col := range sb.identityColumns {
				if v, ok := lastVals[i].(int); ok {
					conditions[i] = fmt.Sprintf("%s > %d", col, v)
				} else if v, ok := lastVals[i].(string); ok {
					conditions[i] = fmt.Sprintf("%s > %s", col, pq.QuoteLiteral(v))
				} else {
					panic("unsupported type")
				}
			}
		case int64:
			conditions[0] = fmt.Sprintf("%s > %d ", sb.identityColumns[0], lastVals)
		case string:
			conditions[0] = fmt.Sprintf("%s > %s ", sb.identityColumns[0], pq.QuoteLiteral(lastVals))
		default:
			panic("unsupported type")
		}
		whereClause = "WHERE " + strings.Join(conditions, " AND ")
	}

	return fmt.Sprintf("SELECT %[1]s FROM %[2]s %[3]s ORDER BY %[1]s LIMIT %[4]d FOR NO KEY UPDATE",
		strings.Join(sb.identityColumns, ", "), sb.tableName, whereClause, sb.batchSize)
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
