// SPDX-License-Identifier: Apache-2.0

package migrations

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
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
	identityColumn := getIdentityColumn(table)
	if identityColumn == nil {
		return BackfillNotPossibleError{Table: table.Name}
	}

	// Create a batcher for the table.
	b := batcher{
		table:          table,
		identityColumn: identityColumn,
		lastValue:      nil,
		batchSize:      batchSize,
		batchDelay:     batchDelay,
	}

	// Update each batch of rows, invoking callbacks for each one.
	for batch := 0; ; batch++ {
		for _, cb := range cbs {
			cb(int64(batch * b.batchSize))
		}

		if err := b.updateBatch(ctx, conn); err != nil {
			if errors.Is(err, sql.ErrNoRows) {
				break
			}
			return err
		}

		time.Sleep(b.batchDelay)
	}

	return nil
}

// checkBackfill will return an error if the backfill operation is not supported.
func checkBackfill(table *schema.Table) error {
	col := getIdentityColumn(table)
	if col == nil {
		return BackfillNotPossibleError{Table: table.Name}
	}

	return nil
}

// getIdentityColumn will return a column suitable for use in a backfill operation.
func getIdentityColumn(table *schema.Table) *schema.Column {
	pks := table.GetPrimaryKey()
	if len(pks) == 1 {
		return pks[0]
	}

	// If there is no primary key, look for a unique not null column
	for _, col := range table.Columns {
		if col.Unique && !col.Nullable {
			return &col
		}
	}

	// no suitable column found
	return nil
}

type batcher struct {
	table          *schema.Table
	identityColumn *schema.Column
	lastValue      *string
	batchSize      int
	batchDelay     time.Duration
}

func (b *batcher) updateBatch(ctx context.Context, conn db.DB) error {
	return conn.WithRetryableTransaction(ctx, func(ctx context.Context, tx *sql.Tx) error {
		// Build the query to update the next batch of rows
		query := b.buildQuery()

		// Execute the query to update the next batch of rows and update the last PK
		// value for the next batch
		err := tx.QueryRowContext(ctx, query).Scan(&b.lastValue)
		if err != nil {
			return err
		}

		return nil
	})
}

// buildQuery builds the query used to update the next batch of rows.
func (b *batcher) buildQuery() string {
	whereClause := ""
	if b.lastValue != nil {
		whereClause = fmt.Sprintf("WHERE %s > %v", pq.QuoteIdentifier(b.identityColumn.Name), pq.QuoteLiteral(*b.lastValue))
	}

	return fmt.Sprintf(`
    WITH batch AS (
      SELECT %[1]s FROM %[2]s %[4]s ORDER BY %[1]s LIMIT %[3]d FOR NO KEY UPDATE
    ), update AS (
      UPDATE %[2]s SET %[1]s=%[2]s.%[1]s FROM batch WHERE %[2]s.%[1]s = batch.%[1]s RETURNING %[2]s.%[1]s
    )
    SELECT LAST_VALUE(%[1]s) OVER() FROM update
    `,
		pq.QuoteIdentifier(b.identityColumn.Name),
		pq.QuoteIdentifier(b.table.Name),
		b.batchSize,
		whereClause)
}
