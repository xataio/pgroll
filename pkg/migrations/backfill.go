// SPDX-License-Identifier: Apache-2.0

package migrations

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	"github.com/lib/pq"
	"github.com/xataio/pgroll/pkg/schema"
)

// Backfill updates all rows in the given table, in batches, using the
// following algorithm:
// 1. Get the primary key column for the table.
// 2. Get the first batch of rows from the table, ordered by the primary key.
// 3. Update each row in the batch, setting the value of the primary key column to itself.
// 4. Repeat steps 2 and 3 until no more rows are returned.
func backfill(ctx context.Context, conn *sql.DB, table *schema.Table, cbs ...CallbackFn) error {
	// Get the primary key column for the table
	pks := table.GetPrimaryKey()
	if len(pks) != 1 {
		return errors.New("table must have a single primary key column")
	}
	pk := pks[0]

	// Create a batcher for the table.
	b := batcher{
		table:     table,
		pkColumn:  pk,
		lastPK:    nil,
		batchSize: 1000,
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
	}

	return nil
}

type batcher struct {
	table     *schema.Table
	pkColumn  *schema.Column
	lastPK    interface{}
	batchSize int
}

// updateBatch updates the next batch of rows in the table.
func (b *batcher) updateBatch(ctx context.Context, conn *sql.DB) error {
	// Start the transaction for this batch
	tx, err := conn.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	// Build the query to update the next batch of rows
	query := b.buildQuery()

	// Execute the query to update the next batch of rows and update the last PK
	// value for the next batch
	err = tx.QueryRowContext(ctx, query).Scan(&b.lastPK)
	if err != nil {
		return err
	}

	// Commit the transaction for this batch
	return tx.Commit()
}

// buildQuery builds the query used to update the next batch of rows.
func (b *batcher) buildQuery() string {
	whereClause := ""
	if b.lastPK != nil {
		whereClause = fmt.Sprintf("WHERE %s > %v", pq.QuoteIdentifier(b.pkColumn.Name), b.lastPK)
	}

	return fmt.Sprintf(`
    WITH batch AS (
      SELECT %[1]s FROM %[2]s %[4]s ORDER BY %[1]s LIMIT %[3]d FOR UPDATE
    ), update AS (
      UPDATE %[2]s SET %[1]s=%[2]s.%[1]s FROM batch WHERE %[2]s.%[1]s = batch.%[1]s RETURNING %[2]s.%[1]s
    )
    SELECT LAST_VALUE(%[1]s) OVER() FROM update
    `,
		pq.QuoteIdentifier(b.pkColumn.Name),
		pq.QuoteIdentifier(b.table.Name),
		b.batchSize,
		whereClause)
}
