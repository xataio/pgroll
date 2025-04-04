// SPDX-License-Identifier: Apache-2.0

package backfill

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	"github.com/lib/pq"
	"github.com/xataio/pgroll/pkg/backfill/templates"
	"github.com/xataio/pgroll/pkg/db"
	"github.com/xataio/pgroll/pkg/schema"
)

type Backfill struct {
	conn db.DB
	*Config
}

type CallbackFn func(done int64, total int64)

// New creates a new backfill operation with the given options. The backfill is
// not started until `Start` is invoked.
func New(conn db.DB, c *Config) *Backfill {
	b := &Backfill{
		conn:   conn,
		Config: c,
	}

	return b
}

// Start updates all rows in the given table, in batches, using the
// following algorithm:
// 1. Get the primary key column for the table.
// 2. Get the first batch of rows from the table, ordered by the primary key.
// 3. Update each row in the batch, setting the value of the primary key column to itself.
// 4. Repeat steps 2 and 3 until no more rows are returned.
func (bf *Backfill) Start(ctx context.Context, table *schema.Table) error {
	// Create a batcher for the table.
	var b batcher
	if identityColumns := getIdentityColumns(table); identityColumns != nil {
		b = &pkBatcher{
			BatchConfig: templates.BatchConfig{
				TableName:           table.Name,
				PrimaryKey:          identityColumns,
				BatchSize:           bf.batchSize,
				NeedsBackfillColumn: "_pgroll_needs_backfill",
			},
		}
	} else {
		b = &needsBackfillColumnBatcher{
			table:               table.Name,
			batchSize:           bf.batchSize,
			needsBackfillColumn: "_pgroll_needs_backfill",
		}
	}

	total, err := getRowCount(ctx, bf.conn, table.Name)
	if err != nil {
		return fmt.Errorf("get row count for %q: %w", table.Name, err)
	}

	// Update each batch of rows, invoking callbacks for each one.
	for batch := 0; ; batch++ {
		for _, cb := range bf.callbacks {
			cb(int64(batch*bf.batchSize), total)
		}

		if err := b.updateBatch(ctx, bf.conn); err != nil {
			if errors.Is(err, sql.ErrNoRows) {
				break
			}
			return err
		}

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(bf.batchDelay):
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
	defer rows.Close()

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

// A batcher is responsible for updating a batch of rows in a table.
type batcher interface {
	updateBatch(context.Context, db.DB) error
}

// pkBatcher is responsible for updating a batch of rows in a table.
// The table must have a PK or a unique column.
// It holds the state necessary to update the next batch of rows.
type pkBatcher struct {
	templates.BatchConfig
}

func (b *pkBatcher) updateBatch(ctx context.Context, conn db.DB) error {
	return conn.WithRetryableTransaction(ctx, func(ctx context.Context, tx *sql.Tx) error {
		// Build the query to update the next batch of rows
		sql, err := templates.BuildSQL(b.BatchConfig)
		if err != nil {
			return err
		}

		// Execute the query to update the next batch of rows and update the last PK
		// value for the next batch
		if b.LastValue == nil {
			b.LastValue = make([]string, len(b.PrimaryKey))
		}
		wrapper := make([]any, len(b.LastValue))
		for i := range b.LastValue {
			wrapper[i] = &b.LastValue[i]
		}
		err = tx.QueryRowContext(ctx, sql).Scan(wrapper...)
		if err != nil {
			return err
		}

		return nil
	})
}

// needsBackfillColumnBatcher is responsible for updating a batch of rows in a table
// if the table does not have a PK or a unique column.
type needsBackfillColumnBatcher struct {
	table               string
	batchSize           int
	needsBackfillColumn string
}

func (b *needsBackfillColumnBatcher) updateBatch(ctx context.Context, conn db.DB) error {
	return conn.WithRetryableTransaction(ctx, func(ctx context.Context, tx *sql.Tx) error {
		//nolint:gosec // tablenames are column names are checked
		stmt := fmt.Sprintf("UPDATE %s SET %s = true WHERE ctid IN (SELECT ctid FROM %s WHERE %s = true LIMIT %d)",
			pq.QuoteIdentifier(b.table),
			pq.QuoteIdentifier(b.needsBackfillColumn),
			pq.QuoteIdentifier(b.table),
			pq.QuoteIdentifier(b.needsBackfillColumn),
			b.batchSize)
		res, err := tx.Exec(stmt)
		if err != nil {
			return err
		}
		if count, err := res.RowsAffected(); err != nil || count == 0 {
			return sql.ErrNoRows
		}
		return nil
	})
}
