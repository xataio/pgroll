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
	conn             db.DB
	stateSchema      string
	batchSize        int
	batchDelay       time.Duration
	batchTablePrefix string
	callbacks        []CallbackFn
}

type CallbackFn func(done int64, total int64)

// New creates a new backfill operation with the given options. The backfill is
// not started until `Start` is invoked.
func New(conn db.DB, opts ...OptionFn) *Backfill {
	b := &Backfill{
		conn:             conn,
		batchSize:        1000,
		stateSchema:      "pgroll",
		batchTablePrefix: "batch_",
	}

	for _, opt := range opts {
		opt(b)
	}

	return b
}

// Start backfills all rows in the given table, in batches, using the following
// algorithm:
//
// 1. Begin a REPEATABLE READ transaction and take a transaction snapshot. This
// transaction remains open for the duration of the backfill so that other
// transactions can use the snapshot.
// Then for each batch:
// 2. The primary key values for each batch of rows to be updated is INSERTed
// INTO a table. The transaction that does the INSERT INTO uses the snapshot
// taken in step 1 so that only rows present at the start of the backfill are
// visible.
// 3. The batch of rows is updated in the table being backfilled by setting
// their primary keys to themselves (a no-op update). This update causes any ON
// UPDATE trigger to fire for the affected rows.
func (bf *Backfill) Start(ctx context.Context, table *schema.Table) error {
	// Get the columns to use as the identity columns for the backfill
	identityColumns := getIdentityColumns(table)
	if identityColumns == nil {
		return NotPossibleError{Table: table.Name}
	}

	// Get the total number of rows in the table
	total, err := getRowCount(ctx, bf.conn, table.Name)
	if err != nil {
		return fmt.Errorf("get row count for %q: %w", table.Name, err)
	}

	// Begin a REPEATABLE READ transaction. The transaction is used to take a
	// snapshot that is passed to each batch select operation. This ensures that
	// each batch selection acts only on rows visible at the start of the
	// backfill process.
	tx, err := bf.conn.RawConn().BeginTx(ctx, &sql.TxOptions{
		Isolation: sql.LevelRepeatableRead,
	})
	if err != nil {
		return fmt.Errorf("begin transaction: %w", err)
	}
	defer tx.Rollback()

	// Create a transaction snapshot
	snapshotID, err := getSnapshotID(ctx, tx)
	if err != nil {
		return fmt.Errorf("get snapshot ID: %w", err)
	}

	// Create a table to store the primary key values of each batch of rows
	err = bf.createBatchTable(ctx, bf.conn, table.Name, identityColumns)
	if err != nil {
		return fmt.Errorf("create batch table: %w", err)
	}
	defer bf.dropBatchTable(ctx, bf.conn, table.Name)

	// Create a batcher for the table.
	b := batcher{
		BatchConfig: templates.BatchConfig{
			TableName:        table.Name,
			PrimaryKey:       identityColumns,
			BatchSize:        bf.batchSize,
			SnapshotID:       snapshotID,
			StateSchema:      bf.stateSchema,
			BatchTablePrefix: bf.batchTablePrefix,
		},
	}

	// Update each batch of rows, invoking callbacks for each one.
	for batch := 0; ; batch++ {
		for _, cb := range bf.callbacks {
			cb(int64(batch*bf.batchSize), total)
		}

		// Insert the (primary keys of) the next batch of rows to be updated into
		// the batch table
		err := b.selectBatch(ctx, bf.conn)
		if err != nil {
			if errors.Is(err, sql.ErrNoRows) {
				break
			}
			return fmt.Errorf("select batch: %w", err)
		}

		// Update the batch of rows
		err = b.updateBatch(ctx, bf.conn)
		if err != nil {
			return fmt.Errorf("update batch: %w", err)
		}

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(bf.batchDelay):
		}
	}

	return nil
}

// createBatchTable creates the table used to store the primary key values of each
// batch of rows to be updated during the backfill process.
func (bf *Backfill) createBatchTable(ctx context.Context, conn db.DB, tableName string, idColumns []string) error {
	// Drop the batch table if it already exists
	_, err := conn.ExecContext(ctx, fmt.Sprintf("DROP TABLE IF EXISTS %s.%s",
		pq.QuoteIdentifier(bf.stateSchema),
		pq.QuoteIdentifier(bf.batchTablePrefix+tableName)))
	if err != nil {
		return err
	}

	// Build the query to create the batch table
	sql, err := templates.BuildCreateBatchTable(templates.CreateBatchTableConfig{
		StateSchema:      bf.stateSchema,
		BatchTablePrefix: bf.batchTablePrefix,
		TableName:        tableName,
		IDColumns:        idColumns,
	})
	if err != nil {
		return err
	}

	// Execute the query to create the batch table
	_, err = conn.ExecContext(ctx, sql)
	return err
}

// dropBatchTable drops the table used to store the primary key values of each
// batch of rows to be updated during the backfill process.
func (bf *Backfill) dropBatchTable(ctx context.Context, conn db.DB, tableName string) error {
	_, err := conn.ExecContext(ctx, fmt.Sprintf("DROP TABLE IF EXISTS %s.%s",
		pq.QuoteIdentifier(bf.stateSchema),
		pq.QuoteIdentifier(bf.batchTablePrefix+tableName)))
	return err
}

// getSnapshotID exports a snapshot from the given transaction.
func getSnapshotID(ctx context.Context, tx *sql.Tx) (string, error) {
	var snapshotID string

	err := tx.QueryRowContext(ctx, "SELECT pg_export_snapshot()").
		Scan(&snapshotID)

	return snapshotID, err
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

// IsPossible will return an error if the backfill operation is not supported
// on the table. A backfill is not possible if the table does not have suitable
// identity columns.
func IsPossible(table *schema.Table) error {
	cols := getIdentityColumns(table)
	if cols == nil {
		return NotPossibleError{Table: table.Name}
	}

	return nil
}

// getIdentityColumn will return the identity columns to use for the backfill.
// If the table has a primary key, it will use that. Otherwise, if the table
// has a UNIQUE, NOT NULL column, it will use that.
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
// It holds the state necessary to update the next batch of rows.
type batcher struct {
	templates.BatchConfig
}

func (b *batcher) selectBatch(ctx context.Context, conn db.DB) error {
	opts := &sql.TxOptions{Isolation: sql.LevelRepeatableRead}

	return conn.WithRetryableTransaction(ctx, opts, func(ctx context.Context, tx *sql.Tx) error {
		// Set the transaction snapshot. This ensures that row selection for each
		// batch sees only rows that were present in the table at the start of the
		// backfill process.
		_, err := tx.ExecContext(ctx, fmt.Sprintf("SET TRANSACTION SNAPSHOT %s",
			pq.QuoteLiteral(b.SnapshotID)))
		if err != nil {
			return err
		}

		// Truncate the batch table ready for the next batch of rows
		_, err = tx.ExecContext(ctx, fmt.Sprintf("TRUNCATE TABLE %s.%s",
			pq.QuoteIdentifier(b.StateSchema),
			pq.QuoteIdentifier(b.BatchTablePrefix+b.TableName)))
		if err != nil {
			return err
		}

		// Build the query to retrieve the next batch of rows
		query, err := templates.BuildSelectBatchInto(b.BatchConfig)
		if err != nil {
			return err
		}

		// Execute the query to select the next batch of rows into the batch table
		result, err := tx.ExecContext(ctx, query)
		if err != nil {
			return err
		}

		// Get the number of rows inserted by the query
		n, err := result.RowsAffected()
		if err != nil {
			return err
		}

		// If no rows were inserted, return a no rows error
		if n == 0 {
			return sql.ErrNoRows
		}

		return nil
	})
}

// updateBatch takes the next batch of rows to be updated from the batch table
// and updates them
func (b *batcher) updateBatch(ctx context.Context, conn db.DB) error {
	return conn.WithRetryableTransaction(ctx, nil, func(ctx context.Context, tx *sql.Tx) error {
		// Build the statement to update the batch of rows
		query, err := templates.BuildUpdateBatch(b.BatchConfig)
		if err != nil {
			return err
		}

		// Execute the query to update the batch of rows
		row := tx.QueryRowContext(ctx, query)

		// Initialize the LastValue slice if it is nil
		if b.LastValue == nil {
			b.LastValue = make([]string, len(b.PrimaryKey))
		}

		// Create a slice of pointers to the LastValue slice
		wrapper := make([]interface{}, len(b.LastValue))
		for i := range wrapper {
			wrapper[i] = &b.LastValue[i]
		}

		// Retrieve the last value of the primary key columns for the batch
		err = row.Scan(wrapper...)
		if err != nil {
			return err
		}

		return nil
	})
}
