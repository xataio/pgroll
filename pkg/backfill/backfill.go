// SPDX-License-Identifier: Apache-2.0

package backfill

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/lib/pq"
	"github.com/xataio/pgroll/pkg/backfill/templates"
	"github.com/xataio/pgroll/pkg/db"
	"github.com/xataio/pgroll/pkg/schema"
)

// CNeedsBackfillColumn is the name of the internal column created
// by pgroll to mark rows that must be backfilled
const CNeedsBackfillColumn = "_pgroll_needs_backfill"

// Task represents a backfill task for a specific table from an operation.
type Task struct {
	table    *schema.Table
	triggers []OperationTrigger
}

// Job is a collection of all tables that need to be backfilled and their associated triggers.
type Job struct {
	schemaName   string
	latestSchema string
	triggers     map[string]triggerConfig

	Tables []*schema.Table
}

type Backfill struct {
	conn db.DB
	*Config
}

type CallbackFn func(done int64, total int64)

func NewTask(table *schema.Table, triggers ...OperationTrigger) *Task {
	return &Task{
		table:    table,
		triggers: triggers,
	}
}

func NewJob(schemaName, latestSchema string) *Job {
	return &Job{
		schemaName:   schemaName,
		latestSchema: latestSchema,
		triggers:     make(map[string]triggerConfig, 0),
		Tables:       make([]*schema.Table, 0),
	}
}

func (t *Task) AddTriggers(other *Task) {
	t.triggers = append(t.triggers, other.triggers...)
}

func (j *Job) AddTask(t *Task) {
	if t.table != nil {
		j.Tables = append(j.Tables, t.table)
	}

	for _, trigger := range t.triggers {
		if tg, exists := j.triggers[trigger.Name]; exists {
			// If the trigger already exists, append the SQL to the existing trigger config
			// The rewriting is necessary to ensure that the expression uses the NEW prefix with the physical column name
			// For example, if the user sets the following expression in their second operation:
			// CASE WHEN "review" = 'bad' THEN 'bad review' ELSE 'good review' END, it must be rewritten to:
			// CASE WHEN NEW."_pgroll_new_review" = 'bad' THEN 'bad review' ELSE 'good review' END.
			// Otherwise, the trigger will not work correctly because it will reference the old column name.
			tg.SQL = append(tg.SQL, rewriteTriggerSQL(trigger.SQL, findColumnName(tg.Columns, tg.PhysicalColumn), tg.PhysicalColumn))
			j.triggers[trigger.Name] = tg
		} else {
			// If the trigger does not exist, create a new trigger config
			// No need to rewrite the SQL here, as it is the first time we are adding it.
			j.triggers[trigger.Name] = triggerConfig{
				Name:                trigger.Name,
				Direction:           trigger.Direction,
				Columns:             trigger.Columns,
				SchemaName:          j.schemaName,
				TableName:           trigger.TableName,
				PhysicalColumn:      trigger.PhysicalColumn,
				LatestSchema:        j.latestSchema,
				SQL:                 []string{trigger.SQL},
				NeedsBackfillColumn: CNeedsBackfillColumn,
			}
		}
	}
}

// rewriteTriggerSQL rewrites the SQL migrations expression provided by the user
// in the up or down attribute of the operations config.
// The column name are turned from user defined names the physical column name with NEW prefix.
// This is only needed, If there is already an backfilling step in the trigger SQL.
func rewriteTriggerSQL(sql string, from, to string) string {
	return strings.ReplaceAll(sql, from, fmt.Sprintf("NEW.%s", pq.QuoteIdentifier(to)))
}

// findColumnName returns the original, user defined column name from the map of columns
// for the provided physical column name.
// __pgroll_new_name -> name
func findColumnName(columns map[string]*schema.Column, columnName string) string {
	for name, col := range columns {
		if col.Name == columnName {
			return name
		}
	}
	return columnName
}

// New creates a new backfill operation with the given options. The backfill is
// not started until `Start` is invoked.
func New(conn db.DB, c *Config) *Backfill {
	b := &Backfill{
		conn:   conn,
		Config: c,
	}

	return b
}

// CreateTriggers creates the triggers for the tables before starting the backfill.
func (bf *Backfill) CreateTriggers(ctx context.Context, j *Job) error {
	for _, trigger := range j.triggers {
		a := &createTriggerAction{
			conn: bf.conn,
			cfg:  trigger,
		}
		if err := a.execute(ctx); err != nil {
			return fmt.Errorf("creating trigger %q: %w", trigger.Name, err)
		}
	}
	return nil
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
				NeedsBackfillColumn: CNeedsBackfillColumn,
			},
		}
	} else {
		b = &needsBackfillColumnBatcher{
			table:               table.Name,
			batchSize:           bf.batchSize,
			needsBackfillColumn: CNeedsBackfillColumn,
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
	rows, err = conn.QueryContext(ctx, fmt.Sprintf(`SELECT count(*) from %s`, pq.QuoteIdentifier(tableName)))
	if err != nil {
		return 0, fmt.Errorf("getting row count for %q: %w", tableName, err)
	}
	if err := db.ScanFirstValue(rows, &total); err != nil {
		return 0, fmt.Errorf("scanning row count for %q: %w", tableName, err)
	}

	return total, nil
}

// getIdentityColumns will return a column suitable for use in a backfill operation.
func getIdentityColumns(table *schema.Table) []string {
	if len(table.PrimaryKey) != 0 {
		return table.PrimaryKey
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
