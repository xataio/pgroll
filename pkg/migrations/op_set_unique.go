// SPDX-License-Identifier: Apache-2.0

package migrations

import (
	"context"
	"fmt"
	"time"

	"github.com/lib/pq"
	"github.com/xataio/pgroll/pkg/db"
	"github.com/xataio/pgroll/pkg/schema"
)

type OpSetUnique struct {
	Name   string `json:"name"`
	Table  string `json:"table"`
	Column string `json:"column"`
	Up     string `json:"up"`
	Down   string `json:"down"`
}

var _ Operation = (*OpSetUnique)(nil)

func (o *OpSetUnique) Start(ctx context.Context, conn db.DB, latestSchema string, tr SQLTransformer, s *schema.Schema, cbs ...CallbackFn) (*schema.Table, error) {
	table := s.GetTable(o.Table)
	column := table.GetColumn(o.Column)

	for retryCount := 5; retryCount > 0; retryCount-- {
		// Add a unique index to the new column
		if err := addUniqueIndex(ctx, conn, table.Name, column.Name, o.Name); err != nil {
			return nil, fmt.Errorf("failed to add unique index: %w", err)
		}

		// Make sure Postgres is done creating the index
		isInProgress := true
		for isInProgress {
			var err error
			isInProgress, err = isIndexInProgress(ctx, conn, s.Name, o.Name)
			if err != nil {
				return nil, err
			}
			// Still in progress, sleep for 0.5 seconds and check again
			time.Sleep(500 * time.Millisecond)
		}

		// Check pg_index to see if it's valid or not. Break if it's valid.
		isValid, err := isIndexValid(ctx, conn, s.Name, o.Name)
		if err != nil {
			return nil, err
		}

		if isValid {
			break
		}

		// If not valid, since Postgres has already given up validating the index,
		// it will remain invalid forever. Drop it and try again.
		_, err = conn.ExecContext(ctx, fmt.Sprintf("DROP INDEX IF EXISTS %s.%s", pq.QuoteIdentifier(s.Name), pq.QuoteIdentifier(o.Name)))
		if err != nil {
			return nil, fmt.Errorf("failed to drop index: %w", err)
		}
	}

	return table, nil
}

func (o *OpSetUnique) Complete(ctx context.Context, conn db.DB, tr SQLTransformer, s *schema.Schema) error {
	// Create a unique constraint using the unique index
	_, err := conn.ExecContext(ctx, fmt.Sprintf("ALTER TABLE IF EXISTS %s ADD CONSTRAINT %s UNIQUE USING INDEX %s",
		pq.QuoteIdentifier(o.Table),
		pq.QuoteIdentifier(o.Name),
		pq.QuoteIdentifier(o.Name)))
	if err != nil {
		return err
	}

	return err
}

func (o *OpSetUnique) Rollback(ctx context.Context, conn db.DB, tr SQLTransformer, s *schema.Schema) error {
	return nil
}

func (o *OpSetUnique) Validate(ctx context.Context, s *schema.Schema) error {
	if o.Name == "" {
		return FieldRequiredError{Name: "name"}
	}

	table := s.GetTable(o.Table)
	if table == nil {
		return TableDoesNotExistError{Name: o.Table}
	}

	if table.GetColumn(o.Column) == nil {
		return ColumnDoesNotExistError{Table: o.Table, Name: o.Column}
	}

	if table.ConstraintExists(o.Name) {
		return ConstraintAlreadyExistsError{
			Table:      table.Name,
			Constraint: o.Name,
		}
	}

	return nil
}

func addUniqueIndex(ctx context.Context, conn db.DB, table, column, name string) error {
	// create unique index concurrently
	_, err := conn.ExecContext(ctx, fmt.Sprintf("CREATE UNIQUE INDEX CONCURRENTLY IF NOT EXISTS %s ON %s (%s)",
		pq.QuoteIdentifier(name),
		pq.QuoteIdentifier(table),
		pq.QuoteIdentifier(column)))

	return err
}

func isIndexInProgress(ctx context.Context, conn db.DB, schemaname string, indexname string) (bool, error) {
	var isInProgress bool
	rows, err := conn.QueryContext(ctx, `
		SELECT EXISTS(
			SELECT * FROM pg_catalog.pg_stat_progress_create_index
			WHERE index_relid = $1::regclass
			)`,
		fmt.Sprintf("%s.%s", pq.QuoteIdentifier(schemaname), pq.QuoteIdentifier(indexname)))
	if err != nil {
		return false, fmt.Errorf("getting index in progress with name %q: %w", indexname, err)
	}
	if rows == nil {
		// if rows == nil && err != nil, then it means we have queried a fake db.
		// In that case, we can safely return false.
		return false, nil
	}
	if err := db.ScanFirstValue(rows, &isInProgress); err != nil {
		return false, fmt.Errorf("scanning index in progress with name %q: %w", indexname, err)
	}

	return isInProgress, nil
}

func isIndexValid(ctx context.Context, conn db.DB, schemaname string, indexname string) (bool, error) {
	var isValid bool
	rows, err := conn.QueryContext(ctx, `
		SELECT indisvalid
		FROM pg_catalog.pg_index
		WHERE indexrelid = $1::regclass`,
		fmt.Sprintf("%s.%s", pq.QuoteIdentifier(schemaname), pq.QuoteIdentifier(indexname)))
	if err != nil {
		return false, fmt.Errorf("getting index with name %q: %w", indexname, err)
	}
	if rows == nil {
		// if rows == nil && err != nil, then it means we have queried a fake db.
		// In that case, we can safely return true.
		return true, nil
	}
	if err := db.ScanFirstValue(rows, &isValid); err != nil {
		return false, fmt.Errorf("scanning index with name %q: %w", indexname, err)
	}

	return isValid, nil
}
