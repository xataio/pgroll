// SPDX-License-Identifier: Apache-2.0

package migrations

import (
	"context"
	"database/sql"
	"fmt"
	"strings"

	"github.com/lib/pq"
	"github.com/xataio/pgroll/pkg/schema"
)

var _ Operation = (*OpCreateIndex)(nil)

func (o *OpCreateIndex) Start(ctx context.Context, conn *sql.DB, stateSchema string, s *schema.Schema, cbs ...CallbackFn) error {
	// create index concurrently
	_, err := conn.ExecContext(ctx, fmt.Sprintf("CREATE INDEX CONCURRENTLY IF NOT EXISTS %s ON %s (%s)",
		pq.QuoteIdentifier(o.Name),
		pq.QuoteIdentifier(o.Table),
		strings.Join(quoteColumnNames(o.Columns), ", ")))
	return err
}

func (o *OpCreateIndex) Complete(ctx context.Context, conn *sql.DB, s *schema.Schema) error {
	// No-op
	return nil
}

func (o *OpCreateIndex) Rollback(ctx context.Context, conn *sql.DB) error {
	// drop the index concurrently
	_, err := conn.ExecContext(ctx, fmt.Sprintf("DROP INDEX CONCURRENTLY IF EXISTS %s", o.Name))

	return err
}

func (o *OpCreateIndex) Validate(ctx context.Context, s *schema.Schema) error {
	if o.Name == "" {
		return FieldRequiredError{Name: "name"}
	}
	
	if err := validateName(o.Name); err != nil {
		return err
	}

	table := s.GetTable(o.Table)
	if table == nil {
		return TableDoesNotExistError{Name: o.Table}
	}

	for _, column := range o.Columns {
		if table.GetColumn(column) == nil {
			return ColumnDoesNotExistError{Table: o.Table, Name: column}
		}
	}

	// Index names must be unique across the entire schema.
	for _, table := range s.Tables {
		_, ok := table.Indexes[o.Name]
		if ok {
			return IndexAlreadyExistsError{Name: o.Name}
		}
	}

	return nil
}

func quoteColumnNames(columns []string) (quoted []string) {
	for _, col := range columns {
		quoted = append(quoted, pq.QuoteIdentifier(col))
	}
	return quoted
}
