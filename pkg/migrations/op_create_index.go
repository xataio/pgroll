package migrations

import (
	"context"
	"database/sql"
	"fmt"
	"strings"

	"github.com/lib/pq"

	"pg-roll/pkg/schema"
)

type OpCreateIndex struct {
	Table   string   `json:"table"`
	Columns []string `json:"columns"`
}

var _ Operation = (*OpCreateIndex)(nil)

func (o *OpCreateIndex) Start(ctx context.Context, conn *sql.DB, schemaName string, stateSchema string, s *schema.Schema) error {
	// create index concurrently
	_, err := conn.ExecContext(ctx, fmt.Sprintf("CREATE INDEX CONCURRENTLY IF NOT EXISTS %s ON %s (%s)",
		pq.QuoteIdentifier(IndexName(o.Table, o.Columns)),
		pq.QuoteIdentifier(o.Table),
		strings.Join(quoteColumnNames(o.Columns), ", ")))
	return err
}

func (o *OpCreateIndex) Complete(ctx context.Context, conn *sql.DB) error {
	// No-op
	return nil
}

func (o *OpCreateIndex) Rollback(ctx context.Context, conn *sql.DB) error {
	// drop the index concurrently
	_, err := conn.ExecContext(ctx, fmt.Sprintf("DROP INDEX CONCURRENTLY IF EXISTS %s",
		IndexName(o.Table, o.Columns)))

	return err
}

func (o *OpCreateIndex) Validate(ctx context.Context, s *schema.Schema) error {
	table := s.GetTable(o.Table)

	if table == nil {
		return TableDoesNotExistError{Name: o.Table}
	}

	for _, column := range o.Columns {
		if table.GetColumn(column) == nil {
			return ColumnDoesNotExistError{Table: o.Table, Name: column}
		}
	}

	return nil
}

func IndexName(table string, columns []string) string {
	return "_pgroll_idx_" + table + "_" + strings.Join(columns, "_")
}

func quoteColumnNames(columns []string) (quoted []string) {
	for _, col := range columns {
		quoted = append(quoted, pq.QuoteIdentifier(col))
	}
	return quoted
}
