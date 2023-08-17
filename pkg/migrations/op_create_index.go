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
	Name    *string  `json:"name"`
	Table   string   `json:"table"`
	Columns []string `json:"columns"`
}

var _ Operation = (*OpCreateIndex)(nil)

func (o *OpCreateIndex) Start(ctx context.Context, conn *sql.DB, schemaName string, stateSchema string, s *schema.Schema) error {
	// create index concurrently
	_, err := conn.ExecContext(ctx, fmt.Sprintf("CREATE INDEX CONCURRENTLY IF NOT EXISTS %s ON %s (%s)",
		pq.QuoteIdentifier(indexName(o)),
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
	_, err := conn.ExecContext(ctx, fmt.Sprintf("DROP INDEX CONCURRENTLY IF EXISTS %s", indexName(o)))

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

	// Index names must be unique across the entire schema.
	for _, table := range s.Tables {
		_, ok := table.Indexes[indexName(o)]
		if ok {
			return IndexAlreadyExistsError{Name: indexName(o)}
		}
	}

	return nil
}

func GenerateIndexName(table string, columns []string) string {
	return "_pgroll_idx_" + table + "_" + strings.Join(columns, "_")
}

func indexName(o *OpCreateIndex) string {
	if o.Name != nil {
		return *o.Name
	}

	return GenerateIndexName(o.Table, o.Columns)
}

func quoteColumnNames(columns []string) (quoted []string) {
	for _, col := range columns {
		quoted = append(quoted, pq.QuoteIdentifier(col))
	}
	return quoted
}
