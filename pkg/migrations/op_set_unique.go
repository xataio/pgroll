package migrations

import (
	"context"
	"database/sql"
	"fmt"
	"strings"

	"github.com/lib/pq"

	"pg-roll/pkg/schema"
)

type OpSetUnique struct {
	Name    string   `json:"name"`
	Table   string   `json:"table"`
	Columns []string `json:"columns"`
}

var _ Operation = (*OpSetUnique)(nil)

func (o *OpSetUnique) Start(ctx context.Context, conn *sql.DB, schemaName string, stateSchema string, s *schema.Schema) error {
	// create unique index concurrently
	_, err := conn.ExecContext(ctx, fmt.Sprintf("CREATE UNIQUE INDEX CONCURRENTLY IF NOT EXISTS %s ON %s (%s)",
		pq.QuoteIdentifier(o.Name),
		pq.QuoteIdentifier(o.Table),
		strings.Join(quoteColumnNames(o.Columns), ", ")))
	return err
}

func (o *OpSetUnique) Complete(ctx context.Context, conn *sql.DB) error {
	// create a unique constraint using the unique index
	_, err := conn.ExecContext(ctx, fmt.Sprintf("ALTER TABLE IF EXISTS %s ADD CONSTRAINT %s UNIQUE USING INDEX %s",
		pq.QuoteIdentifier(o.Table),
		pq.QuoteIdentifier(o.Name),
		pq.QuoteIdentifier(o.Name)))

	return err
}

func (o *OpSetUnique) Rollback(ctx context.Context, conn *sql.DB) error {
	// drop the index concurrently
	_, err := conn.ExecContext(ctx, fmt.Sprintf("DROP INDEX CONCURRENTLY IF EXISTS %s", o.Name))

	return err
}

func (o *OpSetUnique) Validate(ctx context.Context, s *schema.Schema) error {
	if o.Name == "" {
		return NameRequiredError{}
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

	return nil
}
