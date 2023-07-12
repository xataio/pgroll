package migrations

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/lib/pq"

	"pg-roll/pkg/schema"
)

type OpAddColumn struct {
	Table  string `json:"table"`
	Column Column `json:"column"`
}

var _ Operation = (*OpAddColumn)(nil)

func (o *OpAddColumn) Start(ctx context.Context, conn *sql.DB, s *schema.Schema) error {
	table := s.GetTable(o.Table)

	if err := addColumn(ctx, conn, *o, table); err != nil {
		return fmt.Errorf("failed to start add column operation: %w", err)
	}

	table.AddColumn(o.Column.Name, schema.Column{
		Name: TemporaryName(o.Column.Name),
	})

	return nil
}

func (o *OpAddColumn) Complete(ctx context.Context, conn *sql.DB) error {
	tempName := TemporaryName(o.Column.Name)

	_, err := conn.ExecContext(ctx, fmt.Sprintf("ALTER TABLE IF EXISTS %s RENAME COLUMN %s TO %s",
		pq.QuoteIdentifier(o.Table),
		pq.QuoteIdentifier(tempName),
		pq.QuoteIdentifier(o.Column.Name),
	))
	return err
}

func (o *OpAddColumn) Rollback(ctx context.Context, conn *sql.DB) error {
	tempName := TemporaryName(o.Column.Name)

	_, err := conn.ExecContext(ctx, fmt.Sprintf("ALTER TABLE IF EXISTS %s DROP COLUMN IF EXISTS %s",
		pq.QuoteIdentifier(o.Table),
		pq.QuoteIdentifier(tempName)))
	return err
}

func (o *OpAddColumn) Validate(ctx context.Context, s *schema.Schema) error {
	table := s.GetTable(o.Table)
	if table == nil {
		return TableDoesNotExistError{Name: o.Table}
	}

	if table.GetColumn(o.Column.Name) != nil {
		return ColumnAlreadyExistsError{Name: o.Column.Name, Table: o.Table}
	}

	return nil
}

func addColumn(ctx context.Context, conn *sql.DB, o OpAddColumn, t *schema.Table) error {
	o.Column.Name = TemporaryName(o.Column.Name)

	_, err := conn.ExecContext(ctx, fmt.Sprintf("ALTER TABLE %s ADD COLUMN %s",
		pq.QuoteIdentifier(t.Name),
		ColumnToSQL(o.Column),
	))
	return err
}
