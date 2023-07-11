package migrations

import (
	"context"
	"database/sql"
	"errors"
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
	if o.Column.Nullable {
		if err := addNullableColumn(ctx, conn, o, table); err != nil {
			return fmt.Errorf("failed to start add column operation: %w", err)
		}
	} else {
		return errors.New("addition of non-nullable columns not implemented")
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
		return fmt.Errorf("table %q does not exist", o.Table)
	}

	if table.GetColumn(o.Column.Name) != nil {
		return fmt.Errorf("column %q already exists in table %q", o.Column.Name, o.Table)
	}

	return nil
}

func addNullableColumn(ctx context.Context, conn *sql.DB, o *OpAddColumn, t *schema.Table) error {
	_, err := conn.ExecContext(ctx, fmt.Sprintf("ALTER TABLE %s ADD COLUMN %s %s",
		pq.QuoteIdentifier(t.Name),
		pq.QuoteIdentifier(TemporaryName(o.Column.Name)),
		o.Column.Type,
	))
	return err
}
