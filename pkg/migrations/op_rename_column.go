package migrations

import (
	"context"
	"database/sql"
	"fmt"

	"pg-roll/pkg/schema"

	"github.com/lib/pq"
)

var _ Operation = (*OpRenameColumn)(nil)

const OpRenameColumnName = "rename_column"

type OpRenameColumn struct {
	Table string `json:"table"`
	From  string `json:"from"`
	To    string `json:"to"`
}

func (o *OpRenameColumn) Start(ctx context.Context, conn *sql.DB, s *schema.Schema) error {
	table := s.GetTable(o.Table)
	if table == nil {
		return fmt.Errorf("table %s does not exist", o.Table)
	}

	return table.RenameColumn(o.From, o.To)
}

func (o *OpRenameColumn) Complete(ctx context.Context, conn *sql.DB) error {
	_, err := conn.ExecContext(ctx, fmt.Sprintf("ALTER TABLE %s RENAME COLUMN %s TO %s",
		pq.QuoteIdentifier(o.Table),
		pq.QuoteIdentifier(o.From),
		pq.QuoteIdentifier(o.To)))
	return err
}

func (o *OpRenameColumn) Rollback(ctx context.Context, conn *sql.DB) error {
	return nil
}
