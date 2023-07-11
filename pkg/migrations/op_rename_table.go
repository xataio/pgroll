package migrations

import (
	"context"
	"database/sql"
	"fmt"

	"pg-roll/pkg/schema"

	"github.com/lib/pq"
)

var _ Operation = (*OpRenameTable)(nil)

type OpRenameTable struct {
	From string `json:"from"`
	To   string `json:"to"`
}

func (o *OpRenameTable) Start(ctx context.Context, conn *sql.DB, s *schema.Schema) error {
	return s.RenameTable(o.From, o.To)
}

func (o *OpRenameTable) Complete(ctx context.Context, conn *sql.DB) error {
	_, err := conn.ExecContext(ctx, fmt.Sprintf("ALTER TABLE IF EXISTS %s RENAME TO %s",
		pq.QuoteIdentifier(o.From),
		pq.QuoteIdentifier(o.To)))
	return err
}

func (o *OpRenameTable) Rollback(ctx context.Context, conn *sql.DB) error {
	return nil
}
