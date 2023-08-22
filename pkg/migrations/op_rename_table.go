package migrations

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/lib/pq"
	"github.com/xataio/pg-roll/pkg/schema"
)

var _ Operation = (*OpRenameTable)(nil)

type OpRenameTable struct {
	From string `json:"from"`
	To   string `json:"to"`
}

func (o *OpRenameTable) Start(ctx context.Context, conn *sql.DB, schemaName, stateSchema string, s *schema.Schema) error {
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

func (o *OpRenameTable) Validate(ctx context.Context, s *schema.Schema) error {
	if s.GetTable(o.From) == nil {
		return TableDoesNotExistError{Name: o.From}
	}
	if s.GetTable(o.To) != nil {
		return TableAlreadyExistsError{Name: o.To}
	}

	return nil
}
