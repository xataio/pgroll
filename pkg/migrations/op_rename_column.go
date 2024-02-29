// SPDX-License-Identifier: Apache-2.0

package migrations

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/lib/pq"
	"github.com/xataio/pgroll/pkg/schema"
)

type OpRenameColumn struct {
	Table string `json:"table"`
	From  string `json:"from"`
	To    string `json:"to"`
}

var _ Operation = (*OpRenameColumn)(nil)

func (o *OpRenameColumn) Start(ctx context.Context, conn *sql.DB, stateSchema string, s *schema.Schema, cbs ...CallbackFn) (*schema.Table, error) {
	table := s.GetTable(o.Table)
	table.RenameColumn(o.From, o.To)
	return nil, nil
}

func (o *OpRenameColumn) Complete(ctx context.Context, conn *sql.DB, s *schema.Schema) error {
	// rename the column in the underlying table
	_, err := conn.ExecContext(ctx, fmt.Sprintf("ALTER TABLE %s RENAME COLUMN %s TO %s",
		pq.QuoteIdentifier(o.Table),
		pq.QuoteIdentifier(o.From),
		pq.QuoteIdentifier(o.To)))
	return err
}

func (o *OpRenameColumn) Rollback(ctx context.Context, conn *sql.DB) error {
	// no-op
	return nil
}

func (o *OpRenameColumn) Validate(ctx context.Context, s *schema.Schema) error {
	table := s.GetTable(o.Table)

	if table.GetColumn(o.To) != nil {
		return ColumnAlreadyExistsError{Table: o.Table, Name: o.To}
	}

	return nil
}
