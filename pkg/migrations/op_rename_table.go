// SPDX-License-Identifier: Apache-2.0

package migrations

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/lib/pq"
	"github.com/xataio/pgroll/pkg/schema"
)

var _ Operation = (*OpRenameTable)(nil)

func (o *OpRenameTable) Start(ctx context.Context, conn *sql.DB, stateSchema string, s *schema.Schema, cbs ...CallbackFn) (*schema.Table, error) {
	return nil, s.RenameTable(o.From, o.To)
}

func (o *OpRenameTable) Complete(ctx context.Context, conn *sql.DB, s *schema.Schema) error {
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
