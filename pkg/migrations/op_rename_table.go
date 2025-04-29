// SPDX-License-Identifier: Apache-2.0

package migrations

import (
	"context"
	"fmt"

	"github.com/lib/pq"
	"github.com/pterm/pterm"

	"github.com/xataio/pgroll/pkg/db"
	"github.com/xataio/pgroll/pkg/schema"
)

var _ Operation = (*OpRenameTable)(nil)

func (o *OpRenameTable) Start(ctx context.Context, logger pterm.Logger, conn db.DB, latestSchema string, s *schema.Schema) (*schema.Table, error) {
	logger.Info("starting operation", logger.Args(o.loggerArgs()...))
	return nil, s.RenameTable(o.From, o.To)
}

func (o *OpRenameTable) Complete(ctx context.Context, logger pterm.Logger, conn db.DB, s *schema.Schema) error {
	logger.Info("completing operation", logger.Args(o.loggerArgs()...))
	_, err := conn.ExecContext(ctx, fmt.Sprintf("ALTER TABLE IF EXISTS %s RENAME TO %s",
		pq.QuoteIdentifier(o.From),
		pq.QuoteIdentifier(o.To)))

	return err
}

func (o *OpRenameTable) Rollback(ctx context.Context, logger pterm.Logger, conn db.DB, s *schema.Schema) error {
	logger.Info("rolling back operation", logger.Args(o.loggerArgs()...))
	s.RenameTable(o.To, o.From)
	return nil
}

func (o *OpRenameTable) Validate(ctx context.Context, s *schema.Schema) error {
	if s.GetTable(o.From) == nil {
		return TableDoesNotExistError{Name: o.From}
	}
	if s.GetTable(o.To) != nil {
		return TableAlreadyExistsError{Name: o.To}
	}
	if err := ValidateIdentifierLength(o.To); err != nil {
		return err
	}

	s.RenameTable(o.From, o.To)
	return nil
}

func (o *OpRenameTable) loggerArgs() []any {
	return []any{
		"operation", OpNameRenameTable,
		"from", o.From,
		"to", o.To,
	}
}
