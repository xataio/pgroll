// SPDX-License-Identifier: Apache-2.0

package migrations

import (
	"context"

	"github.com/xataio/pgroll/pkg/backfill"
	"github.com/xataio/pgroll/pkg/db"
	"github.com/xataio/pgroll/pkg/schema"
)

var _ Operation = (*OpRenameTable)(nil)

func (o *OpRenameTable) Start(ctx context.Context, l Logger, conn db.DB, s *schema.Schema) ([]DBAction, *backfill.Task, error) {
	l.LogOperationStart(o)

	return nil, nil, s.RenameTable(o.From, o.To)
}

func (o *OpRenameTable) Complete(l Logger, conn db.DB, s *schema.Schema) ([]DBAction, error) {
	l.LogOperationComplete(o)

	return []DBAction{NewRenameTableAction(conn, o.From, o.To)}, nil
}

func (o *OpRenameTable) Rollback(l Logger, conn db.DB, s *schema.Schema) ([]DBAction, error) {
	l.LogOperationRollback(o)

	s.RenameTable(o.To, o.From)
	return nil, nil
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
