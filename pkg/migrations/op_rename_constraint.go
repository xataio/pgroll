// SPDX-License-Identifier: Apache-2.0

package migrations

import (
	"context"

	"github.com/xataio/pgroll/pkg/backfill"
	"github.com/xataio/pgroll/pkg/db"
	"github.com/xataio/pgroll/pkg/schema"
)

var _ Operation = (*OpRenameConstraint)(nil)

func (o *OpRenameConstraint) Start(ctx context.Context, l Logger, conn db.DB, latestSchema string, s *schema.Schema) (*backfill.Task, error) {
	l.LogOperationStart(o)

	// no-op
	return nil, nil
}

func (o *OpRenameConstraint) Complete(l Logger, conn db.DB, s *schema.Schema) ([]DBAction, error) {
	l.LogOperationComplete(o)

	return []DBAction{
		// rename the constraint in the underlying table
		NewRenameConstraintAction(conn, o.Table, o.From, o.To),
	}, nil
}

func (o *OpRenameConstraint) Rollback(l Logger, conn db.DB, s *schema.Schema) ([]DBAction, error) {
	l.LogOperationRollback(o)

	// no-op
	return nil, nil
}

func (o *OpRenameConstraint) Validate(ctx context.Context, s *schema.Schema) error {
	table := s.GetTable(o.Table)

	if table == nil {
		return TableDoesNotExistError{Name: o.Table}
	}

	if !table.ConstraintExists(o.From) {
		return ConstraintDoesNotExistError{Table: o.Table, Constraint: o.From}
	}

	if table.ConstraintExists(o.To) {
		return ConstraintAlreadyExistsError{Table: o.Table, Constraint: o.To}
	}

	if err := ValidateIdentifierLength(o.To); err != nil {
		return err
	}

	return nil
}
