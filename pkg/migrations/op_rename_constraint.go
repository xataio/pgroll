// SPDX-License-Identifier: Apache-2.0

package migrations

import (
	"context"

	"github.com/pterm/pterm"
	"github.com/xataio/pgroll/pkg/db"
	"github.com/xataio/pgroll/pkg/schema"
)

var _ Operation = (*OpRenameConstraint)(nil)

func (o *OpRenameConstraint) Start(ctx context.Context, logger pterm.Logger, conn db.DB, latestSchema string, s *schema.Schema) (*schema.Table, error) {
	logger.Info("starting operation", logger.Args(o.loggerArgs()...))
	// no-op
	return nil, nil
}

func (o *OpRenameConstraint) Complete(ctx context.Context, logger pterm.Logger, conn db.DB, s *schema.Schema) error {
	logger.Info("completing operation", logger.Args(o.loggerArgs()...))
	// rename the constraint in the underlying table
	return NewRenameConstraintAction(conn, o.Table, o.From, o.To).Execute(ctx)
}

func (o *OpRenameConstraint) Rollback(ctx context.Context, logger pterm.Logger, conn db.DB, s *schema.Schema) error {
	logger.Info("rolling back operation", logger.Args(o.loggerArgs()...))
	// no-op
	return nil
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

func (o *OpRenameConstraint) loggerArgs() []any {
	return []any{
		"operation", OpNameRenameConstraint,
		"from", o.From,
		"to", o.To,
		"table", o.Table,
	}
}
