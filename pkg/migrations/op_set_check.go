// SPDX-License-Identifier: Apache-2.0

package migrations

import (
	"context"
	"fmt"

	"github.com/xataio/pgroll/pkg/db"
	"github.com/xataio/pgroll/pkg/schema"
)

type OpSetCheckConstraint struct {
	Table  string          `json:"table"`
	Column string          `json:"column"`
	Check  CheckConstraint `json:"check"`
	Up     string          `json:"up"`
	Down   string          `json:"down"`
}

var _ Operation = (*OpSetCheckConstraint)(nil)

func (o *OpSetCheckConstraint) Start(ctx context.Context, l Logger, conn db.DB, latestSchema string, s *schema.Schema) (*schema.Table, error) {
	l.LogOperationStart(o)

	table := s.GetTable(o.Table)
	if table == nil {
		return nil, TableDoesNotExistError{Name: o.Table}
	}

	// Add the check constraint to the new column as NOT VALID.
	if err := NewCreateCheckConstraintAction(conn, table.Name, o.Check.Name, o.Check.Constraint, []string{o.Column}, o.Check.NoInherit, true).Execute(ctx); err != nil {
		return nil, fmt.Errorf("failed to add check constraint: %w", err)
	}

	return table, nil
}

func (o *OpSetCheckConstraint) Complete(ctx context.Context, l Logger, conn db.DB, s *schema.Schema) error {
	l.LogOperationComplete(o)

	// Validate the check constraint
	err := NewValidateConstraintAction(conn, o.Table, o.Check.Name).Execute(ctx)
	if err != nil {
		return err
	}

	return nil
}

func (o *OpSetCheckConstraint) Rollback(ctx context.Context, l Logger, conn db.DB, s *schema.Schema) error {
	l.LogOperationRollback(o)

	return nil
}

func (o *OpSetCheckConstraint) Validate(ctx context.Context, s *schema.Schema) error {
	if err := o.Check.Validate(); err != nil {
		return CheckConstraintError{
			Table:  o.Table,
			Column: o.Column,
			Err:    err,
		}
	}

	table := s.GetTable(o.Table)
	if table == nil {
		return TableDoesNotExistError{Name: o.Table}
	}

	if table.ConstraintExists(o.Check.Name) {
		return ConstraintAlreadyExistsError{
			Table:      table.Name,
			Constraint: o.Check.Name,
		}
	}

	if o.Up == "" {
		return FieldRequiredError{Name: "up"}
	}

	if o.Down == "" {
		return FieldRequiredError{Name: "down"}
	}

	return nil
}
