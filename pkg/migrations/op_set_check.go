// SPDX-License-Identifier: Apache-2.0

package migrations

import (
	"context"
	"fmt"
	"strings"

	"github.com/lib/pq"
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

func (o *OpSetCheckConstraint) Start(ctx context.Context, conn db.DB, tr SQLTransformer, s *schema.Schema, cbs ...CallbackFn) (*schema.Table, error) {
	table := s.GetTable(o.Table)

	// Add the check constraint to the new column as NOT VALID.
	if err := o.addCheckConstraint(ctx, conn); err != nil {
		return nil, fmt.Errorf("failed to add check constraint: %w", err)
	}

	return table, nil
}

func (o *OpSetCheckConstraint) Complete(ctx context.Context, conn db.DB, tr SQLTransformer, s *schema.Schema) error {
	// Validate the check constraint
	_, err := conn.ExecContext(ctx, fmt.Sprintf("ALTER TABLE IF EXISTS %s VALIDATE CONSTRAINT %s",
		pq.QuoteIdentifier(o.Table),
		pq.QuoteIdentifier(o.Check.Name)))
	if err != nil {
		return err
	}

	return nil
}

func (o *OpSetCheckConstraint) Rollback(ctx context.Context, conn db.DB, tr SQLTransformer) error {
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

	if o.Up == "" {
		return FieldRequiredError{Name: "up"}
	}

	if o.Down == "" {
		return FieldRequiredError{Name: "down"}
	}

	return nil
}

func (o *OpSetCheckConstraint) addCheckConstraint(ctx context.Context, conn db.DB) error {
	_, err := conn.ExecContext(ctx, fmt.Sprintf("ALTER TABLE %s ADD CONSTRAINT %s CHECK (%s) NOT VALID",
		pq.QuoteIdentifier(o.Table),
		pq.QuoteIdentifier(o.Check.Name),
		rewriteCheckExpression(o.Check.Constraint, o.Column, TemporaryName(o.Column)),
	))

	return err
}

// In order for the `check` expression to be easy to write, migration authors specify
// the check expression as though it were being applied to the old column,
// On migration start, however, the check is actually applied to the new (temporary)
// column.
// This function naively rewrites the check expression to apply to the new column.
func rewriteCheckExpression(check string, oldColumn, newColumn string) string {
	return strings.ReplaceAll(check, oldColumn, newColumn)
}
