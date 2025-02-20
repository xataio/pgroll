// SPDX-License-Identifier: Apache-2.0

package migrations

import (
	"context"
	"fmt"

	"github.com/lib/pq"

	"github.com/xataio/pgroll/pkg/db"
	"github.com/xataio/pgroll/pkg/schema"
)

var _ Operation = (*OpRenameConstraint)(nil)

func (o *OpRenameConstraint) Start(ctx context.Context, conn db.DB, latestSchema string, s *schema.Schema) (*schema.Table, error) {
	// no-op
	return nil, nil
}

func (o *OpRenameConstraint) Complete(ctx context.Context, conn db.DB, s *schema.Schema) error {
	// rename the constraint in the underlying table
	_, err := conn.ExecContext(ctx, fmt.Sprintf("ALTER TABLE %s RENAME CONSTRAINT %s TO %s",
		pq.QuoteIdentifier(o.Table),
		pq.QuoteIdentifier(o.From),
		pq.QuoteIdentifier(o.To)))
	return err
}

func (o *OpRenameConstraint) Rollback(ctx context.Context, conn db.DB, s *schema.Schema) error {
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
