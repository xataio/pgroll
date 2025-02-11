// SPDX-License-Identifier: Apache-2.0

package migrations

import (
	"context"
	"fmt"

	"github.com/lib/pq"

	"github.com/xataio/pgroll/pkg/db"
	"github.com/xataio/pgroll/pkg/schema"
)

var _ Operation = (*OpRenameTable)(nil)

func (o *OpRenameTable) Start(ctx context.Context, conn db.DB, latestSchema string, tr SQLTransformer, s *schema.Schema) (*schema.Table, error) {
	return nil, s.RenameTable(o.From, o.To)
}

func (o *OpRenameTable) Complete(ctx context.Context, conn db.DB, tr SQLTransformer, s *schema.Schema) error {
	_, err := conn.ExecContext(ctx, fmt.Sprintf("ALTER TABLE IF EXISTS %s RENAME TO %s",
		pq.QuoteIdentifier(o.From),
		pq.QuoteIdentifier(o.To)))

	// Rename the table in the virtual schema so that the `Complete` methods
	// of subsequent operations in the same migration can find it.
	s.RenameTable(o.From, o.To)

	// Update the physical name of the table in the virtual schema now that it
	// has really been renamed.
	table := s.GetTable(o.To)
	table.Name = o.To

	return err
}

func (o *OpRenameTable) Rollback(ctx context.Context, conn db.DB, tr SQLTransformer, s *schema.Schema) error {
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
