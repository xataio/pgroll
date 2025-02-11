// SPDX-License-Identifier: Apache-2.0

package migrations

import (
	"context"
	"fmt"

	"github.com/lib/pq"
	"github.com/xataio/pgroll/pkg/db"
	"github.com/xataio/pgroll/pkg/schema"
)

var _ Operation = (*OpDropTable)(nil)

func (o *OpDropTable) Start(ctx context.Context, conn db.DB, latestSchema string, tr SQLTransformer, s *schema.Schema) (*schema.Table, error) {
	table := s.GetTable(o.Name)

	// Soft-delete the table in order that a create table operation in the same
	// migration can create a table with the same name
	_, err := conn.ExecContext(ctx, fmt.Sprintf("ALTER TABLE IF EXISTS %s RENAME TO %s",
		table.Name,
		DeletionName(table.Name)))
	if err != nil {
		return nil, fmt.Errorf("failed to rename table %s: %w", o.Name, err)
	}

	s.RemoveTable(o.Name)
	return nil, nil
}

func (o *OpDropTable) Complete(ctx context.Context, conn db.DB, tr SQLTransformer, s *schema.Schema) error {
	deletionName := DeletionName(o.Name)

	// Perform the actual deletion of the soft-deleted table
	_, err := conn.ExecContext(ctx, fmt.Sprintf("DROP TABLE IF EXISTS %s", pq.QuoteIdentifier(deletionName)))

	return err
}

func (o *OpDropTable) Rollback(ctx context.Context, conn db.DB, tr SQLTransformer, s *schema.Schema) error {
	// Mark the table as no longer deleted so that it is visible to preceding
	// Rollbacks in the same migration
	s.UnRemoveTable(o.Name)

	// Rename the table back to its original name from its soft-deleted name
	table := s.GetTable(o.Name)
	_, err := conn.ExecContext(ctx, fmt.Sprintf("ALTER TABLE IF EXISTS %s RENAME TO %s",
		DeletionName(table.Name),
		table.Name))
	if err != nil {
		return fmt.Errorf("failed to rename table %s: %w", o.Name, err)
	}

	return nil
}

func (o *OpDropTable) Validate(ctx context.Context, s *schema.Schema) error {
	table := s.GetTable(o.Name)

	if table == nil {
		return TableDoesNotExistError{Name: o.Name}
	}

	s.RemoveTable(table.Name)
	return nil
}
