// SPDX-License-Identifier: Apache-2.0

package migrations

import (
	"context"
	"fmt"

	"github.com/xataio/pgroll/pkg/backfill"
	"github.com/xataio/pgroll/pkg/db"
	"github.com/xataio/pgroll/pkg/schema"
)

var (
	_ Operation  = (*OpDropTable)(nil)
	_ Createable = (*OpDropTable)(nil)
)

func (o *OpDropTable) Start(ctx context.Context, l Logger, conn db.DB, latestSchema string, s *schema.Schema) (*backfill.Job, error) {
	l.LogOperationStart(o)

	table := s.GetTable(o.Name)
	if table == nil {
		return nil, TableDoesNotExistError{Name: o.Name}
	}

	// Soft-delete the table in order that a create table operation in the same
	// migration can create a table with the same name
	err := NewRenameTableAction(conn, table.Name, DeletionName(table.Name)).Execute(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to rename table %s: %w", o.Name, err)
	}

	s.RemoveTable(o.Name)
	return nil, nil
}

func (o *OpDropTable) Complete(ctx context.Context, l Logger, conn db.DB, s *schema.Schema) error {
	l.LogOperationComplete(o)

	deletionName := DeletionName(o.Name)

	// Perform the actual deletion of the soft-deleted table
	return NewDropTableAction(conn, deletionName).Execute(ctx)
}

func (o *OpDropTable) Rollback(ctx context.Context, l Logger, conn db.DB, s *schema.Schema) error {
	l.LogOperationRollback(o)

	// Mark the table as no longer deleted so that it is visible to preceding
	// Rollbacks in the same migration
	s.UnRemoveTable(o.Name)

	// Rename the table back to its original name from its soft-deleted name
	table := s.GetTable(o.Name)
	err := NewRenameTableAction(conn, DeletionName(table.Name), table.Name).Execute(ctx)
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
