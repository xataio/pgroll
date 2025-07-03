// SPDX-License-Identifier: Apache-2.0

package migrations

import (
	"context"

	"github.com/xataio/pgroll/pkg/db"
	"github.com/xataio/pgroll/pkg/schema"
)

var (
	_ Operation  = (*OpDropTable)(nil)
	_ Createable = (*OpDropTable)(nil)
)

func (o *OpDropTable) Start(ctx context.Context, l Logger, conn db.DB, s *schema.Schema) (*StartResult, error) {
	l.LogOperationStart(o)

	table := s.GetTable(o.Name)
	if table == nil {
		return nil, TableDoesNotExistError{Name: o.Name}
	}

	s.RemoveTable(o.Name)

	// Soft-delete the table in order that a create table operation in the same
	// migration can create a table with the same name
	dbActions := []DBAction{
		NewRenameTableAction(conn, table.Name, DeletionName(table.Name)),
	}
	return &StartResult{Actions: dbActions}, nil
}

func (o *OpDropTable) Complete(l Logger, conn db.DB, s *schema.Schema) ([]DBAction, error) {
	l.LogOperationComplete(o)

	return []DBAction{
		// Perform the actual deletion of the soft-deleted table
		NewDropTableAction(conn, DeletionName(o.Name)),
	}, nil
}

func (o *OpDropTable) Rollback(l Logger, conn db.DB, s *schema.Schema) ([]DBAction, error) {
	l.LogOperationRollback(o)

	// Mark the table as no longer deleted so that it is visible to preceding
	// Rollbacks in the same migration
	s.UnRemoveTable(o.Name)

	// Rename the table back to its original name from its soft-deleted name
	table := s.GetTable(o.Name)

	return []DBAction{
		NewRenameTableAction(conn, DeletionName(table.Name), table.Name),
	}, nil
}

func (o *OpDropTable) Validate(ctx context.Context, s *schema.Schema) error {
	table := s.GetTable(o.Name)

	if table == nil {
		return TableDoesNotExistError{Name: o.Name}
	}

	s.RemoveTable(table.Name)
	return nil
}
