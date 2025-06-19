// SPDX-License-Identifier: Apache-2.0

package migrations

import (
	"context"

	"github.com/xataio/pgroll/pkg/backfill"
	"github.com/xataio/pgroll/pkg/db"
	"github.com/xataio/pgroll/pkg/schema"
)

var _ Operation = (*OpRenameColumn)(nil)

func (o *OpRenameColumn) Start(ctx context.Context, l Logger, conn db.DB, latestSchema string, s *schema.Schema) (*backfill.Task, error) {
	l.LogOperationStart(o)

	// Rename the table in the in-memory schema.
	table := s.GetTable(o.Table)
	if table == nil {
		return nil, TableDoesNotExistError{Name: o.Table}
	}
	column := table.GetColumn(o.From)
	if column == nil {
		return nil, ColumnDoesNotExistError{Table: o.Table, Name: o.From}
	}
	table.RenameColumn(o.From, o.To)

	// Update the name of the column in any constraints that reference the
	// renamed column.
	table.RenameConstraintColumns(o.From, o.To)

	return nil, nil
}

func (o *OpRenameColumn) Complete(ctx context.Context, l Logger, conn db.DB, s *schema.Schema) error {
	l.LogOperationComplete(o)
	return NewRenameColumnAction(conn, o.Table, o.From, o.To).Execute(ctx)
}

func (o *OpRenameColumn) Rollback(ctx context.Context, l Logger, conn db.DB, s *schema.Schema) error {
	l.LogOperationRollback(o)

	// Rename the column back to the original name in the in-memory schema.
	table := s.GetTable(o.Table)
	table.RenameColumn(o.To, o.From)

	return nil
}

func (o *OpRenameColumn) Validate(ctx context.Context, s *schema.Schema) error {
	table := s.GetTable(o.Table)

	// Ensure that the `from` field is not empty
	if o.From == "" {
		return FieldRequiredError{Name: "from"}
	}

	// Ensure that the `to` field is not empty
	if o.To == "" {
		return FieldRequiredError{Name: "to"}
	}

	// Ensure that the table exists.
	if table == nil {
		return TableDoesNotExistError{Name: o.Table}
	}

	// Ensure that the column exists.
	if table.GetColumn(o.From) == nil {
		return ColumnDoesNotExistError{Table: o.Table, Name: o.From}
	}

	// Ensure that the new column name does not already exist
	if table.GetColumn(o.To) != nil {
		return ColumnAlreadyExistsError{Table: o.Table, Name: o.To}
	}

	// Update the in-memory schema to reflect the column rename so that it is
	// visible to subsequent operations' validation steps.
	table.RenameColumn(o.From, o.To)
	table.RenameConstraintColumns(o.From, o.To)

	return nil
}
