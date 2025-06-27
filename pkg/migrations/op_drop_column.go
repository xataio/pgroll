// SPDX-License-Identifier: Apache-2.0

package migrations

import (
	"context"

	"github.com/xataio/pgroll/pkg/backfill"
	"github.com/xataio/pgroll/pkg/db"
	"github.com/xataio/pgroll/pkg/schema"
)

var (
	_ Operation  = (*OpDropColumn)(nil)
	_ Createable = (*OpDropColumn)(nil)
)

func (o *OpDropColumn) Start(ctx context.Context, l Logger, conn db.DB, s *schema.Schema) ([]DBAction, *backfill.Task, error) {
	l.LogOperationStart(o)

	var task *backfill.Task
	if o.Down != "" {
		task = backfill.NewTask(nil,
			backfill.OperationTrigger{
				Name:           backfill.TriggerName(o.Table, o.Column),
				Direction:      backfill.TriggerDirectionDown,
				Columns:        s.GetTable(o.Table).Columns,
				TableName:      s.GetTable(o.Table).Name,
				PhysicalColumn: o.Column,
				SQL:            o.Down,
			},
		)
	}

	table := s.GetTable(o.Table)
	if table == nil {
		return nil, nil, TableDoesNotExistError{Name: o.Table}
	}
	column := table.GetColumn(o.Column)
	if column == nil {
		return nil, nil, ColumnDoesNotExistError{Table: o.Table, Name: o.Column}
	}

	s.GetTable(o.Table).RemoveColumn(o.Column)

	return nil, task, nil
}

func (o *OpDropColumn) Complete(l Logger, conn db.DB, s *schema.Schema) ([]DBAction, error) {
	l.LogOperationComplete(o)

	return []DBAction{
		NewDropColumnAction(conn, o.Table, o.Column),
		NewDropFunctionAction(conn, backfill.TriggerFunctionName(o.Table, o.Column)),
		NewDropColumnAction(conn, o.Table, backfill.CNeedsBackfillColumn),
	}, nil
}

func (o *OpDropColumn) Rollback(l Logger, conn db.DB, s *schema.Schema) ([]DBAction, error) {
	l.LogOperationRollback(o)

	table := s.GetTable(o.Table)

	// Mark the column as no longer deleted so thats it's visible to preceding
	// rollback operations in the same migration
	s.GetTable(o.Table).UnRemoveColumn(o.Column)

	return []DBAction{
		NewDropFunctionAction(conn, backfill.TriggerFunctionName(o.Table, o.Column)),
		NewDropColumnAction(conn, table.Name, backfill.CNeedsBackfillColumn),
	}, nil
}

func (o *OpDropColumn) Validate(ctx context.Context, s *schema.Schema) error {
	table := s.GetTable(o.Table)

	if table == nil {
		return TableDoesNotExistError{Name: o.Table}
	}
	if table.GetColumn(o.Column) == nil {
		return ColumnDoesNotExistError{Table: o.Table, Name: o.Column}
	}
	return nil
}
