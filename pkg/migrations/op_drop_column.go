// SPDX-License-Identifier: Apache-2.0

package migrations

import (
	"context"

	"github.com/pterm/pterm"
	"github.com/xataio/pgroll/pkg/backfill"
	"github.com/xataio/pgroll/pkg/db"
	"github.com/xataio/pgroll/pkg/schema"
)

var _ Operation = (*OpDropColumn)(nil)

func (o *OpDropColumn) Start(ctx context.Context, logger pterm.Logger, conn db.DB, latestSchema string, s *schema.Schema) (*schema.Table, error) {
	logger.Info("starting operation", logger.Args(o.loggerArgs()...))

	if o.Down != "" {
		err := NewCreateTriggerAction(conn,
			triggerConfig{
				Name:           TriggerName(o.Table, o.Column),
				Direction:      TriggerDirectionDown,
				Columns:        s.GetTable(o.Table).Columns,
				SchemaName:     s.Name,
				LatestSchema:   latestSchema,
				TableName:      s.GetTable(o.Table).Name,
				PhysicalColumn: o.Column,
				SQL:            o.Down,
			},
		).Execute(ctx)
		if err != nil {
			return nil, err
		}
	}

	table := s.GetTable(o.Table)
	if table == nil {
		return nil, TableDoesNotExistError{Name: o.Table}
	}
	column := table.GetColumn(o.Column)
	if column == nil {
		return nil, ColumnDoesNotExistError{Table: o.Table, Name: o.Column}
	}

	s.GetTable(o.Table).RemoveColumn(o.Column)
	return nil, nil
}

func (o *OpDropColumn) Complete(ctx context.Context, logger pterm.Logger, conn db.DB, s *schema.Schema) error {
	logger.Info("completing operation", logger.Args(o.loggerArgs()...))

	removeColumn := NewDropColumnAction(conn, o.Table, o.Column)
	err := removeColumn.Execute(ctx)
	if err != nil {
		return err
	}

	err = NewDropFunctionAction(conn, TriggerFunctionName(o.Table, o.Column)).Execute(ctx)
	if err != nil {
		return err
	}

	removeBackfillColumn := NewDropColumnAction(conn, o.Table, backfill.CNeedsBackfillColumn)
	err = removeBackfillColumn.Execute(ctx)
	if err != nil {
		return err
	}

	return nil
}

func (o *OpDropColumn) Rollback(ctx context.Context, logger pterm.Logger, conn db.DB, s *schema.Schema) error {
	logger.Info("rolling back operation", logger.Args(o.loggerArgs()...))

	table := s.GetTable(o.Table)

	err := NewDropFunctionAction(conn, TriggerFunctionName(o.Table, o.Column)).Execute(ctx)
	if err != nil {
		return err
	}

	removeBackfillColumn := NewDropColumnAction(conn, table.Name, backfill.CNeedsBackfillColumn)
	err = removeBackfillColumn.Execute(ctx)
	if err != nil {
		return err
	}

	// Mark the column as no longer deleted so thats it's visible to preceding
	// rollback operations in the same migration
	s.GetTable(o.Table).UnRemoveColumn(o.Column)

	return nil
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

func (o *OpDropColumn) loggerArgs() []any {
	return []any{
		"operation", OpNameDropColumn,
		"column", o.Column,
		"table", o.Table,
	}
}
