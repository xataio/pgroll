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
	_ Operation  = (*OpCreateConstraint)(nil)
	_ Createable = (*OpCreateConstraint)(nil)
)

func (o *OpCreateConstraint) Start(ctx context.Context, l Logger, conn db.DB, latestSchema string, s *schema.Schema) (*backfill.Task, error) {
	l.LogOperationStart(o)

	table := s.GetTable(o.Table)
	if table == nil {
		return nil, TableDoesNotExistError{Name: o.Table}
	}

	columns := make([]*schema.Column, len(o.Columns))
	for i, colName := range o.Columns {
		columns[i] = table.GetColumn(colName)
		if columns[i] == nil {
			return nil, ColumnDoesNotExistError{Table: o.Table, Name: colName}
		}
	}

	// Duplicate each column using its final name after migration completion
	d := NewColumnDuplicator(conn, table, columns...)
	for _, colName := range o.Columns {
		d = d.WithName(table.GetColumn(colName).Name, TemporaryName(colName))
	}
	if err := d.Duplicate(ctx); err != nil {
		return nil, fmt.Errorf("failed to duplicate columns for new constraint: %w", err)
	}

	// Setup triggers
	for _, colName := range o.Columns {
		upSQL := o.Up[colName]
		err := NewCreateTriggerAction(conn,
			backfill.TriggerConfig{
				Name:           backfill.TriggerName(o.Table, colName),
				Direction:      backfill.TriggerDirectionUp,
				Columns:        table.Columns,
				SchemaName:     s.Name,
				LatestSchema:   latestSchema,
				TableName:      table.Name,
				PhysicalColumn: TemporaryName(colName),
				SQL:            upSQL,
			},
		).Execute(ctx)
		if err != nil {
			return nil, fmt.Errorf("failed to create up trigger: %w", err)
		}

		// Add the new column to the internal schema representation. This is done
		// here, before creation of the down trigger, so that the trigger can declare
		// a variable for the new column. Save the old column name for use as the
		// physical column name in the down trigger first.
		oldPhysicalColumn := table.GetColumn(colName).Name
		table.AddColumn(colName, &schema.Column{
			Name: TemporaryName(colName),
		})

		downSQL := o.Down[colName]
		err = NewCreateTriggerAction(conn,
			backfill.TriggerConfig{
				Name:           backfill.TriggerName(o.Table, TemporaryName(colName)),
				Direction:      backfill.TriggerDirectionDown,
				Columns:        table.Columns,
				LatestSchema:   latestSchema,
				SchemaName:     s.Name,
				TableName:      table.Name,
				PhysicalColumn: oldPhysicalColumn,
				SQL:            downSQL,
			},
		).Execute(ctx)
		if err != nil {
			return nil, fmt.Errorf("failed to create down trigger: %w", err)
		}
	}

	task := &backfill.Task{
		Table: table,
	}

	switch o.Type {
	case OpCreateConstraintTypeUnique, OpCreateConstraintTypePrimaryKey:
		return task, NewCreateUniqueIndexConcurrentlyAction(conn, s.Name, o.Name, table.Name, temporaryNames(o.Columns)...).Execute(ctx)
	case OpCreateConstraintTypeCheck:
		return task, NewCreateCheckConstraintAction(conn, table.Name, o.Name, *o.Check, o.Columns, o.NoInherit, true).Execute(ctx)
	case OpCreateConstraintTypeForeignKey:
		return task, NewCreateFKConstraintAction(conn, table.Name, o.Name, temporaryNames(o.Columns), o.References, false, false, true).Execute(ctx)
	}

	return task, nil
}

func (o *OpCreateConstraint) Complete(ctx context.Context, l Logger, conn db.DB, s *schema.Schema) error {
	l.LogOperationComplete(o)

	switch o.Type {
	case OpCreateConstraintTypeUnique:
		uniqueOp := &OpSetUnique{
			Table: o.Table,
			Name:  o.Name,
		}
		err := uniqueOp.Complete(ctx, l, conn, s)
		if err != nil {
			return err
		}
	case OpCreateConstraintTypeCheck:
		checkOp := &OpSetCheckConstraint{
			Table: o.Table,
			Check: CheckConstraint{
				Name: o.Name,
			},
		}
		err := checkOp.Complete(ctx, l, conn, s)
		if err != nil {
			return err
		}
	case OpCreateConstraintTypeForeignKey:
		fkOp := &OpSetForeignKey{
			Table: o.Table,
			References: ForeignKeyReference{
				Name: o.Name,
			},
		}
		err := fkOp.Complete(ctx, l, conn, s)
		if err != nil {
			return err
		}
	case OpCreateConstraintTypePrimaryKey:
		err := NewAddPrimaryKeyAction(conn, o.Table, o.Name).Execute(ctx)
		if err != nil {
			return err
		}
	}

	for _, col := range o.Columns {
		if err := NewAlterSequenceOwnerAction(conn, o.Table, col, TemporaryName(col)).Execute(ctx); err != nil {
			return err
		}
	}

	removeOldColumns := NewDropColumnAction(conn, o.Table, o.Columns...)
	err := removeOldColumns.Execute(ctx)
	if err != nil {
		return err
	}

	// rename new columns to old name
	table := s.GetTable(o.Table)
	if table == nil {
		return TableDoesNotExistError{Name: o.Table}
	}
	for _, col := range o.Columns {
		column := table.GetColumn(col)
		if column == nil {
			return ColumnDoesNotExistError{Table: o.Table, Name: col}
		}
		if err := NewRenameDuplicatedColumnAction(conn, table, column.Name).Execute(ctx); err != nil {
			return err
		}
	}

	if err := o.removeTriggers(ctx, conn); err != nil {
		return err
	}

	removeBackfillColumn := NewDropColumnAction(conn, o.Table, backfill.CNeedsBackfillColumn)
	err = removeBackfillColumn.Execute(ctx)

	return err
}

func (o *OpCreateConstraint) Rollback(ctx context.Context, l Logger, conn db.DB, s *schema.Schema) error {
	l.LogOperationRollback(o)

	table := s.GetTable(o.Table)
	if table == nil {
		return TableDoesNotExistError{Name: o.Table}
	}

	removeDuplicatedColumns := NewDropColumnAction(conn, table.Name, temporaryNames(o.Columns)...)
	err := removeDuplicatedColumns.Execute(ctx)
	if err != nil {
		return err
	}

	if err := o.removeTriggers(ctx, conn); err != nil {
		return err
	}

	removeBackfillColumn := NewDropColumnAction(conn, table.Name, backfill.CNeedsBackfillColumn)
	err = removeBackfillColumn.Execute(ctx)

	return err
}

func (o *OpCreateConstraint) removeTriggers(ctx context.Context, conn db.DB) error {
	dropFuncs := make([]string, 0, len(o.Columns)*2)
	for _, column := range o.Columns {
		dropFuncs = append(dropFuncs, backfill.TriggerFunctionName(o.Table, column))
		dropFuncs = append(dropFuncs, backfill.TriggerFunctionName(o.Table, TemporaryName(column)))
	}
	return NewDropFunctionAction(conn, dropFuncs...).Execute(ctx)
}

func (o *OpCreateConstraint) Validate(ctx context.Context, s *schema.Schema) error {
	table := s.GetTable(o.Table)
	if table == nil {
		return TableDoesNotExistError{Name: o.Table}
	}

	if err := ValidateIdentifierLength(o.Name); err != nil {
		return err
	}

	if table.ConstraintExists(o.Name) {
		return ConstraintAlreadyExistsError{
			Table:      o.Table,
			Constraint: o.Name,
		}
	}

	for _, col := range o.Columns {
		if table.GetColumn(col) == nil {
			return ColumnDoesNotExistError{
				Table: o.Table,
				Name:  col,
			}
		}
		if _, ok := o.Up[col]; !ok {
			return ColumnMigrationMissingError{
				Table: o.Table,
				Name:  col,
			}
		}
		if _, ok := o.Down[col]; !ok {
			return ColumnMigrationMissingError{
				Table: o.Table,
				Name:  col,
			}
		}
	}

	switch o.Type {
	case OpCreateConstraintTypeUnique:
		if len(o.Columns) == 0 {
			return FieldRequiredError{Name: "columns"}
		}
	case OpCreateConstraintTypeCheck:
		if o.Check == nil || *o.Check == "" {
			return FieldRequiredError{Name: "check"}
		}
	case OpCreateConstraintTypeForeignKey:
		if o.References == nil {
			return FieldRequiredError{Name: "references"}
		}
		table := s.GetTable(o.References.Table)
		if table == nil {
			return TableDoesNotExistError{Name: o.References.Table}
		}
		for _, col := range o.References.Columns {
			if table.GetColumn(col) == nil {
				return ColumnDoesNotExistError{
					Table: o.References.Table,
					Name:  col,
				}
			}
		}
	}

	return nil
}

func temporaryNames(columns []string) []string {
	names := make([]string, len(columns))
	for i, col := range columns {
		names[i] = TemporaryName(col)
	}
	return names
}
