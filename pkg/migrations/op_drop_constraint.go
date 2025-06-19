// SPDX-License-Identifier: Apache-2.0

package migrations

import (
	"context"
	"fmt"

	"github.com/lib/pq"

	"github.com/xataio/pgroll/pkg/backfill"
	"github.com/xataio/pgroll/pkg/db"
	"github.com/xataio/pgroll/pkg/schema"
)

var _ Operation = (*OpDropConstraint)(nil)

func (o *OpDropConstraint) Start(ctx context.Context, l Logger, conn db.DB, latestSchema string, s *schema.Schema) (*schema.Table, error) {
	l.LogOperationStart(o)

	table := s.GetTable(o.Table)
	if table == nil {
		return nil, TableDoesNotExistError{Name: o.Table}
	}

	// By this point Validate() should have run which ensures the constraint exists and that we only have
	// one column associated with it.
	column := table.GetColumn(table.GetConstraintColumns(o.Name)[0])
	if column == nil {
		return nil, ColumnDoesNotExistError{Table: o.Table, Name: table.GetConstraintColumns(o.Name)[0]}
	}

	// Create a copy of the column on the underlying table.
	d := NewColumnDuplicator(conn, table, column).WithoutConstraint(o.Name)
	if err := d.Duplicate(ctx); err != nil {
		return nil, fmt.Errorf("failed to duplicate column: %w", err)
	}

	// Add a trigger to copy values from the old column to the new, rewriting values using the `up` SQL.
	err := NewCreateTriggerAction(conn,
		backfill.TriggerConfig{
			Name:           backfill.TriggerName(o.Table, column.Name),
			Direction:      backfill.TriggerDirectionUp,
			Columns:        table.Columns,
			SchemaName:     s.Name,
			LatestSchema:   latestSchema,
			TableName:      o.Table,
			PhysicalColumn: TemporaryName(column.Name),
			SQL:            o.upSQL(column.Name),
		},
	).Execute(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to create up trigger: %w", err)
	}

	// Add the new column to the internal schema representation. This is done
	// here, before creation of the down trigger, so that the trigger can declare
	// a variable for the new column.
	table.AddColumn(column.Name, &schema.Column{
		Name: TemporaryName(column.Name),
	})

	// Add a trigger to copy values from the new column to the old, rewriting values using the `down` SQL.
	err = NewCreateTriggerAction(conn,
		backfill.TriggerConfig{
			Name:           backfill.TriggerName(o.Table, TemporaryName(column.Name)),
			Direction:      backfill.TriggerDirectionDown,
			Columns:        table.Columns,
			SchemaName:     s.Name,
			LatestSchema:   latestSchema,
			TableName:      o.Table,
			PhysicalColumn: column.Name,
			SQL:            o.Down,
		},
	).Execute(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to create down trigger: %w", err)
	}
	return table, nil
}

func (o *OpDropConstraint) Complete(ctx context.Context, l Logger, conn db.DB, s *schema.Schema) error {
	l.LogOperationComplete(o)

	// We have already validated that there is single column related to this constraint.
	table := s.GetTable(o.Table)
	column := table.GetColumn(table.GetConstraintColumns(o.Name)[0])

	// Remove the up and down function and trigger
	err := NewDropFunctionAction(conn, backfill.TriggerFunctionName(o.Table, column.Name), backfill.TriggerFunctionName(o.Table, TemporaryName(column.Name))).Execute(ctx)
	if err != nil {
		return err
	}

	if err := NewAlterSequenceOwnerAction(conn, o.Table, column.Name, TemporaryName(column.Name)).Execute(ctx); err != nil {
		return err
	}

	removeBackfillColumn := NewDropColumnAction(conn, table.Name, backfill.CNeedsBackfillColumn)
	err = removeBackfillColumn.Execute(ctx)
	if err != nil {
		return err
	}

	removeOldColumn := NewDropColumnAction(conn,
		o.Table,
		column.Name)
	err = removeOldColumn.Execute(ctx)
	if err != nil {
		return err
	}

	// Rename the new column to the old column name
	if err := NewRenameDuplicatedColumnAction(conn, table, column.Name).Execute(ctx); err != nil {
		return err
	}

	return err
}

func (o *OpDropConstraint) Rollback(ctx context.Context, l Logger, conn db.DB, s *schema.Schema) error {
	l.LogOperationRollback(o)

	// We have already validated that there is single column related to this constraint.
	table := s.GetTable(o.Table)
	columnName := table.GetConstraintColumns(o.Name)[0]

	removeNewColumn := NewDropColumnAction(conn, o.Table, TemporaryName(columnName))
	err := removeNewColumn.Execute(ctx)
	if err != nil {
		return err
	}

	// Remove the up and down functions and triggers
	if err := NewDropFunctionAction(
		conn,
		backfill.TriggerFunctionName(o.Table, columnName),
		backfill.TriggerFunctionName(o.Table, TemporaryName(columnName)),
	).Execute(ctx); err != nil {
		return err
	}

	removeBackfillColumn := NewDropColumnAction(conn, table.Name, backfill.CNeedsBackfillColumn)
	err = removeBackfillColumn.Execute(ctx)

	return err
}

func (o *OpDropConstraint) Validate(ctx context.Context, s *schema.Schema) error {
	table := s.GetTable(o.Table)
	if table == nil {
		return TableDoesNotExistError{Name: o.Table}
	}

	if o.Name == "" {
		return FieldRequiredError{Name: "name"}
	}

	if !table.ConstraintExists(o.Name) {
		return ConstraintDoesNotExistError{Table: o.Table, Constraint: o.Name}
	}

	columns := table.GetConstraintColumns(o.Name)

	// We already know the constraint exists because we checked it earlier so we only need to check the
	// case where there are multiple columns.
	if len(columns) > 1 {
		return MultiColumnConstraintsNotSupportedError{
			Table:      table.Name,
			Constraint: o.Name,
		}
	}

	if o.Down == "" {
		return FieldRequiredError{Name: "down"}
	}

	return nil
}

func (o *OpDropConstraint) upSQL(column string) string {
	if o.Up != "" {
		return o.Up
	}

	return pq.QuoteIdentifier(column)
}
