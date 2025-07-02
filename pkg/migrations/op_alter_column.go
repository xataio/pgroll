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

var (
	_ Operation  = (*OpAlterColumn)(nil)
	_ Createable = (*OpAlterColumn)(nil)
)

func (o *OpAlterColumn) Start(ctx context.Context, l Logger, conn db.DB, s *schema.Schema) (*StartOperation, error) {
	l.LogOperationStart(o)

	table := s.GetTable(o.Table)
	if table == nil {
		return nil, TableDoesNotExistError{Name: o.Table}
	}
	column := table.GetColumn(o.Column)
	if column == nil {
		return nil, ColumnDoesNotExistError{Table: o.Table, Name: o.Column}
	}
	ops := o.subOperations()

	// Duplicate the column on the underlying table.
	d := duplicatorForOperations(ops, conn, table, column).
		WithName(column.Name, TemporaryName(o.Column))
	if err := d.Execute(ctx); err != nil {
		return nil, fmt.Errorf("failed to duplicate column: %w", err)
	}

	// Copy the columns from table columns, so we can use it later
	// in the down trigger with the physical name
	upColumns := make(map[string]*schema.Column)
	for name, col := range table.Columns {
		upColumns[name] = col
	}

	// Add a trigger to copy values from the old column to the new, rewriting values using the `up` SQL.
	triggers := make([]backfill.OperationTrigger, 0)
	triggers = append(triggers,
		backfill.OperationTrigger{
			Name:           backfill.TriggerName(o.Table, o.Column),
			Direction:      backfill.TriggerDirectionUp,
			TableName:      table.Name,
			Columns:        upColumns,
			PhysicalColumn: TemporaryName(o.Column),
			SQL:            o.upSQLForOperations(ops),
		},
	)

	// Add the new column to the internal schema representation. This is done
	// here, before creation of the down trigger, so that the trigger can declare
	// a variable for the new column. Save the old column name for use as the
	// physical column name. in the down trigger first.
	oldPhysicalColumn := column.Name
	table.AddColumn(o.Column, &schema.Column{
		Name: TemporaryName(o.Column),
	})

	// Add a trigger to copy values from the new column to the old.
	triggers = append(triggers,
		backfill.OperationTrigger{
			Name:           backfill.TriggerName(o.Table, TemporaryName(o.Column)),
			Direction:      backfill.TriggerDirectionDown,
			TableName:      table.Name,
			Columns:        table.Columns,
			PhysicalColumn: oldPhysicalColumn,
			SQL:            o.downSQLForOperations(ops),
		},
	)
	task := backfill.NewTask(table, triggers...)

	var dbActions []DBAction
	// perform any operation specific start steps
	for _, op := range ops {
		startOp, err := op.Start(ctx, l, conn, s)
		if err != nil {
			return nil, err
		}
		task.AddTriggers(startOp.BackfillTask)
		dbActions = append(dbActions, startOp.Actions...)
	}

	return &StartOperation{Actions: dbActions, BackfillTask: task}, nil
}

func (o *OpAlterColumn) Complete(l Logger, conn db.DB, s *schema.Schema) ([]DBAction, error) {
	l.LogOperationComplete(o)

	ops := o.subOperations()

	dbActions := make([]DBAction, 0)
	// Perform any operation specific completion steps
	for _, op := range ops {
		actions, err := op.Complete(l, conn, s)
		if err != nil {
			return nil, err
		}
		dbActions = append(dbActions, actions...)
	}

	// Rename the new column to the old column name
	table := s.GetTable(o.Table)
	if table == nil {
		return nil, TableDoesNotExistError{Name: o.Table}
	}
	column := table.GetColumn(o.Column)
	if column == nil {
		return nil, ColumnDoesNotExistError{Table: o.Table, Name: o.Column}
	}

	return append(dbActions, []DBAction{
		NewAlterSequenceOwnerAction(conn, o.Table, o.Column, TemporaryName(o.Column)),
		NewDropColumnAction(conn, o.Table, o.Column),
		NewDropFunctionAction(conn,
			backfill.TriggerFunctionName(o.Table, o.Column),
			backfill.TriggerFunctionName(o.Table, TemporaryName(o.Column)),
		),
		NewDropColumnAction(conn, o.Table, backfill.CNeedsBackfillColumn),
		NewRenameDuplicatedColumnAction(conn, table, column.Name),
	}...), nil
}

func (o *OpAlterColumn) Rollback(l Logger, conn db.DB, s *schema.Schema) ([]DBAction, error) {
	l.LogOperationRollback(o)

	table := s.GetTable(o.Table)
	if table == nil {
		return nil, TableDoesNotExistError{Name: o.Table}
	}
	column := table.GetColumn(o.Column)
	if column == nil {
		return nil, ColumnDoesNotExistError{Table: o.Table, Name: o.Column}
	}

	// Perform any operation specific rollback steps
	dbActions := make([]DBAction, 0)
	ops := o.subOperations()
	for _, ops := range ops {
		actions, err := ops.Rollback(l, conn, nil)
		if err != nil {
			return nil, err
		}
		dbActions = append(dbActions, actions...)
	}

	dbActions = append(dbActions,
		NewDropColumnAction(conn, table.Name, column.Name),
		NewDropFunctionAction(conn,
			backfill.TriggerFunctionName(o.Table, o.Column),
			backfill.TriggerFunctionName(o.Table, TemporaryName(o.Column)),
		),
		NewDropColumnAction(conn, table.Name, backfill.CNeedsBackfillColumn),
	)

	return dbActions, nil
}

func (o *OpAlterColumn) Validate(ctx context.Context, s *schema.Schema) error {
	// Validate that the table and column exist
	table := s.GetTable(o.Table)
	if table == nil {
		return TableDoesNotExistError{Name: o.Table}
	}
	if table.GetColumn(o.Column) == nil {
		return ColumnDoesNotExistError{Table: o.Table, Name: o.Column}
	}

	ops := o.subOperations()

	// Ensure that at least one sub-operation or rename is present
	if len(ops) == 0 {
		return AlterColumnNoChangesError{Table: o.Table, Column: o.Column}
	}

	// Validate the sub-operations in isolation
	for _, op := range ops {
		if err := op.Validate(ctx, s); err != nil {
			return err
		}
	}

	return nil
}

func (o *OpAlterColumn) subOperations() []Operation {
	var ops []Operation

	if o.Type != nil {
		ops = append(ops, &OpChangeType{
			Table:  o.Table,
			Column: o.Column,
			Type:   *o.Type,
			Up:     o.Up,
			Down:   o.Down,
		})
	}
	if o.Check != nil {
		ops = append(ops, &OpSetCheckConstraint{
			Table:  o.Table,
			Column: o.Column,
			Check:  *o.Check,
			Up:     o.Up,
			Down:   o.Down,
		})
	}
	if o.References != nil {
		ops = append(ops, &OpSetForeignKey{
			Table:      o.Table,
			Column:     o.Column,
			References: *o.References,
			Up:         o.Up,
			Down:       o.Down,
		})
	}
	if o.Nullable != nil && !*o.Nullable {
		ops = append(ops, &OpSetNotNull{
			Table:  o.Table,
			Column: o.Column,
			Up:     o.Up,
			Down:   o.Down,
		})
	}
	if o.Nullable != nil && *o.Nullable {
		ops = append(ops, &OpDropNotNull{
			Table:  o.Table,
			Column: o.Column,
			Up:     o.Up,
			Down:   o.Down,
		})
	}
	if o.Unique != nil {
		ops = append(ops, &OpSetUnique{
			Table:  o.Table,
			Column: o.Column,
			Name:   o.Unique.Name,
			Up:     o.Up,
			Down:   o.Down,
		})
	}
	if o.Default.IsSpecified() {
		// o.Default is either a valid value or `null`.
		var defaultPtr *string
		if d, err := o.Default.Get(); err == nil {
			defaultPtr = &d
		}

		ops = append(ops, &OpSetDefault{
			Table:   o.Table,
			Column:  o.Column,
			Default: defaultPtr,
			Up:      o.Up,
			Down:    o.Down,
		})
	}
	if o.Comment.IsSpecified() {
		var comment *string
		if c, err := o.Comment.Get(); err == nil {
			comment = &c
		}

		ops = append(ops, &OpSetComment{
			Table:   o.Table,
			Column:  o.Column,
			Comment: comment,
			Up:      o.Up,
			Down:    o.Down,
		})
	}

	return ops
}

// duplicatorForOperations returns a Duplicator for the given operations
func duplicatorForOperations(ops []Operation, conn db.DB, table *schema.Table, column *schema.Column) *duplicator {
	d := NewColumnDuplicator(conn, table, column)

	for _, op := range ops {
		switch op := (op).(type) {
		case *OpDropNotNull:
			d = d.WithoutNotNull(column.Name)
		case *OpChangeType:
			d = d.WithType(column.Name, op.Type)
		}
	}
	return d
}

// downSQLForOperations returns the `down` SQL for the given operations, applying
// an appropriate default if no `down` SQL is provided.
func (o *OpAlterColumn) downSQLForOperations(ops []Operation) string {
	if o.Down != "" {
		return o.Down
	}

	for _, op := range ops {
		switch (op).(type) {
		case *OpSetUnique, *OpSetNotNull, *OpSetDefault, *OpSetComment:
			return pq.QuoteIdentifier(o.Column)
		}
	}

	return ""
}

// upSQLForOperations returns the `up` SQL for the given operations, applying
// an appropriate default if no `up` SQL is provided.
func (o *OpAlterColumn) upSQLForOperations(ops []Operation) string {
	if o.Up != "" {
		return o.Up
	}

	for _, op := range ops {
		switch (op).(type) {
		case *OpDropNotNull, *OpSetDefault, *OpSetComment:
			return pq.QuoteIdentifier(o.Column)
		}
	}

	return ""
}
