// SPDX-License-Identifier: Apache-2.0

package migrations

import (
	"context"
	"fmt"
	"strconv"

	"github.com/lib/pq"

	"github.com/pterm/pterm"
	"github.com/xataio/pgroll/pkg/backfill"
	"github.com/xataio/pgroll/pkg/db"
	"github.com/xataio/pgroll/pkg/schema"
)

var _ Operation = (*OpAlterColumn)(nil)

func (o *OpAlterColumn) Start(ctx context.Context, l Logger, conn db.DB, latestSchema string, s *schema.Schema) (*schema.Table, error) {
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
	if err := d.Duplicate(ctx); err != nil {
		return nil, fmt.Errorf("failed to duplicate column: %w", err)
	}

	// Add a trigger to copy values from the old column to the new, rewriting values using the `up` SQL.
	err := NewCreateTriggerAction(conn,
		triggerConfig{
			Name:           TriggerName(o.Table, o.Column),
			Direction:      TriggerDirectionUp,
			Columns:        table.Columns,
			SchemaName:     s.Name,
			LatestSchema:   latestSchema,
			TableName:      table.Name,
			PhysicalColumn: TemporaryName(o.Column),
			SQL:            o.upSQLForOperations(ops),
		},
	).Execute(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to create up trigger: %w", err)
	}

	// Add the new column to the internal schema representation. This is done
	// here, before creation of the down trigger, so that the trigger can declare
	// a variable for the new column. Save the old column name for use as the
	// physical column name. in the down trigger first.
	oldPhysicalColumn := column.Name
	table.AddColumn(o.Column, &schema.Column{
		Name: TemporaryName(o.Column),
	})

	// Add a trigger to copy values from the new column to the old.
	err = NewCreateTriggerAction(conn,
		triggerConfig{
			Name:           TriggerName(o.Table, TemporaryName(o.Column)),
			Direction:      TriggerDirectionDown,
			Columns:        table.Columns,
			LatestSchema:   latestSchema,
			SchemaName:     s.Name,
			TableName:      table.Name,
			PhysicalColumn: oldPhysicalColumn,
			SQL:            o.downSQLForOperations(ops),
		},
	).Execute(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to create down trigger: %w", err)
	}

	// perform any operation specific start steps
	for _, op := range ops {
		if _, err := op.Start(ctx, l, conn, latestSchema, s); err != nil {
			return nil, err
		}
	}

	return table, nil
}

func (o *OpAlterColumn) Complete(ctx context.Context, l Logger, conn db.DB, s *schema.Schema) error {
	l.LogOperationComplete(o)

	ops := o.subOperations()

	// Perform any operation specific completion steps
	for _, op := range ops {
		if err := op.Complete(ctx, l, conn, s); err != nil {
			return err
		}
	}

	if err := alterSequenceOwnerToDuplicatedColumn(ctx, conn, o.Table, o.Column); err != nil {
		return err
	}

	removeOldColumn := NewDropColumnAction(conn, o.Table, o.Column)
	err := removeOldColumn.Execute(ctx)
	if err != nil {
		return err
	}

	// Remove the up and down function and trigger
	err = NewDropFunctionAction(conn, TriggerFunctionName(o.Table, o.Column), TriggerFunctionName(o.Table, TemporaryName(o.Column))).Execute(ctx)
	if err != nil {
		return err
	}

	removeBackfillColumn := NewDropColumnAction(conn, o.Table, backfill.CNeedsBackfillColumn)
	err = removeBackfillColumn.Execute(ctx)
	if err != nil {
		return err
	}

	// Rename the new column to the old column name
	table := s.GetTable(o.Table)
	if table == nil {
		return TableDoesNotExistError{Name: o.Table}
	}
	column := table.GetColumn(o.Column)
	if column == nil {
		return ColumnDoesNotExistError{Table: o.Table, Name: o.Column}
	}
	if err := RenameDuplicatedColumn(ctx, conn, table, column); err != nil {
		return err
	}

	return nil
}

func (o *OpAlterColumn) Rollback(ctx context.Context, l Logger, conn db.DB, s *schema.Schema) error {
	l.LogOperationRollback(o)

	table := s.GetTable(o.Table)
	if table == nil {
		return TableDoesNotExistError{Name: o.Table}
	}
	column := table.GetColumn(o.Column)
	if column == nil {
		return ColumnDoesNotExistError{Table: o.Table, Name: o.Column}
	}

	// Perform any operation specific rollback steps
	ops := o.subOperations()
	for _, ops := range ops {
		if err := ops.Rollback(ctx, l, conn, nil); err != nil {
			return err
		}
	}

	rollbackAddColumn := NewDropColumnAction(conn, table.Name, column.Name)
	err := rollbackAddColumn.Execute(ctx)
	if err != nil {
		return err
	}

	// Remove the up function and trigger
	_, err = conn.ExecContext(ctx, fmt.Sprintf("DROP FUNCTION IF EXISTS %s CASCADE",
		pq.QuoteIdentifier(TriggerFunctionName(o.Table, o.Column)),
	))
	if err != nil {
		return err
	}

	// Remove the down function and trigger
	_, err = conn.ExecContext(ctx, fmt.Sprintf("DROP FUNCTION IF EXISTS %s CASCADE",
		pq.QuoteIdentifier(TriggerFunctionName(o.Table, TemporaryName(o.Column))),
	))
	if err != nil {
		return err
	}

	removeBackfillColumn := NewDropColumnAction(conn, table.Name, backfill.CNeedsBackfillColumn)
	return removeBackfillColumn.Execute(ctx)
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
func duplicatorForOperations(ops []Operation, conn db.DB, table *schema.Table, column *schema.Column) *Duplicator {
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

func (o *OpAlterColumn) Create() {
	o.Table, _ = pterm.DefaultInteractiveTextInput.WithDefaultText("table").Show()
	o.Column, _ = pterm.DefaultInteractiveTextInput.WithDefaultText("column").Show()
	newType, _ := pterm.DefaultInteractiveTextInput.WithDefaultText("type").Show()
	if newType != "" {
		o.Type = &newType
	}
	unique, _ := pterm.DefaultInteractiveTextInput.WithDefaultText("unique_constraint").Show()
	if unique != "" {
		o.Unique = &UniqueConstraint{Name: unique}
	}
	nullableStr, _ := pterm.DefaultInteractiveTextInput.WithDefaultText("nullable").Show()
	if nullableStr != "" {
		nullable, _ := strconv.ParseBool(nullableStr)
		o.Nullable = &nullable
	}
	addCheckConstraint, _ := pterm.DefaultInteractiveConfirm.
		WithDefaultText("Add check constraint").
		WithDefaultValue(false).
		Show()
	if addCheckConstraint {
		var c CheckConstraint
		c.Name, _ = pterm.DefaultInteractiveTextInput.WithDefaultText("name").Show()
		c.Constraint, _ = pterm.DefaultInteractiveTextInput.WithDefaultText("constraint").Show()
		c.NoInherit, _ = pterm.DefaultInteractiveConfirm.WithDefaultText("no_inherit").WithDefaultValue(false).Show()
		o.Check = &c
	}
	addForeignKey, _ := pterm.DefaultInteractiveConfirm.
		WithDefaultText("Add foreign key constraint").
		WithDefaultValue(false).
		Show()
	if addForeignKey {
		var r ForeignKeyReference
		r.Name, _ = pterm.DefaultInteractiveTextInput.WithDefaultText("name").Show()
		r.Table, _ = pterm.DefaultInteractiveTextInput.WithDefaultText("table").Show()
		r.Column, _ = pterm.DefaultInteractiveTextInput.WithDefaultText("column").Show()
		r.Deferrable, _ = pterm.DefaultInteractiveConfirm.WithDefaultText("deferrable").WithDefaultValue(false).Show()
		r.InitiallyDeferred, _ = pterm.DefaultInteractiveConfirm.WithDefaultText("initially_deferred").WithDefaultValue(false).Show()
		onDelete, _ := pterm.DefaultInteractiveSelect.
			WithDefaultText("on_delete").
			WithOptions([]string{"CASCADE", "SET NULL", "RESTRICT", "NO ACTION"}).
			WithDefaultOption("NO ACTION").
			Show()
		r.OnDelete = ForeignKeyAction(onDelete)
		onUpdate, _ := pterm.DefaultInteractiveSelect.
			WithDefaultText("on_update").
			WithOptions([]string{"CASCADE", "SET NULL", "RESTRICT", "NO ACTION"}).
			WithDefaultOption("NO ACTION").
			Show()
		r.OnUpdate = ForeignKeyAction(onUpdate)
		o.References = &r
	}
	o.Up, _ = pterm.DefaultInteractiveTextInput.WithDefaultText("up").Show()
	o.Down, _ = pterm.DefaultInteractiveTextInput.WithDefaultText("down").Show()
}
