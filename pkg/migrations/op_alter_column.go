// SPDX-License-Identifier: Apache-2.0

package migrations

import (
	"context"
	"fmt"
	"maps"

	"github.com/lib/pq"
	"github.com/xataio/pgroll/pkg/db"
	"github.com/xataio/pgroll/pkg/schema"
)

var _ Operation = (*OpAlterColumn)(nil)

func (o *OpAlterColumn) Start(ctx context.Context, conn db.DB, latestSchema string, tr SQLTransformer, s *schema.Schema, cbs ...CallbackFn) (*schema.Table, error) {
	table := s.GetTable(o.Table)
	column := table.GetColumn(o.Column)
	ops := o.subOperations()

	// Duplicate the column on the underlying table.
	if !o.IsRenameOnly() {
		d := duplicatorForOperations(ops, conn, table, column)
		if err := d.Duplicate(ctx); err != nil {
			return nil, fmt.Errorf("failed to duplicate column: %w", err)
		}
	}

	// perform any operation specific start steps
	for _, op := range ops {
		if _, err := op.Start(ctx, conn, latestSchema, tr, s, cbs...); err != nil {
			return nil, err
		}
	}

	// Add a trigger to copy values from the old column to the new, rewriting values using the `up` SQL.
	// Rename column operations do not require this trigger.
	if !o.IsRenameOnly() {
		err := createTrigger(ctx, conn, tr, triggerConfig{
			Name:           TriggerName(o.Table, o.Column),
			Direction:      TriggerDirectionUp,
			Columns:        table.Columns,
			SchemaName:     s.Name,
			LatestSchema:   latestSchema,
			TableName:      o.Table,
			PhysicalColumn: TemporaryName(o.Column),
			SQL:            o.upSQLForOperations(ops),
		})
		if err != nil {
			return nil, fmt.Errorf("failed to create up trigger: %w", err)
		}

		// Add the new column to the internal schema representation. This is done
		// here, before creation of the down trigger, so that the trigger can declare
		// a variable for the new column.
		table.AddColumn(o.Column, schema.Column{
			Name: TemporaryName(o.Column),
		})

		// If the column has been renamed, temporarily update the column name in
		// the internal schema representation to ensure that the variable name in
		// the down trigger corresponds to the new name of column.
		cols := table.Columns
		if o.Name != nil {
			cols = maps.Clone(table.Columns)
			cols[*o.Name] = cols[o.Column]
			delete(cols, o.Column)
		}

		// Add a trigger to copy values from the new column to the old.
		err = createTrigger(ctx, conn, tr, triggerConfig{
			Name:           TriggerName(o.Table, TemporaryName(o.Column)),
			Direction:      TriggerDirectionDown,
			Columns:        cols,
			LatestSchema:   latestSchema,
			SchemaName:     s.Name,
			TableName:      o.Table,
			PhysicalColumn: o.Column,
			SQL:            o.downSQLForOperations(ops),
		})
		if err != nil {
			return nil, fmt.Errorf("failed to create down trigger: %w", err)
		}
	}

	// rename the column in the virtual schema if required
	if o.Name != nil {
		table.RenameColumn(o.Column, *o.Name)
	}

	if o.IsRenameOnly() {
		return nil, nil
	}
	return table, nil
}

func (o *OpAlterColumn) Complete(ctx context.Context, conn db.DB, tr SQLTransformer, s *schema.Schema) error {
	ops := o.subOperations()

	// Perform any operation specific completion steps
	for _, op := range ops {
		if err := op.Complete(ctx, conn, tr, s); err != nil {
			return err
		}
	}

	if !o.IsRenameOnly() {
		// Drop the old column
		_, err := conn.ExecContext(ctx, fmt.Sprintf("ALTER TABLE IF EXISTS %s DROP COLUMN IF EXISTS %s",
			pq.QuoteIdentifier(o.Table),
			pq.QuoteIdentifier(o.Column)))
		if err != nil {
			return err
		}

		// Remove the up function and trigger
		_, err = conn.ExecContext(ctx, fmt.Sprintf("DROP FUNCTION IF EXISTS %s CASCADE",
			pq.QuoteIdentifier(TriggerFunctionName(o.Table, o.Column))))
		if err != nil {
			return err
		}

		// Remove the down function and trigger
		_, err = conn.ExecContext(ctx, fmt.Sprintf("DROP FUNCTION IF EXISTS %s CASCADE",
			pq.QuoteIdentifier(TriggerFunctionName(o.Table, TemporaryName(o.Column)))))
		if err != nil {
			return err
		}

		// Rename the new column to the old column name
		table := s.GetTable(o.Table)
		column := table.GetColumn(o.Column)
		if err := RenameDuplicatedColumn(ctx, conn, table, column); err != nil {
			return err
		}
	}

	// rename the column in the underlying table if required
	if o.Name != nil {
		_, err := conn.ExecContext(ctx, fmt.Sprintf("ALTER TABLE %s RENAME COLUMN %s TO %s",
			pq.QuoteIdentifier(o.Table),
			pq.QuoteIdentifier(o.Column),
			pq.QuoteIdentifier(*o.Name)))
		if err != nil {
			return err
		}
	}

	return nil
}

func (o *OpAlterColumn) Rollback(ctx context.Context, conn db.DB, tr SQLTransformer) error {
	ops := o.subOperations()

	// Perform any operation specific rollback steps
	for _, ops := range ops {
		if err := ops.Rollback(ctx, conn, tr); err != nil {
			return err
		}
	}

	if !o.IsRenameOnly() {
		// Drop the new column
		_, err := conn.ExecContext(ctx, fmt.Sprintf("ALTER TABLE %s DROP COLUMN IF EXISTS %s",
			pq.QuoteIdentifier(o.Table),
			pq.QuoteIdentifier(TemporaryName(o.Column)),
		))
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
	}

	return nil
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

	// If the operation requires backfills (ie it isn't a rename-only operation),
	// ensure that the column meets the requirements for backfilling.
	if !o.IsRenameOnly() {
		if err := checkBackfill(table); err != nil {
			return err
		}
	}

	// If the column is being renamed, ensure that the target column name does
	// not already exist.
	if o.Name != nil {
		if table.GetColumn(*o.Name) != nil {
			return ColumnAlreadyExistsError{Table: o.Table, Name: *o.Name}
		}
	}

	ops := o.subOperations()

	// Ensure that at least one sub-operation or rename is present
	if len(ops) == 0 && o.Name == nil {
		return AlterColumnNoChangesError{Table: o.Table, Column: o.Column}
	}

	// Rename-only operations are not allowed to have `up` or `down` SQL
	if o.IsRenameOnly() {
		if o.Up != "" {
			return NoUpSQLAllowedError{}
		}
		if o.Down != "" {
			return NoDownSQLAllowedError{}
		}
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
	if o.Default != nil {
		ops = append(ops, &OpSetDefault{
			Table:   o.Table,
			Column:  o.Column,
			Default: *o.Default,
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
			d = d.WithoutNotNull()
		case *OpChangeType:
			d = d.WithType(op.Type)
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

// IsRenameOnly returns true if the operation is a rename column operation only.
func (o *OpAlterColumn) IsRenameOnly() bool {
	return len(o.subOperations()) == 0 && o.Name != nil
}
