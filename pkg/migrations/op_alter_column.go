// SPDX-License-Identifier: Apache-2.0

package migrations

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/lib/pq"
	"github.com/xataio/pgroll/pkg/schema"
)

var _ Operation = (*OpAlterColumn)(nil)

func (o *OpAlterColumn) Start(ctx context.Context, conn *sql.DB, stateSchema string, tr SQLTransformer, s *schema.Schema, cbs ...CallbackFn) (*schema.Table, error) {
	table := s.GetTable(o.Table)
	column := table.GetColumn(o.Column)

	op := o.innerOperation()

	if _, ok := op.(*OpRenameColumn); !ok {
		// Duplicate the column on the underlying table.
		d := duplicatorForOperation(o.innerOperation(), conn, table, column)
		if err := d.Duplicate(ctx); err != nil {
			return nil, fmt.Errorf("failed to duplicate column: %w", err)
		}
	}

	// perform any operation specific start steps
	tbl, err := op.Start(ctx, conn, stateSchema, tr, s, cbs...)
	if err != nil {
		return nil, err
	}

	// Add a trigger to copy values from the old column to the new, rewriting values using the `up` SQL.
	// Rename column operations do not require this trigger.
	if _, ok := op.(*OpRenameColumn); !ok {
		err = createTrigger(ctx, conn, tr, triggerConfig{
			Name:           TriggerName(o.Table, o.Column),
			Direction:      TriggerDirectionUp,
			Columns:        table.Columns,
			SchemaName:     s.Name,
			TableName:      o.Table,
			PhysicalColumn: TemporaryName(o.Column),
			StateSchema:    stateSchema,
			SQL:            o.upSQLForOperation(op),
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

		// Add a trigger to copy values from the new column to the old.
		err = createTrigger(ctx, conn, tr, triggerConfig{
			Name:           TriggerName(o.Table, TemporaryName(o.Column)),
			Direction:      TriggerDirectionDown,
			Columns:        table.Columns,
			SchemaName:     s.Name,
			TableName:      o.Table,
			PhysicalColumn: o.Column,
			StateSchema:    stateSchema,
			SQL:            o.downSQLForOperation(op),
		})
		if err != nil {
			return nil, fmt.Errorf("failed to create down trigger: %w", err)
		}
	}

	return tbl, nil
}

func (o *OpAlterColumn) Complete(ctx context.Context, conn *sql.DB, tr SQLTransformer, s *schema.Schema) error {
	op := o.innerOperation()

	// Perform any operation specific completion steps
	if err := op.Complete(ctx, conn, tr, s); err != nil {
		return err
	}

	if _, ok := op.(*OpRenameColumn); !ok {
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

	return nil
}

func (o *OpAlterColumn) Rollback(ctx context.Context, conn *sql.DB, tr SQLTransformer) error {
	op := o.innerOperation()

	// Perform any operation specific rollback steps
	if err := op.Rollback(ctx, conn, tr); err != nil {
		return err
	}

	if _, ok := op.(*OpRenameColumn); !ok {
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
	// Ensure that the operation describes only one change to the column
	if cnt := o.numChanges(); cnt != 1 {
		return MultipleAlterColumnChangesError{Changes: cnt}
	}

	// Validate that the table and column exist
	table := s.GetTable(o.Table)
	if table == nil {
		return TableDoesNotExistError{Name: o.Table}
	}
	if table.GetColumn(o.Column) == nil {
		return ColumnDoesNotExistError{Table: o.Table, Name: o.Column}
	}

	// Ensure that the column has a primary key defined on exactly one column.
	err := checkBackfill(table)
	if err != nil {
		return err
	}

	// Apply any special validation rules for the inner operation
	op := o.innerOperation()
	if _, ok := op.(*OpRenameColumn); ok {
		if o.Up != "" {
			return NoUpSQLAllowedError{}
		}
		if o.Down != "" {
			return NoDownSQLAllowedError{}
		}
	}

	// Validate the inner operation in isolation
	return op.Validate(ctx, s)
}

func (o *OpAlterColumn) innerOperation() Operation {
	switch {
	case o.Name != nil:
		return &OpRenameColumn{
			Table: o.Table,
			From:  o.Column,
			To:    *o.Name,
		}

	case o.Type != nil:
		return &OpChangeType{
			Table:  o.Table,
			Column: o.Column,
			Type:   *o.Type,
			Up:     o.Up,
			Down:   o.Down,
		}

	case o.Check != nil:
		return &OpSetCheckConstraint{
			Table:  o.Table,
			Column: o.Column,
			Check:  *o.Check,
			Up:     o.Up,
			Down:   o.Down,
		}

	case o.References != nil:
		return &OpSetForeignKey{
			Table:      o.Table,
			Column:     o.Column,
			References: *o.References,
			Up:         o.Up,
			Down:       o.Down,
		}

	case o.Nullable != nil && !*o.Nullable:
		return &OpSetNotNull{
			Table:  o.Table,
			Column: o.Column,
			Up:     o.Up,
			Down:   o.Down,
		}

	case o.Nullable != nil && *o.Nullable:
		return &OpDropNotNull{
			Table:  o.Table,
			Column: o.Column,
			Up:     o.Up,
			Down:   o.Down,
		}

	case o.Unique != nil:
		return &OpSetUnique{
			Table:  o.Table,
			Column: o.Column,
			Name:   o.Unique.Name,
			Up:     o.Up,
			Down:   o.Down,
		}
	}
	return nil
}

// numChanges returns the number of kinds of change that one 'alter column'
// operation represents.
func (o *OpAlterColumn) numChanges() int {
	fieldsSet := 0

	if o.Name != nil {
		fieldsSet++
	}
	if o.Type != nil {
		fieldsSet++
	}
	if o.Check != nil {
		fieldsSet++
	}
	if o.References != nil {
		fieldsSet++
	}
	if o.Nullable != nil {
		fieldsSet++
	}
	if o.Unique != nil {
		fieldsSet++
	}

	return fieldsSet
}

// duplicatorForOperation returns a Duplicator for the given operation.
func duplicatorForOperation(op Operation, conn *sql.DB, table *schema.Table, column *schema.Column) *Duplicator {
	d := NewColumnDuplicator(conn, table, column)

	switch op := (op).(type) {
	case *OpDropNotNull:
		d = d.WithoutNotNull()
	case *OpChangeType:
		d = d.WithType(op.Type)
	}
	return d
}

// downSQLForOperation returns the down SQL for the given operation, applying
// an appropriate default if none is provided.
func (o *OpAlterColumn) downSQLForOperation(op Operation) string {
	if o.Down != "" {
		return o.Down
	}

	switch (op).(type) {
	case *OpSetUnique, *OpSetNotNull:
		return pq.QuoteIdentifier(o.Column)
	}

	return ""
}

// upSQLForOperation returns the up SQL for the given operation, applying
// an appropriate default if none is provided.
func (o *OpAlterColumn) upSQLForOperation(op Operation) string {
	if o.Up != "" {
		return o.Up
	}

	if _, ok := op.(*OpDropNotNull); ok {
		return pq.QuoteIdentifier(o.Column)
	}

	return ""
}
