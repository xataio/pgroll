// SPDX-License-Identifier: Apache-2.0

package migrations

import (
	"context"
	"fmt"

	"github.com/lib/pq"

	"github.com/xataio/pgroll/pkg/db"
	"github.com/xataio/pgroll/pkg/schema"
)

var _ Operation = (*OpDropConstraint)(nil)

func (o *OpDropConstraint) Start(ctx context.Context, conn db.DB, latestSchema string, tr SQLTransformer, s *schema.Schema, cbs ...CallbackFn) (*schema.Table, error) {
	table := s.GetTable(o.Table)

	// By this point Validate() should have run which ensures the constraint exists and that we only have
	// one column associated with it.
	column := table.GetColumn(table.GetConstraintColumns(o.Name)[0])

	// Create a copy of the column on the underlying table.
	d := NewColumnDuplicator(conn, table).WithColumn(column).WithoutConstraint(o.Name)
	if err := d.Duplicate(ctx); err != nil {
		return nil, fmt.Errorf("failed to duplicate column: %w", err)
	}

	// Add a trigger to copy values from the old column to the new, rewriting values using the `up` SQL.
	err := createTrigger(ctx, conn, tr, triggerConfig{
		Name:           TriggerName(o.Table, column.Name),
		Direction:      TriggerDirectionUp,
		Columns:        table.Columns,
		SchemaName:     s.Name,
		LatestSchema:   latestSchema,
		TableName:      o.Table,
		PhysicalColumn: TemporaryName(column.Name),
		SQL:            o.upSQL(column.Name),
	})
	if err != nil {
		return nil, fmt.Errorf("failed to create up trigger: %w", err)
	}

	// Add the new column to the internal schema representation. This is done
	// here, before creation of the down trigger, so that the trigger can declare
	// a variable for the new column.
	table.AddColumn(column.Name, schema.Column{
		Name: TemporaryName(column.Name),
	})

	// Add a trigger to copy values from the new column to the old, rewriting values using the `down` SQL.
	err = createTrigger(ctx, conn, tr, triggerConfig{
		Name:           TriggerName(o.Table, TemporaryName(column.Name)),
		Direction:      TriggerDirectionDown,
		Columns:        table.Columns,
		SchemaName:     s.Name,
		LatestSchema:   latestSchema,
		TableName:      o.Table,
		PhysicalColumn: column.Name,
		SQL:            o.Down,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to create down trigger: %w", err)
	}
	return table, nil
}

func (o *OpDropConstraint) Complete(ctx context.Context, conn db.DB, tr SQLTransformer, s *schema.Schema) error {
	// We have already validated that there is single column related to this constraint.
	table := s.GetTable(o.Table)
	column := table.GetColumn(table.GetConstraintColumns(o.Name)[0])

	// Remove the up function and trigger
	_, err := conn.ExecContext(ctx, fmt.Sprintf("DROP FUNCTION IF EXISTS %s CASCADE",
		pq.QuoteIdentifier(TriggerFunctionName(o.Table, column.Name))))
	if err != nil {
		return err
	}

	// Remove the down function and trigger
	_, err = conn.ExecContext(ctx, fmt.Sprintf("DROP FUNCTION IF EXISTS %s CASCADE",
		pq.QuoteIdentifier(TriggerFunctionName(o.Table, TemporaryName(column.Name)))))
	if err != nil {
		return err
	}

	// Drop the old column
	_, err = conn.ExecContext(ctx, fmt.Sprintf("ALTER TABLE IF EXISTS %s DROP COLUMN IF EXISTS %s",
		pq.QuoteIdentifier(o.Table),
		pq.QuoteIdentifier(column.Name)))
	if err != nil {
		return err
	}

	// Rename the new column to the old column name
	if err := RenameDuplicatedColumn(ctx, conn, table, column); err != nil {
		return err
	}

	return err
}

func (o *OpDropConstraint) Rollback(ctx context.Context, conn db.DB, tr SQLTransformer, s *schema.Schema) error {
	// We have already validated that there is single column related to this constraint.
	table := s.GetTable(o.Table)
	column := table.GetColumn(table.GetConstraintColumns(o.Name)[0])

	// Drop the new column
	_, err := conn.ExecContext(ctx, fmt.Sprintf("ALTER TABLE %s DROP COLUMN IF EXISTS %s",
		pq.QuoteIdentifier(o.Table),
		pq.QuoteIdentifier(TemporaryName(column.Name)),
	))
	if err != nil {
		return err
	}

	// Remove the up function and trigger
	_, err = conn.ExecContext(ctx, fmt.Sprintf("DROP FUNCTION IF EXISTS %s CASCADE",
		pq.QuoteIdentifier(TriggerFunctionName(o.Table, column.Name)),
	))
	if err != nil {
		return err
	}

	// Remove the down function and trigger
	_, err = conn.ExecContext(ctx, fmt.Sprintf("DROP FUNCTION IF EXISTS %s CASCADE",
		pq.QuoteIdentifier(TriggerFunctionName(o.Table, TemporaryName(column.Name))),
	))

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
