// SPDX-License-Identifier: Apache-2.0

package migrations

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/lib/pq"
	"github.com/xataio/pgroll/pkg/schema"
)

type OpSetNotNull struct {
	Table  string `json:"table"`
	Column string `json:"column"`
	Up     string `json:"up"`
	Down   string `json:"down"`
}

var _ Operation = (*OpSetNotNull)(nil)

func (o *OpSetNotNull) Start(ctx context.Context, conn *sql.DB, stateSchema string, s *schema.Schema, cbs ...CallbackFn) (*schema.Table, error) {
	table := s.GetTable(o.Table)
	column := table.GetColumn(o.Column)

	// Create a copy of the column on the underlying table.
	d := NewColumnDuplicator(conn, table, column)
	if err := d.Duplicate(ctx); err != nil {
		return nil, fmt.Errorf("failed to duplicate column: %w", err)
	}

	// Add an unchecked NOT NULL constraint to the new column.
	if err := addNotNullConstraint(ctx, conn, o.Table, o.Column, TemporaryName(o.Column)); err != nil {
		return nil, fmt.Errorf("failed to add not null constraint: %w", err)
	}

	// Add a trigger to copy values from the old column to the new, rewriting values using the `up` SQL.
	err := createTrigger(ctx, conn, triggerConfig{
		Name:           TriggerName(o.Table, o.Column),
		Direction:      TriggerDirectionUp,
		Columns:        table.Columns,
		SchemaName:     s.Name,
		TableName:      o.Table,
		PhysicalColumn: TemporaryName(o.Column),
		StateSchema:    stateSchema,
		SQL:            o.Up,
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
	err = createTrigger(ctx, conn, triggerConfig{
		Name:           TriggerName(o.Table, TemporaryName(o.Column)),
		Direction:      TriggerDirectionDown,
		Columns:        table.Columns,
		SchemaName:     s.Name,
		TableName:      o.Table,
		PhysicalColumn: o.Column,
		StateSchema:    stateSchema,
		SQL:            o.downSQL(),
	})
	if err != nil {
		return nil, fmt.Errorf("failed to create down trigger: %w", err)
	}

	return table, nil
}

func (o *OpSetNotNull) Complete(ctx context.Context, conn *sql.DB, s *schema.Schema) error {
	// Validate the NOT NULL constraint on the old column.
	// The constraint must be valid because:
	// * Existing NULL values in the old column were rewritten using the `up` SQL during backfill.
	// * New NULL values written to the old column during the migration period were also rewritten using `up` SQL.
	_, err := conn.ExecContext(ctx, fmt.Sprintf("ALTER TABLE IF EXISTS %s VALIDATE CONSTRAINT %s",
		pq.QuoteIdentifier(o.Table),
		pq.QuoteIdentifier(NotNullConstraintName(o.Column))))
	if err != nil {
		return err
	}

	// Use the validated constraint to add `NOT NULL` to the new column
	_, err = conn.ExecContext(ctx, fmt.Sprintf("ALTER TABLE IF EXISTS %s ALTER COLUMN %s SET NOT NULL",
		pq.QuoteIdentifier(o.Table),
		pq.QuoteIdentifier(TemporaryName(o.Column))))
	if err != nil {
		return err
	}

	// Drop the NOT NULL constraint
	_, err = conn.ExecContext(ctx, fmt.Sprintf("ALTER TABLE IF EXISTS %s DROP CONSTRAINT IF EXISTS %s",
		pq.QuoteIdentifier(o.Table),
		pq.QuoteIdentifier(NotNullConstraintName(o.Column))))
	if err != nil {
		return err
	}

	// Drop the old column
	_, err = conn.ExecContext(ctx, fmt.Sprintf("ALTER TABLE IF EXISTS %s DROP COLUMN IF EXISTS %s",
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

	return nil
}

func (o *OpSetNotNull) Rollback(ctx context.Context, conn *sql.DB) error {
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

	return err
}

func (o *OpSetNotNull) Validate(ctx context.Context, s *schema.Schema) error {
	column := s.GetTable(o.Table).GetColumn(o.Column)

	if !column.Nullable {
		return ColumnIsNotNullableError{Table: o.Table, Name: o.Column}
	}

	if o.Up == "" {
		return FieldRequiredError{Name: "up"}
	}

	return nil
}

// Down SQL is either user-specified or defaults to copying the value from the new column to the old.
func (o *OpSetNotNull) downSQL() string {
	if o.Down == "" {
		return fmt.Sprintf("NEW.%s", pq.QuoteIdentifier(TemporaryName(o.Column)))
	}
	return o.Down
}
