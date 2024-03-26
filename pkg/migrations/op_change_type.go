// SPDX-License-Identifier: Apache-2.0

package migrations

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/lib/pq"
	"github.com/xataio/pgroll/pkg/schema"
)

type OpChangeType struct {
	Table  string `json:"table"`
	Column string `json:"column"`
	Type   string `json:"type"`
	Up     string `json:"up"`
	Down   string `json:"down"`
}

var _ Operation = (*OpChangeType)(nil)

func (o *OpChangeType) Start(ctx context.Context, conn *sql.DB, stateSchema string, tr SQLTransformer, s *schema.Schema, cbs ...CallbackFn) (*schema.Table, error) {
	table := s.GetTable(o.Table)
	column := table.GetColumn(o.Column)

	// Create a copy of the column on the underlying table.
	d := NewColumnDuplicator(conn, table, column).WithType(o.Type)
	if err := d.Duplicate(ctx); err != nil {
		return nil, fmt.Errorf("failed to duplicate column: %w", err)
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

	// Add a trigger to copy values from the new column to the old, rewriting values using the `down` SQL.
	err = createTrigger(ctx, conn, triggerConfig{
		Name:           TriggerName(o.Table, TemporaryName(o.Column)),
		Direction:      TriggerDirectionDown,
		Columns:        table.Columns,
		SchemaName:     s.Name,
		TableName:      o.Table,
		PhysicalColumn: o.Column,
		StateSchema:    stateSchema,
		SQL:            o.Down,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to create down trigger: %w", err)
	}

	return table, nil
}

func (o *OpChangeType) Complete(ctx context.Context, conn *sql.DB, s *schema.Schema) error {
	// Remove the up function and trigger
	_, err := conn.ExecContext(ctx, fmt.Sprintf("DROP FUNCTION IF EXISTS %s CASCADE",
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

	// Drop the old column
	_, err = conn.ExecContext(ctx, fmt.Sprintf("ALTER TABLE IF EXISTS %s DROP COLUMN IF EXISTS %s",
		pq.QuoteIdentifier(o.Table),
		pq.QuoteIdentifier(o.Column)))
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

func (o *OpChangeType) Rollback(ctx context.Context, conn *sql.DB, tr SQLTransformer) error {
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

func (o *OpChangeType) Validate(ctx context.Context, s *schema.Schema) error {
	if o.Up == "" {
		return FieldRequiredError{Name: "up"}
	}

	if o.Down == "" {
		return FieldRequiredError{Name: "down"}
	}
	return nil
}
