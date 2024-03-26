// SPDX-License-Identifier: Apache-2.0

package migrations

import (
	"context"
	"database/sql"
	"fmt"
	"strings"

	"github.com/lib/pq"
	"github.com/xataio/pgroll/pkg/schema"
)

type OpSetForeignKey struct {
	Table      string              `json:"table"`
	Column     string              `json:"column"`
	References ForeignKeyReference `json:"references"`
	Up         string              `json:"up"`
	Down       string              `json:"down"`
}

var _ Operation = (*OpSetForeignKey)(nil)

func (o *OpSetForeignKey) Start(ctx context.Context, conn *sql.DB, stateSchema string, tr SQLTransformer, s *schema.Schema, cbs ...CallbackFn) (*schema.Table, error) {
	table := s.GetTable(o.Table)
	column := table.GetColumn(o.Column)

	// Create a copy of the column on the underlying table.
	d := NewColumnDuplicator(conn, table, column)
	if err := d.Duplicate(ctx); err != nil {
		return nil, fmt.Errorf("failed to duplicate column: %w", err)
	}

	// Create a NOT VALID foreign key constraint on the new column.
	if err := o.addForeignKeyConstraint(ctx, conn); err != nil {
		return nil, fmt.Errorf("failed to add foreign key constraint: %w", err)
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

func (o *OpSetForeignKey) Complete(ctx context.Context, conn *sql.DB, tr SQLTransformer, s *schema.Schema) error {
	// Validate the foreign key constraint
	_, err := conn.ExecContext(ctx, fmt.Sprintf("ALTER TABLE IF EXISTS %s VALIDATE CONSTRAINT %s",
		pq.QuoteIdentifier(o.Table),
		pq.QuoteIdentifier(o.References.Name)))
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

	return err
}

func (o *OpSetForeignKey) Rollback(ctx context.Context, conn *sql.DB, tr SQLTransformer) error {
	// Drop the new column, taking the constraint on the column with it
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

func (o *OpSetForeignKey) Validate(ctx context.Context, s *schema.Schema) error {
	if err := o.References.Validate(s); err != nil {
		return ColumnReferenceError{
			Table:  o.Table,
			Column: o.Column,
			Err:    err,
		}
	}

	if o.Up == "" {
		return FieldRequiredError{Name: "up"}
	}

	if o.Down == "" {
		return FieldRequiredError{Name: "down"}
	}

	return nil
}

func (o *OpSetForeignKey) addForeignKeyConstraint(ctx context.Context, conn *sql.DB) error {
	tempColumnName := TemporaryName(o.Column)

	onDelete := "NO ACTION"
	if o.References.OnDelete != "" {
		onDelete = strings.ToUpper(string(o.References.OnDelete))
	}

	_, err := conn.ExecContext(ctx,
		fmt.Sprintf("ALTER TABLE %s ADD CONSTRAINT %s FOREIGN KEY (%s) REFERENCES %s (%s) ON DELETE %s NOT VALID",
			pq.QuoteIdentifier(o.Table),
			pq.QuoteIdentifier(o.References.Name),
			pq.QuoteIdentifier(tempColumnName),
			pq.QuoteIdentifier(o.References.Table),
			pq.QuoteIdentifier(o.References.Column),
			onDelete,
		))

	return err
}
