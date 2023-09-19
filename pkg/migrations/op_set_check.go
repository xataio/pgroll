package migrations

import (
	"context"
	"database/sql"
	"fmt"
	"strings"

	"github.com/lib/pq"
	"github.com/xataio/pg-roll/pkg/schema"
)

type OpSetCheckConstraint struct {
	Table  string          `json:"table"`
	Column string          `json:"column"`
	Check  CheckConstraint `json:"check"`
	Up     string          `json:"up"`
	Down   string          `json:"down"`
}

var _ Operation = (*OpSetCheckConstraint)(nil)

func (o *OpSetCheckConstraint) Start(ctx context.Context, conn *sql.DB, stateSchema string, s *schema.Schema) error {
	table := s.GetTable(o.Table)
	column := table.GetColumn(o.Column)

	// Create a copy of the column on the underlying table.
	if err := duplicateColumn(ctx, conn, table, *column); err != nil {
		return fmt.Errorf("failed to duplicate column: %w", err)
	}

	// Add the check constraint to the new column as NOT VALID.
	if err := o.addCheckConstraint(ctx, conn); err != nil {
		return fmt.Errorf("failed to add check constraint: %w", err)
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
		return fmt.Errorf("failed to create up trigger: %w", err)
	}

	// Backfill the new column with values from the old column.
	if err := backFill(ctx, conn, o.Table, TemporaryName(o.Column)); err != nil {
		return fmt.Errorf("failed to backfill column: %w", err)
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
		return fmt.Errorf("failed to create down trigger: %w", err)
	}
	return nil
}

func (o *OpSetCheckConstraint) Complete(ctx context.Context, conn *sql.DB) error {
	// Validate the check constraint
	_, err := conn.ExecContext(ctx, fmt.Sprintf("ALTER TABLE IF EXISTS %s VALIDATE CONSTRAINT %s",
		pq.QuoteIdentifier(o.Table),
		pq.QuoteIdentifier(o.Check.Name)))
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
	_, err = conn.ExecContext(ctx, fmt.Sprintf("ALTER TABLE IF EXISTS %s RENAME COLUMN %s TO %s",
		pq.QuoteIdentifier(o.Table),
		pq.QuoteIdentifier(TemporaryName(o.Column)),
		pq.QuoteIdentifier(o.Column)))
	if err != nil {
		return err
	}

	return err
}

func (o *OpSetCheckConstraint) Rollback(ctx context.Context, conn *sql.DB) error {
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

func (o *OpSetCheckConstraint) Validate(ctx context.Context, s *schema.Schema) error {
	if err := o.Check.Validate(); err != nil {
		return CheckConstraintError{
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

func (o *OpSetCheckConstraint) addCheckConstraint(ctx context.Context, conn *sql.DB) error {
	_, err := conn.ExecContext(ctx, fmt.Sprintf("ALTER TABLE %s ADD CONSTRAINT %s CHECK (%s) NOT VALID",
		pq.QuoteIdentifier(o.Table),
		pq.QuoteIdentifier(o.Check.Name),
		rewriteCheckExpression(o.Check.Constraint, o.Column, TemporaryName(o.Column)),
	))

	return err
}

// In order for the `check` expression to be easy to write, migration authors specify
// the check expression as though it were being applied to the old column,
// On migration start, however, the check is actually applied to the new (temporary)
// column.
// This function naively rewrites the check expression to apply to the new column.
func rewriteCheckExpression(check string, oldColumn, newColumn string) string {
	return strings.ReplaceAll(check, oldColumn, newColumn)
}
