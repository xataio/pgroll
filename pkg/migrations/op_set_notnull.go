package migrations

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/lib/pq"
	"github.com/xataio/pg-roll/pkg/schema"
)

type OpSetNotNull struct {
	Table  string  `json:"table"`
	Column string  `json:"column"`
	Up     *string `json:"up"`
}

var _ Operation = (*OpSetNotNull)(nil)

func (o *OpSetNotNull) Start(ctx context.Context, conn *sql.DB, schemaName string, stateSchema string, s *schema.Schema) error {
	table := s.GetTable(o.Table)
	column := table.GetColumn(o.Column)

	// Create a copy of the column on the underlying table.
	if err := duplicateColumn(ctx, conn, table, *column); err != nil {
		return fmt.Errorf("failed to duplicate column: %w", err)
	}

	// Add an unchecked NOT NULL constraint to the new column.
	if err := addNotNullConstraint(ctx, conn, o.Table, o.Column, TemporaryName(o.Column)); err != nil {
		return fmt.Errorf("failed to add not null constraint: %w", err)
	}

	// Add a trigger to copy values from the old column to the new, rewriting NULL values using the `up` SQL.
	err := createTrigger(ctx, conn, s, triggerConfig{
		Direction:      TriggerDirectionUp,
		SchemaName:     schemaName,
		StateSchema:    stateSchema,
		Table:          o.Table,
		Column:         o.Column,
		PhysicalColumn: TemporaryName(o.Column),
		SQL:            *o.Up,
		TestExpr:       fmt.Sprintf("NEW.%s IS NULL", pq.QuoteIdentifier(o.Column)),
		ElseExpr: fmt.Sprintf("NEW.%s = NEW.%s;",
			pq.QuoteIdentifier(TemporaryName(o.Column)),
			pq.QuoteIdentifier(o.Column)),
	})
	if err != nil {
		return fmt.Errorf("failed to create up trigger: %w", err)
	}

	// Backfill the new column with values from the old column.
	if err := backFill(ctx, conn, o.Table, TemporaryName(o.Column)); err != nil {
		return fmt.Errorf("failed to backfill column: %w", err)
	}

	// Add a trigger to copy values from the new column to the old.
	err = createTrigger(ctx, conn, s, triggerConfig{
		Direction:      TriggerDirectionDown,
		SchemaName:     schemaName,
		StateSchema:    stateSchema,
		Table:          o.Table,
		Column:         TemporaryName(o.Column),
		PhysicalColumn: o.Column,
		SQL:            fmt.Sprintf("NEW.%s", pq.QuoteIdentifier(TemporaryName(o.Column))),
	})
	if err != nil {
		return fmt.Errorf("failed to create down trigger: %w", err)
	}

	table.AddColumn(o.Column, schema.Column{
		Name: TemporaryName(o.Column),
	})

	return nil
}

func (o *OpSetNotNull) Complete(ctx context.Context, conn *sql.DB) error {
	// Validate the NOT NULL constraint on the old column.
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
	_, err = conn.ExecContext(ctx, fmt.Sprintf("ALTER TABLE IF EXISTS %s RENAME COLUMN %s TO %s",
		pq.QuoteIdentifier(o.Table),
		pq.QuoteIdentifier(TemporaryName(o.Column)),
		pq.QuoteIdentifier(o.Column)))
	if err != nil {
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
	table := s.GetTable(o.Table)
	if table == nil {
		return TableDoesNotExistError{Name: o.Table}
	}

	if table.GetColumn(o.Column) == nil {
		return ColumnDoesNotExistError{Table: o.Table, Name: o.Column}
	}

	if o.Up == nil {
		return UpSQLRequiredError{}
	}
	return nil
}

func duplicateColumn(ctx context.Context, conn *sql.DB, table *schema.Table, column schema.Column) error {
	column.Name = TemporaryName(column.Name)

	_, err := conn.ExecContext(ctx, fmt.Sprintf("ALTER TABLE %s ADD COLUMN %s",
		pq.QuoteIdentifier(table.Name),
		schemaColumnToSQL(column),
	))

	return err
}

// TODO: This function needs to be able to duplicate a column more precisely
// including constraints, indexes, defaults, etc.
func schemaColumnToSQL(c schema.Column) string {
	return fmt.Sprintf("%s %s", pq.QuoteIdentifier(c.Name), c.Type)
}
