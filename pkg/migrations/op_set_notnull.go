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
	if err := addNotNullConstraint(ctx, conn, o.Table, o.Column, o.Column); err != nil {
		return fmt.Errorf("failed to add check constraint: %w", err)
	}

	if o.Up != nil {
		err := createTrigger(ctx, conn, s, triggerConfig{
			Direction:      TriggerDirectionUp,
			SchemaName:     schemaName,
			StateSchema:    stateSchema,
			Table:          o.Table,
			Column:         o.Column,
			PhysicalColumn: o.Column,
			SQL:            *o.Up,
		})
		if err != nil {
			return fmt.Errorf("failed to create trigger: %w", err)
		}
	}

	return nil
}

func (o *OpSetNotNull) Complete(ctx context.Context, conn *sql.DB) error {
	// drop the function and trigger that uses it
	_, err := conn.ExecContext(ctx, fmt.Sprintf("DROP FUNCTION IF EXISTS %s CASCADE",
		pq.QuoteIdentifier(TriggerFunctionName(o.Table, o.Column))))
	if err != nil {
		return err
	}

	// validate the not null constraint
	_, err = conn.ExecContext(ctx, fmt.Sprintf("ALTER TABLE IF EXISTS %s VALIDATE CONSTRAINT %s",
		pq.QuoteIdentifier(o.Table),
		pq.QuoteIdentifier(NotNullConstraintName(o.Column))))
	if err != nil {
		return err
	}

	// set the column to not null
	_, err = conn.ExecContext(ctx, fmt.Sprintf("ALTER TABLE IF EXISTS %s ALTER COLUMN %s SET NOT NULL",
		pq.QuoteIdentifier(o.Table),
		pq.QuoteIdentifier(o.Column)))
	if err != nil {
		return err
	}

	// drop the not null constraint
	_, err = conn.ExecContext(ctx, fmt.Sprintf("ALTER TABLE IF EXISTS %s DROP CONSTRAINT IF EXISTS %s",
		pq.QuoteIdentifier(o.Table),
		pq.QuoteIdentifier(NotNullConstraintName(o.Column))))
	if err != nil {
		return err
	}

	return nil
}

func (o *OpSetNotNull) Rollback(ctx context.Context, conn *sql.DB) error {
	// drop the not null constraint
	_, err := conn.ExecContext(ctx, fmt.Sprintf("ALTER TABLE IF EXISTS %s DROP CONSTRAINT %s",
		pq.QuoteIdentifier(o.Table),
		pq.QuoteIdentifier(NotNullConstraintName(o.Column))))
	if err != nil {
		return err
	}

	// drop the function and trigger that uses it
	_, err = conn.ExecContext(ctx, fmt.Sprintf("DROP FUNCTION IF EXISTS %s CASCADE",
		pq.QuoteIdentifier(TriggerFunctionName(o.Table, o.Column))))

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

	return nil
}
