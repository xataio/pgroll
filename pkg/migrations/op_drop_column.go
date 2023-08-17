package migrations

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/lib/pq"

	"pg-roll/pkg/schema"
)

type OpDropColumn struct {
	Table  string  `json:"table"`
	Column string  `json:"column"`
	Down   *string `json:"down,omitempty"`
}

var _ Operation = (*OpDropColumn)(nil)

func (o *OpDropColumn) Start(ctx context.Context, conn *sql.DB, schemaName string, stateSchema string, s *schema.Schema) error {
	if o.Down != nil {
		err := createTrigger(ctx, conn, s, triggerConfig{
			Direction:      TriggerDirectionDown,
			SchemaName:     schemaName,
			StateSchema:    stateSchema,
			Table:          o.Table,
			Column:         o.Column,
			PhysicalColumn: o.Column,
			SQL:            *o.Down,
		})
		if err != nil {
			return err
		}
	}

	s.GetTable(o.Table).RemoveColumn(o.Column)
	return nil
}

func (o *OpDropColumn) Complete(ctx context.Context, conn *sql.DB) error {
	_, err := conn.ExecContext(ctx, fmt.Sprintf("ALTER TABLE %s DROP COLUMN %s", o.Table, o.Column))
	if err != nil {
		return err
	}

	_, err = conn.ExecContext(ctx, fmt.Sprintf("DROP FUNCTION IF EXISTS %s CASCADE",
		pq.QuoteIdentifier(TriggerFunctionName(o.Table, o.Column))))

	return err
}

func (o *OpDropColumn) Rollback(ctx context.Context, conn *sql.DB) error {
	_, err := conn.ExecContext(ctx, fmt.Sprintf("DROP FUNCTION IF EXISTS %s CASCADE",
		pq.QuoteIdentifier(TriggerFunctionName(o.Table, o.Column))))

	return err
}

func (o *OpDropColumn) Validate(ctx context.Context, s *schema.Schema) error {
	table := s.GetTable(o.Table)

	if table == nil {
		return TableDoesNotExistError{Name: o.Table}
	}
	if table.GetColumn(o.Column) == nil {
		return ColumnDoesNotExistError{Table: o.Table, Name: o.Column}
	}
	return nil
}
