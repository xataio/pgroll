// SPDX-License-Identifier: Apache-2.0

package migrations

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/lib/pq"
	"github.com/xataio/pgroll/pkg/schema"
)

var _ Operation = (*OpDropColumn)(nil)

func (o *OpDropColumn) Start(ctx context.Context, conn *sql.DB, stateSchema string, s *schema.Schema, cbs ...CallbackFn) error {
	if o.Down != nil {
		err := createTrigger(ctx, conn, triggerConfig{
			Name:           TriggerName(o.Table, o.Column),
			Direction:      TriggerDirectionDown,
			Columns:        s.GetTable(o.Table).Columns,
			SchemaName:     s.Name,
			TableName:      o.Table,
			PhysicalColumn: o.Column,
			StateSchema:    stateSchema,
			SQL:            *o.Down,
		})
		if err != nil {
			return err
		}
	}

	s.GetTable(o.Table).RemoveColumn(o.Column)
	return nil
}

func (o *OpDropColumn) Complete(ctx context.Context, conn *sql.DB, s *schema.Schema) error {
	_, err := conn.ExecContext(ctx, fmt.Sprintf("ALTER TABLE %s DROP COLUMN %s",
		pq.QuoteIdentifier(o.Table),
		pq.QuoteIdentifier(o.Column)))
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
	if err := validateName(o.Column); err != nil {
		return err
	}
	table := s.GetTable(o.Table)

	if table == nil {
		return TableDoesNotExistError{Name: o.Table}
	}
	if table.GetColumn(o.Column) == nil {
		return ColumnDoesNotExistError{Table: o.Table, Name: o.Column}
	}
	return nil
}
