// SPDX-License-Identifier: Apache-2.0

package migrations

import (
	"context"
	"fmt"

	"github.com/lib/pq"
	"github.com/xataio/pgroll/pkg/db"
	"github.com/xataio/pgroll/pkg/schema"
)

var _ Operation = (*OpDropColumn)(nil)

func (o *OpDropColumn) Start(ctx context.Context, conn db.DB, latestSchema string, tr SQLTransformer, s *schema.Schema, cbs ...CallbackFn) (*schema.Table, error) {
	if o.Down != "" {
		err := createTrigger(ctx, conn, tr, triggerConfig{
			Name:           TriggerName(o.Table, o.Column),
			Direction:      TriggerDirectionDown,
			Columns:        s.GetTable(o.Table).Columns,
			SchemaName:     s.Name,
			LatestSchema:   latestSchema,
			TableName:      o.Table,
			PhysicalColumn: o.Column,
			SQL:            o.Down,
		})
		if err != nil {
			return nil, err
		}
	}

	s.GetTable(o.Table).RemoveColumn(o.Column)
	return nil, nil
}

func (o *OpDropColumn) Complete(ctx context.Context, conn db.DB, tr SQLTransformer, s *schema.Schema) error {
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

func (o *OpDropColumn) Rollback(ctx context.Context, conn db.DB, tr SQLTransformer) error {
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
