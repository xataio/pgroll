// SPDX-License-Identifier: Apache-2.0

package migrations

import (
	"context"

	"github.com/xataio/pgroll/pkg/db"
	"github.com/xataio/pgroll/pkg/schema"
)

// OpDropNotNull is an operation that drops the NOT NULL constraint from a column
type OpDropNotNull struct {
	Table  string `json:"table"`
	Column string `json:"column"`
	Up     string `json:"up"`
	Down   string `json:"down"`
}

var _ Operation = (*OpDropNotNull)(nil)

func (o *OpDropNotNull) Start(ctx context.Context, l Logger, conn db.DB, latestSchema string, s *schema.Schema) (*schema.Table, error) {
	l.LogOperationStart(o)

	table := s.GetTable(o.Table)
	if table == nil {
		return nil, TableDoesNotExistError{Name: o.Table}
	}

	return table, nil
}

func (o *OpDropNotNull) Complete(ctx context.Context, l Logger, conn db.DB, s *schema.Schema) error {
	l.LogOperationComplete(o)
	return nil
}

func (o *OpDropNotNull) Rollback(ctx context.Context, l Logger, conn db.DB, s *schema.Schema) error {
	l.LogOperationRollback(o)
	return nil
}

func (o *OpDropNotNull) Validate(ctx context.Context, s *schema.Schema) error {
	column := s.GetTable(o.Table).GetColumn(o.Column)
	if column.Nullable {
		return ColumnIsNullableError{Table: o.Table, Name: o.Column}
	}

	if o.Down == "" {
		return FieldRequiredError{Name: "down"}
	}

	return nil
}
