// SPDX-License-Identifier: Apache-2.0

package migrations

import (
	"context"
	"database/sql"

	"github.com/xataio/pgroll/pkg/schema"
)

type OpDropNotNull struct {
	Table  string `json:"table"`
	Column string `json:"column"`
	Up     string `json:"up"`
	Down   string `json:"down"`
}

var _ Operation = (*OpDropNotNull)(nil)

func (o *OpDropNotNull) Start(ctx context.Context, conn *sql.DB, stateSchema string, tr SQLTransformer, s *schema.Schema, cbs ...CallbackFn) (*schema.Table, error) {
	table := s.GetTable(o.Table)

	return table, nil
}

func (o *OpDropNotNull) Complete(ctx context.Context, conn *sql.DB, tr SQLTransformer, s *schema.Schema) error {
	return nil
}

func (o *OpDropNotNull) Rollback(ctx context.Context, conn *sql.DB, tr SQLTransformer) error {
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
