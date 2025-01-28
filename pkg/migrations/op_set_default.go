// SPDX-License-Identifier: Apache-2.0

package migrations

import (
	"context"
	"fmt"

	"github.com/lib/pq"

	"github.com/xataio/pgroll/pkg/backfill"
	"github.com/xataio/pgroll/pkg/db"
	"github.com/xataio/pgroll/pkg/schema"
)

type OpSetDefault struct {
	Table   string  `json:"table"`
	Column  string  `json:"column"`
	Default *string `json:"default"`
	Up      string  `json:"up"`
	Down    string  `json:"down"`
}

var _ Operation = (*OpSetDefault)(nil)

func (o *OpSetDefault) Start(ctx context.Context, conn db.DB, latestSchema string, tr SQLTransformer, s *schema.Schema, cbs ...backfill.CallbackFn) (*schema.Table, error) {
	table := s.GetTable(o.Table)
	column := table.GetColumn(o.Column)

	var err error
	if o.Default == nil {
		_, err = conn.ExecContext(ctx, fmt.Sprintf(`ALTER TABLE %s ALTER COLUMN %s DROP DEFAULT`,
			pq.QuoteIdentifier(table.Name),
			pq.QuoteIdentifier(column.Name)))
	} else {
		_, err = conn.ExecContext(ctx, fmt.Sprintf(`ALTER TABLE %s ALTER COLUMN %s SET DEFAULT %s`,
			pq.QuoteIdentifier(table.Name),
			pq.QuoteIdentifier(column.Name),
			*o.Default))
	}
	if err != nil {
		return nil, err
	}

	return table, nil
}

func (o *OpSetDefault) Complete(ctx context.Context, conn db.DB, tr SQLTransformer, s *schema.Schema) error {
	return nil
}

func (o *OpSetDefault) Rollback(ctx context.Context, conn db.DB, tr SQLTransformer, s *schema.Schema) error {
	return nil
}

func (o *OpSetDefault) Validate(ctx context.Context, s *schema.Schema) error {
	return nil
}
