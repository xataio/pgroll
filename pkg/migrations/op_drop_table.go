// SPDX-License-Identifier: Apache-2.0

package migrations

import (
	"context"
	"fmt"

	"github.com/lib/pq"
	"github.com/xataio/pgroll/pkg/db"
	"github.com/xataio/pgroll/pkg/schema"
)

var _ Operation = (*OpDropTable)(nil)

func (o *OpDropTable) Start(ctx context.Context, conn db.DB, latestSchema string, tr SQLTransformer, s *schema.Schema, cbs ...CallbackFn) (*schema.Table, error) {
	s.RemoveTable(o.Name)
	return nil, nil
}

func (o *OpDropTable) Complete(ctx context.Context, conn db.DB, tr SQLTransformer, s *schema.Schema) error {
	_, err := conn.ExecContext(ctx, fmt.Sprintf("DROP TABLE IF EXISTS %s", pq.QuoteIdentifier(o.Name)))

	return err
}

func (o *OpDropTable) Rollback(ctx context.Context, conn db.DB, tr SQLTransformer) error {
	return nil
}

func (o *OpDropTable) Validate(ctx context.Context, s *schema.Schema) error {
	table := s.GetTable(o.Name)

	if table == nil {
		return TableDoesNotExistError{Name: o.Name}
	}
	return nil
}

func (o *OpDropTable) DeriveSchema(ctx context.Context, s *schema.Schema) error {
	s.RemoveTable(o.Table)
	return nil
}
