// SPDX-License-Identifier: Apache-2.0

package migrations

import (
	"context"

	"github.com/xataio/pgroll/pkg/db"
	"github.com/xataio/pgroll/pkg/schema"
)

// OpSetComment is a operation that sets a comment on a object.
type OpSetComment struct {
	Table   string  `json:"table"`
	Column  string  `json:"column"`
	Comment *string `json:"comment"`
	Up      string  `json:"up"`
	Down    string  `json:"down"`
}

var _ Operation = (*OpSetComment)(nil)

func (o *OpSetComment) Start(ctx context.Context, conn db.DB, latestSchema string, tr SQLTransformer, s *schema.Schema, cbs ...CallbackFn) (*schema.Table, error) {
	tbl := s.GetTable(o.Table)

	return tbl, addCommentToColumn(ctx, conn, o.Table, TemporaryName(o.Column), o.Comment)
}

func (o *OpSetComment) Complete(ctx context.Context, conn db.DB, tr SQLTransformer, s *schema.Schema) error {
	return nil
}

func (o *OpSetComment) Rollback(ctx context.Context, conn db.DB, tr SQLTransformer, s *schema.Schema) error {
	return nil
}

func (o *OpSetComment) Validate(ctx context.Context, s *schema.Schema) error {
	return nil
}
