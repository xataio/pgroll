// SPDX-License-Identifier: Apache-2.0

package migrations

import (
	"context"

	"github.com/xataio/pgroll/pkg/backfill"
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

func (o *OpSetComment) Start(ctx context.Context, l Logger, conn db.DB, s *schema.Schema) (*StartResult, error) {
	l.LogOperationStart(o)

	tbl := s.GetTable(o.Table)
	if tbl == nil {
		return nil, TableDoesNotExistError{Name: o.Table}
	}

	dbActions := []DBAction{
		NewCommentColumnAction(conn, o.Table, TemporaryName(o.Column), o.Comment),
	}

	return &StartResult{Actions: dbActions, BackfillTask: backfill.NewTask(tbl)}, nil
}

func (o *OpSetComment) Complete(l Logger, conn db.DB, s *schema.Schema) ([]DBAction, error) {
	l.LogOperationComplete(o)

	return []DBAction{
		NewCommentColumnAction(conn, o.Table, o.Column, o.Comment),
	}, nil
}

func (o *OpSetComment) Rollback(l Logger, conn db.DB, s *schema.Schema) ([]DBAction, error) {
	l.LogOperationRollback(o)

	return nil, nil
}

func (o *OpSetComment) Validate(ctx context.Context, s *schema.Schema) error {
	return nil
}
