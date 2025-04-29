// SPDX-License-Identifier: Apache-2.0

package migrations

import (
	"context"

	"github.com/pterm/pterm"
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

func (o *OpSetComment) Start(ctx context.Context, logger pterm.Logger, conn db.DB, latestSchema string, s *schema.Schema) (*schema.Table, error) {
	logger.Info("starting operation", logger.Args(o.loggerArgs()...))
	tbl := s.GetTable(o.Table)
	if tbl == nil {
		return nil, TableDoesNotExistError{Name: o.Table}
	}

	return tbl, NewCommentColumnAction(conn, o.Table, TemporaryName(o.Column), o.Comment).Execute(ctx)
}

func (o *OpSetComment) Complete(ctx context.Context, logger pterm.Logger, conn db.DB, s *schema.Schema) error {
	logger.Info("completing operation", logger.Args(o.loggerArgs()...))
	return NewCommentColumnAction(conn, o.Table, o.Column, o.Comment).Execute(ctx)
}

func (o *OpSetComment) Rollback(ctx context.Context, logger pterm.Logger, conn db.DB, s *schema.Schema) error {
	logger.Info("rolling back operation", logger.Args(o.loggerArgs()...))
	return nil
}

func (o *OpSetComment) Validate(ctx context.Context, s *schema.Schema) error {
	return nil
}

func (o *OpSetComment) loggerArgs() []any {
	args := []any{
		"operation", OpNameAlterColumn,
		"column", o.Column,
		"table", o.Table,
	}
	if o.Comment != nil {
		args = append(args, "comment", *o.Comment)
	}
	return args
}
