// SPDX-License-Identifier: Apache-2.0

package migrations

import (
	"context"

	"github.com/pterm/pterm"
	"github.com/xataio/pgroll/pkg/db"
	"github.com/xataio/pgroll/pkg/schema"
)

type OpChangeType struct {
	Table  string `json:"table"`
	Column string `json:"column"`
	Type   string `json:"type"`
	Up     string `json:"up"`
	Down   string `json:"down"`
}

var _ Operation = (*OpChangeType)(nil)

func (o *OpChangeType) Start(ctx context.Context, logger pterm.Logger, conn db.DB, latestSchema string, s *schema.Schema) (*schema.Table, error) {
	logger.Info("starting operation", logger.Args(o.loggerArgs()...))

	table := s.GetTable(o.Table)
	if table == nil {
		return nil, TableDoesNotExistError{Name: o.Table}
	}

	return table, nil
}

func (o *OpChangeType) Complete(ctx context.Context, logger pterm.Logger, conn db.DB, s *schema.Schema) error {
	logger.Info("completing operation", logger.Args(o.loggerArgs()...))

	return nil
}

func (o *OpChangeType) Rollback(ctx context.Context, logger pterm.Logger, conn db.DB, s *schema.Schema) error {
	logger.Info("rolling back operation", logger.Args(o.loggerArgs()...))

	return nil
}

func (o *OpChangeType) Validate(ctx context.Context, s *schema.Schema) error {
	if o.Up == "" {
		return FieldRequiredError{Name: "up"}
	}

	if o.Down == "" {
		return FieldRequiredError{Name: "down"}
	}
	return nil
}

func (o *OpChangeType) loggerArgs() []any {
	return []any{
		"operation", OpNameAlterColumn,
		"column", o.Column,
		"table", o.Table,
		"type", o.Type,
	}
}
