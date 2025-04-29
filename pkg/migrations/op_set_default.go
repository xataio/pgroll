// SPDX-License-Identifier: Apache-2.0

package migrations

import (
	"context"
	"fmt"

	"github.com/lib/pq"
	"github.com/pterm/pterm"

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

func (o *OpSetDefault) Start(ctx context.Context, logger pterm.Logger, conn db.DB, latestSchema string, s *schema.Schema) (*schema.Table, error) {
	logger.Info("starting operation", logger.Args(o.loggerArgs()...))

	table := s.GetTable(o.Table)
	if table == nil {
		return nil, TableDoesNotExistError{Name: o.Table}
	}
	column := table.GetColumn(o.Column)
	if column == nil {
		return nil, ColumnDoesNotExistError{Table: o.Table, Name: o.Column}
	}

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

func (o *OpSetDefault) Complete(ctx context.Context, logger pterm.Logger, conn db.DB, s *schema.Schema) error {
	logger.Info("completing operation", logger.Args(o.loggerArgs()...))
	return nil
}

func (o *OpSetDefault) Rollback(ctx context.Context, logger pterm.Logger, conn db.DB, s *schema.Schema) error {
	logger.Info("rolling back operation", logger.Args(o.loggerArgs()...))
	return nil
}

func (o *OpSetDefault) Validate(ctx context.Context, s *schema.Schema) error {
	return nil
}

func (o *OpSetDefault) loggerArgs() []any {
	args := []any{
		"operation", OpNameAlterColumn,
		"table", o.Table,
		"column", o.Column,
	}
	if o.Default != nil {
		args = append(args, "default", *o.Default)
	}
	return args
}
