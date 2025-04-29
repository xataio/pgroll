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

type OpSetUnique struct {
	Name   string `json:"name"`
	Table  string `json:"table"`
	Column string `json:"column"`
	Up     string `json:"up"`
	Down   string `json:"down"`
}

var _ Operation = (*OpSetUnique)(nil)

func (o *OpSetUnique) Start(ctx context.Context, logger pterm.Logger, conn db.DB, latestSchema string, s *schema.Schema) (*schema.Table, error) {
	logger.Info("starting operation", logger.Args(o.loggerArgs()...))

	table := s.GetTable(o.Table)
	if table == nil {
		return nil, TableDoesNotExistError{Name: o.Table}
	}
	column := table.GetColumn(o.Column)
	if column == nil {
		return nil, ColumnDoesNotExistError{Table: o.Table, Name: o.Column}
	}

	return table, NewCreateUniqueIndexConcurrentlyAction(conn, s.Name, o.Name, table.Name, column.Name).Execute(ctx)
}

func (o *OpSetUnique) Complete(ctx context.Context, logger pterm.Logger, conn db.DB, s *schema.Schema) error {
	logger.Info("completing operation", logger.Args(o.loggerArgs()...))

	// Create a unique constraint using the unique index
	_, err := conn.ExecContext(ctx, fmt.Sprintf("ALTER TABLE IF EXISTS %s ADD CONSTRAINT %s UNIQUE USING INDEX %s",
		pq.QuoteIdentifier(o.Table),
		pq.QuoteIdentifier(o.Name),
		pq.QuoteIdentifier(o.Name)))
	if err != nil {
		return err
	}

	return err
}

func (o *OpSetUnique) Rollback(ctx context.Context, logger pterm.Logger, conn db.DB, s *schema.Schema) error {
	logger.Info("rolling back operation", logger.Args(o.loggerArgs()...))

	return nil
}

func (o *OpSetUnique) Validate(ctx context.Context, s *schema.Schema) error {
	if o.Name == "" {
		return FieldRequiredError{Name: "name"}
	}

	table := s.GetTable(o.Table)
	if table == nil {
		return TableDoesNotExistError{Name: o.Table}
	}

	if table.GetColumn(o.Column) == nil {
		return ColumnDoesNotExistError{Table: o.Table, Name: o.Column}
	}

	if table.ConstraintExists(o.Name) {
		return ConstraintAlreadyExistsError{
			Table:      table.Name,
			Constraint: o.Name,
		}
	}

	return nil
}

func (o *OpSetUnique) loggerArgs() []any {
	return []any{
		"operation", OpNameAlterColumn,
		"column", o.Column,
		"table", o.Table,
		"unique", true,
	}
}
