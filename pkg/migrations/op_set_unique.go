// SPDX-License-Identifier: Apache-2.0

package migrations

import (
	"context"

	"github.com/xataio/pgroll/pkg/backfill"
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

func (o *OpSetUnique) Start(ctx context.Context, l Logger, conn db.DB, latestSchema string, s *schema.Schema) (*backfill.Task, error) {
	l.LogOperationStart(o)

	table := s.GetTable(o.Table)
	if table == nil {
		return nil, TableDoesNotExistError{Name: o.Table}
	}
	column := table.GetColumn(o.Column)
	if column == nil {
		return nil, ColumnDoesNotExistError{Table: o.Table, Name: o.Column}
	}

	return backfill.NewTask(table), NewCreateUniqueIndexConcurrentlyAction(conn, s.Name, o.Name, table.Name, column.Name).Execute(ctx)
}

func (o *OpSetUnique) Complete(ctx context.Context, l Logger, conn db.DB, s *schema.Schema) error {
	l.LogOperationComplete(o)

	// Create a unique constraint using the unique index
	return NewAddConstraintUsingUniqueIndex(conn, o.Table, o.Name, o.Name).Execute(ctx)
}

func (o *OpSetUnique) Rollback(ctx context.Context, l Logger, conn db.DB, s *schema.Schema) error {
	l.LogOperationRollback(o)

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
