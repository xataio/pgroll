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

func (o *OpSetUnique) Start(ctx context.Context, l Logger, conn db.DB, s *schema.Schema) (*StartResult, error) {
	l.LogOperationStart(o)

	table := s.GetTable(o.Table)
	if table == nil {
		return nil, TableDoesNotExistError{Name: o.Table}
	}
	column := table.GetColumn(o.Column)
	if column == nil {
		return nil, ColumnDoesNotExistError{Table: o.Table, Name: o.Column}
	}

	dbActions := []DBAction{
		NewCreateUniqueIndexConcurrentlyAction(conn, s.Name, o.Name, table.Name, column.Name),
	}
	return &StartResult{Actions: dbActions, BackfillTask: backfill.NewTask(table)}, nil
}

func (o *OpSetUnique) Complete(l Logger, conn db.DB, s *schema.Schema) ([]DBAction, error) {
	l.LogOperationComplete(o)

	// Create a unique constraint using the unique index
	return []DBAction{NewAddConstraintUsingUniqueIndex(conn, o.Table, o.Name, o.Name)}, nil
}

func (o *OpSetUnique) Rollback(l Logger, conn db.DB, s *schema.Schema) ([]DBAction, error) {
	l.LogOperationRollback(o)

	return nil, nil
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
