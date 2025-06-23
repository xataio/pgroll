// SPDX-License-Identifier: Apache-2.0

package migrations

import (
	"context"
	"fmt"

	"github.com/xataio/pgroll/pkg/backfill"
	"github.com/xataio/pgroll/pkg/db"
	"github.com/xataio/pgroll/pkg/schema"
)

type OpSetNotNull struct {
	Table  string `json:"table"`
	Column string `json:"column"`
	Up     string `json:"up"`
	Down   string `json:"down"`
}

var _ Operation = (*OpSetNotNull)(nil)

func (o *OpSetNotNull) Start(ctx context.Context, l Logger, conn db.DB, latestSchema string, s *schema.Schema) (*backfill.Task, error) {
	l.LogOperationStart(o)

	table := s.GetTable(o.Table)
	if table == nil {
		return nil, TableDoesNotExistError{Name: o.Table}
	}
	column := table.GetColumn(o.Column)
	if column == nil {
		return nil, ColumnDoesNotExistError{Table: o.Table, Name: o.Column}
	}

	// Add an unchecked NOT NULL constraint to the new column.
	skipInherit := false
	skipValidate := true // We will validate the constraint later in the Complete step.
	if err := NewCreateCheckConstraintAction(
		conn,
		table.Name,
		NotNullConstraintName(o.Column),
		fmt.Sprintf("%s IS NOT NULL", o.Column),
		[]string{o.Column},
		skipInherit,
		skipValidate,
	).Execute(ctx); err != nil {
		return nil, fmt.Errorf("failed to add not null constraint: %w", err)
	}

	return backfill.NewTask(table), nil
}

func (o *OpSetNotNull) Complete(l Logger, conn db.DB, s *schema.Schema) ([]DBAction, error) {
	l.LogOperationComplete(o)

	return []DBAction{
		// Validate the NOT NULL constraint on the old column.
		// The constraint must be valid because:
		// * Existing NULL values in the old column were rewritten using the `up` SQL during backfill.
		// * New NULL values written to the old column during the migration period were also rewritten using `up` SQL.
		NewValidateConstraintAction(conn, o.Table, NotNullConstraintName(o.Column)),
		// Use the validated constraint to add `NOT NULL` to the new column
		NewSetNotNullAction(conn, o.Table, TemporaryName(o.Column)),
		// Drop the NOT NULL constraint
		NewDropConstraintAction(conn, o.Table, NotNullConstraintName(o.Column)),
	}, nil
}

func (o *OpSetNotNull) Rollback(l Logger, conn db.DB, s *schema.Schema) ([]DBAction, error) {
	l.LogOperationRollback(o)

	return []DBAction{}, nil
}

func (o *OpSetNotNull) Validate(ctx context.Context, s *schema.Schema) error {
	column := s.GetTable(o.Table).GetColumn(o.Column)

	if !column.Nullable {
		return ColumnIsNotNullableError{Table: o.Table, Name: o.Column}
	}

	if o.Up == "" {
		return FieldRequiredError{Name: "up"}
	}

	return nil
}
