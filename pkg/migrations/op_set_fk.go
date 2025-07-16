// SPDX-License-Identifier: Apache-2.0

package migrations

import (
	"context"

	"github.com/xataio/pgroll/pkg/backfill"
	"github.com/xataio/pgroll/pkg/db"
	"github.com/xataio/pgroll/pkg/schema"
)

type OpSetForeignKey struct {
	Table      string              `json:"table"`
	Column     string              `json:"column"`
	References ForeignKeyReference `json:"references"`
	Up         string              `json:"up"`
	Down       string              `json:"down"`
}

var _ Operation = (*OpSetForeignKey)(nil)

func (o *OpSetForeignKey) Start(ctx context.Context, l Logger, conn db.DB, s *schema.Schema) (*StartResult, error) {
	l.LogOperationStart(o)

	table := s.GetTable(o.Table)
	if table == nil {
		return nil, TableDoesNotExistError{Name: o.Table}
	}
	column := table.GetColumn(o.Column)
	if column == nil {
		return nil, ColumnDoesNotExistError{Table: o.Table, Name: o.Column}
	}
	referencedTable := s.GetTable(o.References.Table)
	if referencedTable == nil {
		return nil, TableDoesNotExistError{Name: o.References.Table}
	}

	referencedColumn := referencedTable.GetColumn(o.References.Column)
	if referencedColumn == nil {
		return nil, ColumnDoesNotExistError{Table: o.References.Table, Name: o.References.Column}
	}

	dbActions := []DBAction{
		// Create a NOT VALID foreign key constraint on the new column.
		NewCreateFKConstraintAction(conn,
			table.Name,
			o.References.Name,
			[]string{column.Name},
			&TableForeignKeyReference{
				Table:     referencedTable.Name,
				Columns:   []string{referencedColumn.Name},
				MatchType: o.References.MatchType,
				OnDelete:  o.References.OnDelete,
				OnUpdate:  o.References.OnUpdate,
			},
			o.References.InitiallyDeferred,
			o.References.Deferrable,
			true,
		),
	}

	return &StartResult{Actions: dbActions, BackfillTask: backfill.NewTask(table)}, nil
}

func (o *OpSetForeignKey) Complete(l Logger, conn db.DB, s *schema.Schema) ([]DBAction, error) {
	l.LogOperationComplete(o)

	table := s.GetTable(o.Table)
	if table == nil {
		return nil, TableDoesNotExistError{Name: o.Table}
	}
	table.ForeignKeys[o.References.Name] = &schema.ForeignKey{
		Name:              o.References.Name,
		Columns:           []string{o.Column},
		ReferencedTable:   o.References.Table,
		ReferencedColumns: []string{o.References.Column},
	}

	return []DBAction{
		// Validate the foreign key constraint
		NewValidateConstraintAction(conn, table.Name, o.References.Name),
	}, nil
}

func (o *OpSetForeignKey) Rollback(l Logger, conn db.DB, s *schema.Schema) ([]DBAction, error) {
	l.LogOperationRollback(o)

	return nil, nil
}

func (o *OpSetForeignKey) Validate(ctx context.Context, s *schema.Schema) error {
	if err := o.References.Validate(s); err != nil {
		return ColumnReferenceError{
			Table:  o.Table,
			Column: o.Column,
			Err:    err,
		}
	}

	table := s.GetTable(o.Table)
	if table == nil {
		return TableDoesNotExistError{Name: o.Table}
	}

	if table.ConstraintExists(o.References.Name) {
		return ConstraintAlreadyExistsError{
			Table:      table.Name,
			Constraint: o.References.Name,
		}
	}

	if o.Up == "" {
		return FieldRequiredError{Name: "up"}
	}

	if o.Down == "" {
		return FieldRequiredError{Name: "down"}
	}

	return nil
}
