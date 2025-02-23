// SPDX-License-Identifier: Apache-2.0

package migrations

import (
	"context"
	"fmt"

	"github.com/lib/pq"

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

func (o *OpSetForeignKey) Start(ctx context.Context, conn db.DB, latestSchema string, s *schema.Schema) (*schema.Table, error) {
	table := s.GetTable(o.Table)

	// Create a NOT VALID foreign key constraint on the new column.
	if err := o.addForeignKeyConstraint(ctx, conn, s); err != nil {
		return nil, fmt.Errorf("failed to add foreign key constraint: %w", err)
	}

	return table, nil
}

func (o *OpSetForeignKey) Complete(ctx context.Context, conn db.DB, s *schema.Schema) error {
	// Validate the foreign key constraint
	_, err := conn.ExecContext(ctx, fmt.Sprintf("ALTER TABLE IF EXISTS %s VALIDATE CONSTRAINT %s",
		pq.QuoteIdentifier(o.Table),
		pq.QuoteIdentifier(o.References.Name)))
	if err != nil {
		return err
	}

	return nil
}

func (o *OpSetForeignKey) Rollback(ctx context.Context, conn db.DB, s *schema.Schema) error {
	return nil
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

func (o *OpSetForeignKey) addForeignKeyConstraint(ctx context.Context, conn db.DB, s *schema.Schema) error {
	table := s.GetTable(o.Table)
	if table == nil {
		return TableDoesNotExistError{Name: o.Table}
	}
	column := table.GetColumn(o.Column)
	if column == nil {
		return ColumnDoesNotExistError{Table: o.Table, Name: o.Column}
	}
	referencedTable := s.GetTable(o.References.Table)
	if referencedTable == nil {
		return TableDoesNotExistError{Name: o.References.Table}
	}

	referencedColumn := referencedTable.GetColumn(o.References.Column)
	if referencedColumn == nil {
		return ColumnDoesNotExistError{Table: o.References.Table, Name: o.References.Column}
	}

	sql := fmt.Sprintf("ALTER TABLE %s ADD ", pq.QuoteIdentifier(table.Name))
	writer := &ConstraintSQLWriter{
		Name:           o.References.Name,
		Columns:        []string{column.Name},
		SkipValidation: true,
	}
	sql += writer.WriteForeignKey(
		referencedTable.Name,
		[]string{referencedColumn.Name},
		o.References.OnDelete,
		o.References.OnUpdate,
		nil,
		o.References.MatchType)

	_, err := conn.ExecContext(ctx, sql)
	return err
}
