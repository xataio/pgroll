// SPDX-License-Identifier: Apache-2.0

package migrations

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/lib/pq"
	"github.com/xataio/pg-roll/pkg/schema"
)

var _ Operation = (*OpCreateTable)(nil)

type OpCreateTable struct {
	Name    string   `json:"name"`
	Columns []Column `json:"columns"`
}

type Column struct {
	Name       string               `json:"name"`
	Type       string               `json:"type"`
	Nullable   bool                 `json:"nullable"`
	Unique     bool                 `json:"unique"`
	PrimaryKey bool                 `json:"pk"`
	Default    *string              `json:"default"`
	Check      *CheckConstraint     `json:"check"`
	References *ForeignKeyReference `json:"references"`
}

func (o *OpCreateTable) Start(ctx context.Context, conn *sql.DB, stateSchema string, s *schema.Schema) error {
	tempName := TemporaryName(o.Name)
	_, err := conn.ExecContext(ctx, fmt.Sprintf("CREATE TABLE %s (%s)",
		pq.QuoteIdentifier(tempName),
		columnsToSQL(o.Columns)))
	if err != nil {
		return err
	}

	columns := make(map[string]schema.Column, len(o.Columns))
	for _, col := range o.Columns {
		columns[col.Name] = schema.Column{
			Name: col.Name,
		}
	}

	s.AddTable(o.Name, schema.Table{
		Name:    tempName,
		Columns: columns,
	})

	return nil
}

func (o *OpCreateTable) Complete(ctx context.Context, conn *sql.DB) error {
	tempName := TemporaryName(o.Name)
	_, err := conn.ExecContext(ctx, fmt.Sprintf("ALTER TABLE IF EXISTS %s RENAME TO %s",
		pq.QuoteIdentifier(tempName),
		pq.QuoteIdentifier(o.Name)))
	return err
}

func (o *OpCreateTable) Rollback(ctx context.Context, conn *sql.DB) error {
	tempName := TemporaryName(o.Name)

	_, err := conn.ExecContext(ctx, fmt.Sprintf("DROP TABLE IF EXISTS %s",
		pq.QuoteIdentifier(tempName)))
	return err
}

func (o *OpCreateTable) Validate(ctx context.Context, s *schema.Schema) error {
	table := s.GetTable(o.Name)
	if table != nil {
		return TableAlreadyExistsError{Name: o.Name}
	}

	for _, col := range o.Columns {
		// Ensure that any foreign key references are valid, ie. the referenced
		// table and column exist.
		if col.References != nil {
			if err := col.References.Validate(s); err != nil {
				return ColumnReferenceError{
					Table:  o.Name,
					Column: col.Name,
					Err:    err,
				}
			}
		}

		// Ensure that any CHECK constraints are valid.
		if col.Check != nil {
			if err := col.Check.Validate(); err != nil {
				return CheckConstraintError{
					Table:  o.Name,
					Column: col.Name,
					Err:    err,
				}
			}
		}
	}

	return nil
}

func columnsToSQL(cols []Column) string {
	var sql string
	for i, col := range cols {
		if i > 0 {
			sql += ", "
		}
		sql += ColumnToSQL(col)
	}
	return sql
}

func ColumnToSQL(col Column) string {
	sql := fmt.Sprintf("%s %s", pq.QuoteIdentifier(col.Name), col.Type)

	if col.PrimaryKey {
		sql += " PRIMARY KEY"
	}
	if col.Unique {
		sql += " UNIQUE"
	}
	if !col.Nullable {
		sql += " NOT NULL"
	}
	if col.Default != nil {
		sql += fmt.Sprintf(" DEFAULT %s", pq.QuoteLiteral(*col.Default))
	}
	if col.References != nil {
		sql += fmt.Sprintf(" CONSTRAINT %s REFERENCES %s(%s)",
			pq.QuoteIdentifier(col.References.Name),
			pq.QuoteIdentifier(col.References.Table),
			pq.QuoteIdentifier(col.References.Column))
	}
	if col.Check != nil {
		sql += fmt.Sprintf(" CONSTRAINT %s CHECK (%s)",
			pq.QuoteIdentifier(col.Check.Name),
			col.Check.Constraint)
	}
	return sql
}
