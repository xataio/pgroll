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
	References *ForeignKeyReference `json:"references"`
}

type ForeignKeyReference struct {
	Name   string `json:"name"`
	Table  string `json:"table"`
	Column string `json:"column"`
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
		if col.References != nil {
			table := s.GetTable(col.References.Table)
			if table == nil {
				return ColumnReferenceError{
					Table:  o.Name,
					Column: col.Name,
					Err:    TableDoesNotExistError{Name: col.References.Table},
				}
			}
			if _, ok := table.Columns[col.References.Column]; !ok {
				return ColumnReferenceError{
					Table:  o.Name,
					Column: col.Name,
					Err: ColumnDoesNotExistError{
						Table: col.References.Table,
						Name:  col.References.Column,
					},
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
		tableRef := col.References.Table
		columnRef := col.References.Column

		sql += fmt.Sprintf(" CONSTRAINT %s REFERENCES %s(%s)",
			pq.QuoteIdentifier(ForeignKeyConstraintName(col.Name, tableRef, columnRef)),
			pq.QuoteIdentifier(tableRef),
			pq.QuoteIdentifier(columnRef))
	}
	return sql
}

func ForeignKeyConstraintName(columnName, tableRef, columnRef string) string {
	return "_pgroll_fk_" + columnName + "_" + tableRef + "_" + columnRef
}
