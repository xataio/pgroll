package migrations

import (
	"context"
	"database/sql"
	"fmt"

	"pg-roll/pkg/schema"

	"github.com/lib/pq"
)

var _ Operation = (*OpCreateTable)(nil)

type OpCreateTable struct {
	Name    string   `json:"name"`
	Columns []Column `json:"columns"`
}

type Column struct {
	Name       string  `json:"name"`
	Type       string  `json:"type"`
	Nullable   bool    `json:"nullable"`
	Unique     bool    `json:"unique"`
	PrimaryKey bool    `json:"pk"`
	Default    *string `json:"default"`
}

func (o *OpCreateTable) Start(ctx context.Context, conn *sql.DB, s *schema.Schema) error {
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
	return sql
}
