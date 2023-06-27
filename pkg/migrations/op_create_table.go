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
	Columns []column `json:"columns"`
}

type column struct {
	Name       string         `json:"name"`
	Type       string         `json:"type"`
	Nullable   bool           `json:"nullable"`
	Unique     bool           `json:"unique"`
	PrimaryKey bool           `json:"pk"`
	Default    sql.NullString `json:"default"`
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

	s.Tables[o.Name] = schema.Table{
		Name:    tempName,
		Columns: columns,
	}

	return nil
}

func columnsToSQL(cols []column) string {
	var sql string
	for i, col := range cols {
		if i > 0 {
			sql += ", "
		}
		sql += fmt.Sprintf("%s %s", pq.QuoteIdentifier(col.Name), col.Type)

		if col.PrimaryKey {
			sql += " PRIMARY KEY"
		}
		if col.Unique {
			sql += " UNIQUE"
		}
		if !col.Nullable {
			sql += " NOT NULL"
		}
		if col.Default.Valid {
			sql += fmt.Sprintf(" DEFAULT %s", col.Default.String)
		}
	}
	return sql
}

func (o *OpCreateTable) Complete(ctx context.Context, conn *sql.DB) error {
	tempName := TemporaryName(o.Name)
	_, err := conn.ExecContext(ctx, fmt.Sprintf("ALTER TABLE %s RENAME TO %s",
		pq.QuoteIdentifier(tempName),
		pq.QuoteIdentifier(o.Name)))
	return err
}
