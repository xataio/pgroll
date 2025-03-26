// SPDX-License-Identifier: Apache-2.0

package migrations

import (
	"context"
	"fmt"
	"strings"

	"github.com/lib/pq"
	"github.com/xataio/pgroll/pkg/db"
)

// DBAction is an interface for common database actions
// pgroll runs during migrations.
type DBAction interface {
	Execute(context.Context) error
}

// dropColumnAction is a DBAction that drops one or more columns from a table.
type dropColumnAction struct {
	conn db.DB

	table   string
	columns []string
}

func NewDropColumnAction(conn db.DB, table string, columns ...string) *dropColumnAction {
	return &dropColumnAction{
		conn:    conn,
		table:   table,
		columns: columns,
	}
}

func (a *dropColumnAction) Execute(ctx context.Context) error {
	_, err := a.conn.ExecContext(ctx, fmt.Sprintf("ALTER TABLE IF EXISTS %s %s",
		pq.QuoteIdentifier(a.table),
		a.dropMultipleColumns()))
	return err
}

func (a *dropColumnAction) dropMultipleColumns() string {
	cols := make([]string, len(a.columns))
	for i, col := range a.columns {
		cols[i] = "DROP COLUMN IF EXISTS " + pq.QuoteIdentifier(col)
	}
	return strings.Join(cols, ", ")
}

// renameColumnAction is a DBAction that renames a column in a table.
type renameColumnAction struct {
	conn db.DB

	table string
	from  string
	to    string
}

func NewRenameColumnAction(conn db.DB, table, from, to string) *renameColumnAction {
	return &renameColumnAction{
		conn:  conn,
		table: table,
		from:  from,
		to:    to,
	}
}

func (a *renameColumnAction) Execute(ctx context.Context) error {
	_, err := a.conn.ExecContext(ctx, fmt.Sprintf("ALTER TABLE IF EXISTS %s RENAME COLUMN %s TO %s",
		pq.QuoteIdentifier(a.table),
		pq.QuoteIdentifier(a.from),
		pq.QuoteIdentifier(a.to)))
	return err
}

// renameConstraintAction is a DBAction that renames a constraint in a table.
type renameConstraintAction struct {
	conn  db.DB
	table string
	from  string
	to    string
}

func NewRenameConstraintAction(conn db.DB, table, from, to string) *renameConstraintAction {
	return &renameConstraintAction{
		conn:  conn,
		table: table,
		from:  from,
		to:    to,
	}
}

func (a *renameConstraintAction) Execute(ctx context.Context) error {
	_, err := a.conn.ExecContext(ctx, fmt.Sprintf("ALTER TABLE IF EXISTS %s RENAME CONSTRAINT %s TO %s",
		pq.QuoteIdentifier(a.table),
		pq.QuoteIdentifier(a.from),
		pq.QuoteIdentifier(a.to)))
	return err
}

// dropFunctionAction is a DBAction that drops a function and all of its dependencies (cascade).
type dropFunctionAction struct {
	conn      db.DB
	functions []string
}

func NewDropFunctionAction(conn db.DB, functions ...string) *dropFunctionAction {
	return &dropFunctionAction{
		conn:      conn,
		functions: functions,
	}
}

func (a *dropFunctionAction) Execute(ctx context.Context) error {
	functions := make([]string, len(a.functions))
	for idx, fn := range a.functions {
		functions[idx] = pq.QuoteIdentifier(fn)
	}
	_, err := a.conn.ExecContext(ctx, fmt.Sprintf("DROP FUNCTION IF EXISTS %s CASCADE",
		strings.Join(functions, ",")))
	return err
}

// commentColumnAction is a DBAction that adds a comment to a column in a table.
type commentColumnAction struct {
	conn    db.DB
	table   string
	column  string
	comment *string
}

func NewCommentColumnAction(conn db.DB, table, column string, comment *string) *commentColumnAction {
	return &commentColumnAction{
		conn:    conn,
		table:   table,
		column:  column,
		comment: comment,
	}
}

func (a *commentColumnAction) Execute(ctx context.Context) error {
	commentSQL := fmt.Sprintf("COMMENT ON COLUMN %s.%s IS %s",
		pq.QuoteIdentifier(a.table),
		pq.QuoteIdentifier(a.column),
		commentToSQL(a.comment))

	_, err := a.conn.ExecContext(ctx, commentSQL)
	return err
}

// commentTableAction is a DBAction that adds a comment to a table.
type commentTableAction struct {
	conn    db.DB
	table   string
	comment *string
}

func NewCommentTableAction(conn db.DB, table string, comment *string) *commentTableAction {
	return &commentTableAction{
		conn:    conn,
		table:   table,
		comment: comment,
	}
}

func (a *commentTableAction) Execute(ctx context.Context) error {
	commentSQL := fmt.Sprintf("COMMENT ON TABLE %s IS %s",
		pq.QuoteIdentifier(a.table),
		commentToSQL(a.comment))

	_, err := a.conn.ExecContext(ctx, commentSQL)
	return err
}

func commentToSQL(comment *string) string {
	if comment == nil {
		return "NULL"
	}
	return pq.QuoteLiteral(*comment)
}
