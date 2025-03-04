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
