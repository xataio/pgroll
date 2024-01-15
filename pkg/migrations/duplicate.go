// SPDX-License-Identifier: Apache-2.0

package migrations

import (
	"context"
	"database/sql"
	"fmt"
	"slices"
	"strings"

	"github.com/lib/pq"
	"github.com/xataio/pgroll/pkg/schema"
)

type Duplicator struct {
	conn     *sql.DB
	table    *schema.Table
	column   *schema.Column
	asName   string
	withType string
}

// NewColumnDuplicator creates a new Duplicator for a column.
func NewColumnDuplicator(conn *sql.DB, table *schema.Table, column *schema.Column) *Duplicator {
	return &Duplicator{
		conn:     conn,
		table:    table,
		column:   column,
		asName:   TemporaryName(column.Name),
		withType: column.Type,
	}
}

func (d *Duplicator) WithType(t string) *Duplicator {
	d.withType = t
	return d
}

// Duplicate creates a new column with the same type and foreign key
// constraints as the original column.
func (d *Duplicator) Duplicate(ctx context.Context) error {
	const (
		cAlterTableSQL    = `ALTER TABLE %s ADD COLUMN %s %s`
		cAddForeignKeySQL = `ADD CONSTRAINT %s FOREIGN KEY (%s) REFERENCES %s (%s)`
	)

	sql := fmt.Sprintf(cAlterTableSQL,
		pq.QuoteIdentifier(d.table.Name),
		pq.QuoteIdentifier(d.asName),
		d.withType)

	for _, fk := range d.table.ForeignKeys {
		if slices.Contains(fk.Columns, d.column.Name) {
			sql += fmt.Sprintf(", "+cAddForeignKeySQL,
				pq.QuoteIdentifier(TemporaryName(fk.Name)),
				strings.Join(quoteColumnNames(copyAndReplace(fk.Columns, d.column.Name, d.asName)), ", "),
				pq.QuoteIdentifier(fk.ReferencedTable),
				strings.Join(quoteColumnNames(fk.ReferencedColumns), ", "))
		}
	}

	_, err := d.conn.ExecContext(ctx, sql)

	return err
}

func copyAndReplace(xs []string, oldValue, newValue string) []string {
	ys := slices.Clone(xs)

	for i, c := range ys {
		if c == oldValue {
			ys[i] = newValue
		}
	}
	return ys
}
