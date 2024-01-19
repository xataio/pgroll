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
		cAlterTableSQL         = `ALTER TABLE %s ADD COLUMN %s %s`
		cSetDefaultSQL         = `ALTER COLUMN %s SET DEFAULT %s`
		cAddForeignKeySQL      = `ADD CONSTRAINT %s FOREIGN KEY (%s) REFERENCES %s (%s)`
		cAddCheckConstraintSQL = `ADD CONSTRAINT %s %s NOT VALID`
	)

	// Generate SQL to duplicate the column's name and type
	sql := fmt.Sprintf(cAlterTableSQL,
		pq.QuoteIdentifier(d.table.Name),
		pq.QuoteIdentifier(d.asName),
		d.withType)

	// Generate SQL to duplicate the column's default value
	if d.column.Default != nil {
		sql += fmt.Sprintf(", "+cSetDefaultSQL, d.asName, *d.column.Default)
	}

	// Generate SQL to duplicate any foreign key constraints on the column
	for _, fk := range d.table.ForeignKeys {
		if slices.Contains(fk.Columns, d.column.Name) {
			sql += fmt.Sprintf(", "+cAddForeignKeySQL,
				pq.QuoteIdentifier(DuplicationName(fk.Name)),
				strings.Join(quoteColumnNames(copyAndReplace(fk.Columns, d.column.Name, d.asName)), ", "),
				pq.QuoteIdentifier(fk.ReferencedTable),
				strings.Join(quoteColumnNames(fk.ReferencedColumns), ", "))
		}
	}

	// Generate SQL to duplicate any check constraints on the column
	for _, cc := range d.table.CheckConstraints {
		if slices.Contains(cc.Columns, d.column.Name) {
			sql += fmt.Sprintf(", "+cAddCheckConstraintSQL,
				pq.QuoteIdentifier(DuplicationName(cc.Name)),
				rewriteCheckExpression(cc.Definition, d.column.Name, d.asName),
			)
		}
	}

	_, err := d.conn.ExecContext(ctx, sql)

	return err
}

func DuplicationName(name string) string {
	return "_pgroll_dup_" + name
}

func IsDuplicatedName(name string) bool {
	return strings.HasPrefix(name, "_pgroll_dup_")
}

func StripDuplicationPrefix(name string) string {
	return strings.TrimPrefix(name, "_pgroll_dup_")
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
