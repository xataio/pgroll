// SPDX-License-Identifier: Apache-2.0

package migrations

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"

	"github.com/lib/pq"
	"github.com/xataio/pgroll/pkg/db"
	"github.com/xataio/pgroll/pkg/schema"
)

// Duplicator duplicates a column in a table, including all constraints and
// comments.
type Duplicator struct {
	conn              db.DB
	table             *schema.Table
	column            *schema.Column
	asName            string
	withoutNotNull    bool
	withType          string
	withoutConstraint string
}

const (
	dataTypeMismatchErrorCode  pq.ErrorCode = "42804"
	undefinedFunctionErrorCode pq.ErrorCode = "42883"
)

// NewColumnDuplicator creates a new Duplicator for a column.
func NewColumnDuplicator(conn db.DB, table *schema.Table, column *schema.Column) *Duplicator {
	return &Duplicator{
		conn:     conn,
		table:    table,
		column:   column,
		asName:   TemporaryName(column.Name),
		withType: column.Type,
	}
}

// WithType sets the type of the new column.
func (d *Duplicator) WithType(t string) *Duplicator {
	d.withType = t
	return d
}

// WithoutConstraint excludes a constraint from being duplicated.
func (d *Duplicator) WithoutConstraint(c string) *Duplicator {
	d.withoutConstraint = c
	return d
}

// WithoutNotNull excludes the NOT NULL constraint from being duplicated.
func (d *Duplicator) WithoutNotNull() *Duplicator {
	d.withoutNotNull = true
	return d
}

// Duplicate duplicates a column in the table, including all constraints and
// comments.
func (d *Duplicator) Duplicate(ctx context.Context) error {
	const (
		cAlterTableSQL                   = `ALTER TABLE %s ADD COLUMN %s %s`
		cAddForeignKeySQL                = `ADD CONSTRAINT %s FOREIGN KEY (%s) REFERENCES %s (%s) ON DELETE %s`
		cAddCheckConstraintSQL           = `ADD CONSTRAINT %s %s NOT VALID`
		cCreateUniqueIndexSQL            = `CREATE UNIQUE INDEX CONCURRENTLY %s ON %s (%s)`
		cSetDefaultSQL                   = `ALTER TABLE %s ALTER COLUMN %s SET DEFAULT %s`
		cAlterTableAddCheckConstraintSQL = `ALTER TABLE %s ADD CONSTRAINT %s %s NOT VALID`
		cCommentOnColumnSQL              = `COMMENT ON COLUMN %s.%s IS %s`
	)

	// Generate SQL to duplicate the column's name and type
	sql := fmt.Sprintf(cAlterTableSQL,
		pq.QuoteIdentifier(d.table.Name),
		pq.QuoteIdentifier(d.asName),
		d.withType)

	// Generate SQL to add an unchecked NOT NULL constraint if the original column
	// is NOT NULL. The constraint will be validated on migration completion.
	if !d.column.Nullable && !d.withoutNotNull {
		sql += fmt.Sprintf(", "+cAddCheckConstraintSQL,
			pq.QuoteIdentifier(DuplicationName(NotNullConstraintName(d.column.Name))),
			fmt.Sprintf("CHECK (%s IS NOT NULL)", pq.QuoteIdentifier(d.asName)),
		)
	}

	// Generate SQL to duplicate any foreign key constraints on the column
	for _, fk := range d.table.ForeignKeys {
		if fk.Name == d.withoutConstraint {
			continue
		}

		if slices.Contains(fk.Columns, d.column.Name) {
			sql += fmt.Sprintf(", "+cAddForeignKeySQL,
				pq.QuoteIdentifier(DuplicationName(fk.Name)),
				strings.Join(quoteColumnNames(copyAndReplace(fk.Columns, d.column.Name, d.asName)), ", "),
				pq.QuoteIdentifier(fk.ReferencedTable),
				strings.Join(quoteColumnNames(fk.ReferencedColumns), ", "),
				fk.OnDelete,
			)
		}
	}

	_, err := d.conn.ExecContext(ctx, sql)
	if err != nil {
		return err
	}

	// Generate SQL to duplicate any default value on the column. This may fail
	// if the default value is not valid for the new column type, in which case
	// the error is ignored.
	if d.column.Default != nil {
		sql := fmt.Sprintf(cSetDefaultSQL, pq.QuoteIdentifier(d.table.Name), d.asName, *d.column.Default)

		_, err := d.conn.ExecContext(ctx, sql)

		err = errorIgnoringErrorCode(err, dataTypeMismatchErrorCode)
		if err != nil {
			return err
		}
	}

	// Generate SQL to duplicate any check constraints on the column. This may faile
	// if the check constraint is not valid for the new column type, in which case
	// the error is ignored.
	for _, cc := range d.table.CheckConstraints {
		if cc.Name == d.withoutConstraint {
			continue
		}

		if slices.Contains(cc.Columns, d.column.Name) {
			sql := fmt.Sprintf(cAlterTableAddCheckConstraintSQL,
				pq.QuoteIdentifier(d.table.Name),
				pq.QuoteIdentifier(DuplicationName(cc.Name)),
				rewriteCheckExpression(cc.Definition, d.column.Name),
			)

			_, err := d.conn.ExecContext(ctx, sql)

			err = errorIgnoringErrorCode(err, undefinedFunctionErrorCode)
			if err != nil {
				return err
			}
		}
	}

	// Generate SQL to duplicate the column's comment
	if d.column.Comment != "" {
		sql = fmt.Sprintf(cCommentOnColumnSQL,
			pq.QuoteIdentifier(d.table.Name),
			pq.QuoteIdentifier(d.asName),
			pq.QuoteLiteral(d.column.Comment),
		)

		_, err = d.conn.ExecContext(ctx, sql)
		if err != nil {
			return err
		}
	}

	// Generate SQL to duplicate any unique constraints on the column
	// The constraint is duplicated by adding a unique index on the column concurrently.
	// The index is converted into a unique constraint on migration completion.
	for _, uc := range d.table.UniqueConstraints {
		if uc.Name == d.withoutConstraint {
			continue
		}

		if slices.Contains(uc.Columns, d.column.Name) {
			sql = fmt.Sprintf(cCreateUniqueIndexSQL,
				pq.QuoteIdentifier(DuplicationName(uc.Name)),
				pq.QuoteIdentifier(d.table.Name),
				strings.Join(quoteColumnNames(copyAndReplace(uc.Columns, d.column.Name, d.asName)), ", "),
			)

			_, err = d.conn.ExecContext(ctx, sql)
			if err != nil {
				return err
			}
		}
	}

	return nil
}

// DiplicationName returns the name of a duplicated column.
func DuplicationName(name string) string {
	return "_pgroll_dup_" + name
}

// IsDuplicatedName returns true if the name is a duplicated column name.
func IsDuplicatedName(name string) bool {
	return strings.HasPrefix(name, "_pgroll_dup_")
}

// StripDuplicationPrefix removes the duplication prefix from a column name.
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

func errorIgnoringErrorCode(err error, code pq.ErrorCode) error {
	pqErr := &pq.Error{}
	if ok := errors.As(err, &pqErr); ok {
		if pqErr.Code == code {
			return nil
		}
	}

	return err
}
