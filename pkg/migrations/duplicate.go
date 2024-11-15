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

// ColumnDuplicator duplicates a column in a table, including all constraints and
// comments.
type ColumnDuplicator struct {
	duplicator        *duplicator
	column            *schema.Column
	asName            string
	withoutNotNull    bool
	withType          string
	withoutConstraint string
}

// ColumnGroupDuplicator duplicates a group of columns in a table, including all constraints and
// comments.
type ColumnGroupDuplicator struct {
	duplicator *duplicator
	columns    []*schema.Column
}

type duplicator struct {
	conn  db.DB
	table *schema.Table
}

const (
	dataTypeMismatchErrorCode  pq.ErrorCode = "42804"
	undefinedFunctionErrorCode pq.ErrorCode = "42883"

	cCreateUniqueIndexSQL            = `CREATE UNIQUE INDEX CONCURRENTLY %s ON %s (%s)`
	cSetDefaultSQL                   = `ALTER TABLE %s ALTER COLUMN %s SET DEFAULT %s`
	cAlterTableAddCheckConstraintSQL = `ALTER TABLE %s ADD CONSTRAINT %s %s NOT VALID`
)

// NewColumnDuplicator creates a new Duplicator for a column.
func NewColumnDuplicator(conn db.DB, table *schema.Table, column *schema.Column) *ColumnDuplicator {
	return &ColumnDuplicator{
		duplicator: &duplicator{
			conn:  conn,
			table: table,
		},
		column:   column,
		asName:   TemporaryName(column.Name),
		withType: column.Type,
	}
}

func NewColumnGroupDuplicator(conn db.DB, table *schema.Table, columns []*schema.Column) *ColumnGroupDuplicator {
	return &ColumnGroupDuplicator{
		duplicator: &duplicator{
			conn:  conn,
			table: table,
		},
		columns: columns,
	}
}

// WithType sets the type of the new column.
func (d *ColumnDuplicator) WithType(t string) *ColumnDuplicator {
	d.withType = t
	return d
}

// WithoutConstraint excludes a constraint from being duplicated.
func (d *ColumnDuplicator) WithoutConstraint(c string) *ColumnDuplicator {
	d.withoutConstraint = c
	return d
}

// WithoutNotNull excludes the NOT NULL constraint from being duplicated.
func (d *ColumnDuplicator) WithoutNotNull() *ColumnDuplicator {
	d.withoutNotNull = true
	return d
}

// Duplicate duplicates a column in the table, including all constraints and
// comments.
func (d *ColumnDuplicator) Duplicate(ctx context.Context) error {
	// Duplicate the column with the new type
	// and check and fk constraints
	if err := d.duplicator.duplicateColumn(ctx, d.column, d.asName, d.withoutNotNull, d.withType, d.withoutConstraint); err != nil {
		return err
	}

	// Duplicate the column's default value
	if err := d.duplicator.duplicateDefault(ctx, d.column, d.asName); err != nil {
		return err
	}

	// Generate SQL to duplicate any check constraints on the column. This may faile
	// if the check constraint is not valid for the new column type, in which case
	// the error is ignored.
	for _, cc := range d.duplicator.table.CheckConstraints {
		if cc.Name == d.withoutConstraint {
			continue
		}

		if slices.Contains(cc.Columns, d.column.Name) {
			sql := fmt.Sprintf(cAlterTableAddCheckConstraintSQL,
				pq.QuoteIdentifier(d.duplicator.table.Name),
				pq.QuoteIdentifier(DuplicationName(cc.Name)),
				rewriteCheckExpression(cc.Definition, d.column.Name),
			)

			_, err := d.duplicator.conn.ExecContext(ctx, sql)

			err = errorIgnoringErrorCode(err, undefinedFunctionErrorCode)
			if err != nil {
				return err
			}
		}
	}

	if err := d.duplicator.duplicateComment(ctx, d.column, d.asName); err != nil {
		return err
	}

	// Generate SQL to duplicate any unique constraints on the column
	// The constraint is duplicated by adding a unique index on the column concurrently.
	// The index is converted into a unique constraint on migration completion.
	for _, uc := range d.duplicator.table.UniqueConstraints {
		if uc.Name == d.withoutConstraint {
			continue
		}

		if slices.Contains(uc.Columns, d.column.Name) {
			sql := fmt.Sprintf(cCreateUniqueIndexSQL,
				pq.QuoteIdentifier(DuplicationName(uc.Name)),
				pq.QuoteIdentifier(d.duplicator.table.Name),
				strings.Join(quoteColumnNames(copyAndReplace(uc.Columns, d.column.Name, d.asName)), ", "),
			)

			_, err := d.duplicator.conn.ExecContext(ctx, sql)
			if err != nil {
				return err
			}
		}
	}

	return nil
}

func (cg *ColumnGroupDuplicator) Duplicate(ctx context.Context) error {
	for _, column := range cg.columns {
		asName := TemporaryName(column.Name)
		withoutNotNull := false
		withoutConstraint := ""
		// Duplicate the column with the new type
		// and check and fk constraints
		if err := cg.duplicator.duplicateColumn(ctx, column, asName, withoutNotNull, column.Type, withoutConstraint); err != nil {
			return err
		}

		// Duplicate the column's default value
		if err := cg.duplicator.duplicateDefault(ctx, column, asName); err != nil {
			return err
		}

		// Duplicate the column's comment
		if err := cg.duplicator.duplicateComment(ctx, column, asName); err != nil {
			return err
		}
	}

	// Generate SQL to duplicate any check constraints on the column group.
	for _, cc := range cg.duplicator.table.CheckConstraints {
		sameColumns := true
		colNames := make([]string, len(cg.columns))
		for i, column := range cg.columns {
			if !slices.Contains(cc.Columns, column.Name) {
				sameColumns = false
				break
			}
			colNames[i] = column.Name
		}

		if sameColumns {
			sql := fmt.Sprintf(cAlterTableAddCheckConstraintSQL,
				pq.QuoteIdentifier(cg.duplicator.table.Name),
				pq.QuoteIdentifier(DuplicationName(cc.Name)),
				rewriteCheckExpression(cc.Definition, colNames...),
			)

			if _, err := cg.duplicator.conn.ExecContext(ctx, sql); err != nil {
				return err
			}
		}
	}

	// Generate SQL to duplicate any unique constraints on the column group.
	for _, uc := range cg.duplicator.table.UniqueConstraints {
		sameColumns := true
		for _, column := range cg.columns {
			if !slices.Contains(uc.Columns, column.Name) {
				sameColumns = false
				break
			}
		}

		if sameColumns {
			sql := fmt.Sprintf(cCreateUniqueIndexSQL,
				pq.QuoteIdentifier(DuplicationName(uc.Name)),
				pq.QuoteIdentifier(cg.duplicator.table.Name),
				strings.Join(quotedTemporaryNames(uc.Columns), ", "),
			)

			if _, err := cg.duplicator.conn.ExecContext(ctx, sql); err != nil {
				return err
			}
		}
	}

	return nil
}

func (d *duplicator) duplicateColumn(
	ctx context.Context,
	column *schema.Column,
	asName string,
	withoutNotNull bool,
	withType string,
	withoutConstraint string,
) error {
	const (
		cAlterTableSQL         = `ALTER TABLE %s ADD COLUMN %s %s`
		cAddForeignKeySQL      = `ADD CONSTRAINT %s FOREIGN KEY (%s) REFERENCES %s (%s) ON DELETE %s`
		cAddCheckConstraintSQL = `ADD CONSTRAINT %s %s NOT VALID`
	)

	// Generate SQL to duplicate the column's name and type
	sql := fmt.Sprintf(cAlterTableSQL,
		pq.QuoteIdentifier(d.table.Name),
		pq.QuoteIdentifier(asName),
		withType)

	// Generate SQL to add an unchecked NOT NULL constraint if the original column
	// is NOT NULL. The constraint will be validated on migration completion.
	if !column.Nullable && !withoutNotNull {
		sql += fmt.Sprintf(", "+cAddCheckConstraintSQL,
			pq.QuoteIdentifier(DuplicationName(NotNullConstraintName(column.Name))),
			fmt.Sprintf("CHECK (%s IS NOT NULL)", pq.QuoteIdentifier(asName)),
		)
	}

	// Generate SQL to duplicate any foreign key constraints on the column
	for _, fk := range d.table.ForeignKeys {
		if fk.Name == withoutConstraint {
			continue
		}

		if slices.Contains(fk.Columns, column.Name) {
			sql += fmt.Sprintf(", "+cAddForeignKeySQL,
				pq.QuoteIdentifier(DuplicationName(fk.Name)),
				strings.Join(quoteColumnNames(copyAndReplace(fk.Columns, column.Name, asName)), ", "),
				pq.QuoteIdentifier(fk.ReferencedTable),
				strings.Join(quoteColumnNames(fk.ReferencedColumns), ", "),
				fk.OnDelete,
			)
		}
	}

	_, err := d.conn.ExecContext(ctx, sql)
	return err
}

func (d *duplicator) duplicateDefault(ctx context.Context, column *schema.Column, asName string) error {
	if column.Default == nil {
		return nil
	}

	const cSetDefaultSQL = `ALTER TABLE %s ALTER COLUMN %s SET DEFAULT %s`

	// Generate SQL to duplicate any default value on the column. This may fail
	// if the default value is not valid for the new column type, in which case
	// the error is ignored.
	sql := fmt.Sprintf(cSetDefaultSQL, pq.QuoteIdentifier(d.table.Name), asName, *column.Default)

	_, err := d.conn.ExecContext(ctx, sql)

	return errorIgnoringErrorCode(err, dataTypeMismatchErrorCode)
}

func (d *duplicator) duplicateComment(ctx context.Context, column *schema.Column, asName string) error {
	if column.Comment == "" {
		return nil
	}

	const cCommentOnColumnSQL = `COMMENT ON COLUMN %s.%s IS %s`

	// Generate SQL to duplicate the column's comment
	sql := fmt.Sprintf(cCommentOnColumnSQL,
		pq.QuoteIdentifier(d.table.Name),
		pq.QuoteIdentifier(asName),
		pq.QuoteLiteral(column.Comment),
	)

	_, err := d.conn.ExecContext(ctx, sql)
	return err
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
