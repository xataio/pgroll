// SPDX-License-Identifier: Apache-2.0

package migrations

import (
	"fmt"

	"github.com/lib/pq"
)

// IsNullable returns true if the column is nullable
func (c *Column) IsNullable() bool {
	return c.Nullable
}

// IsUnique returns true if the column values must be unique
func (c *Column) IsUnique() bool {
	return c.Unique
}

// IsPrimaryKey returns true if the column is part of the primary key
func (c *Column) IsPrimaryKey() bool {
	return c.Pk
}

// HasDefault returns true if the column has a default value
func (c *Column) HasDefault() bool {
	return c.Default != nil
}

// HasImplicitDefault returns true if the column has an implicit default value
func (c *Column) HasImplicitDefault() bool {
	switch c.Type {
	case "smallserial", "serial", "bigserial":
		return true
	default:
		return false
	}
}

// Validate returns true iff the column contains all fields required to create
// the column
func (c *Column) Validate() bool {
	if c.Name == "" {
		return false
	}
	if c.Type == "" {
		return false
	}
	return true
}

// ColumnSQLWriter writes a column to SQL
// It can optionally include the primary key constraint
// When creating a table, the primary key constraint is not added to the column definition
type ColumnSQLWriter struct {
	WithPK bool
}

func (w ColumnSQLWriter) Write(col Column) (string, error) {
	sql := fmt.Sprintf("%s %s", pq.QuoteIdentifier(col.Name), col.Type)

	if w.WithPK && col.IsPrimaryKey() {
		sql += " PRIMARY KEY"
	}

	if col.IsUnique() {
		sql += " UNIQUE"
	}
	if !col.IsNullable() {
		sql += " NOT NULL"
	}
	if col.Default != nil {
		sql += fmt.Sprintf(" DEFAULT %s", *col.Default)
	}

	if col.Generated != nil {
		if col.Generated.Expression != "" {
			sql += fmt.Sprintf(" GENERATED ALWAYS AS (%s) STORED", col.Generated.Expression)
		} else if col.Generated.Identity != nil {
			sql += fmt.Sprintf(" GENERATED %s AS IDENTITY", col.Generated.Identity.UserSpecifiedValues)
			if col.Generated.Identity.SequenceOptions != "" {
				sql += fmt.Sprintf(" (%s)", col.Generated.Identity.SequenceOptions)
			}
		}
	}

	if col.References != nil {
		writer := &ConstraintSQLWriter{Name: col.References.Name}
		sql += " " + writer.WriteForeignKey(
			col.References.Table,
			[]string{col.References.Column},
			col.References.OnDelete,
			col.References.OnUpdate,
			nil,
			col.References.MatchType)
	}
	if col.Check != nil {
		writer := &ConstraintSQLWriter{Name: col.Check.Name}
		sql += " " + writer.WriteCheck(col.Check.Constraint, col.Check.NoInherit)
	}
	return sql, nil
}
