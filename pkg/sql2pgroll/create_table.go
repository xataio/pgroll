// SPDX-License-Identifier: Apache-2.0

package sql2pgroll

import (
	pgq "github.com/xataio/pg_query_go/v6"

	"github.com/xataio/pgroll/pkg/migrations"
)

// convertCreateStmt converts a CREATE TABLE statement to a pgroll operation.
func convertCreateStmt(stmt *pgq.CreateStmt) (migrations.Operations, error) {
	columns := make([]migrations.Column, 0, len(stmt.TableElts))
	for _, elt := range stmt.TableElts {
		columns = append(columns, convertColumnDef(elt.GetColumnDef()))
	}

	return migrations.Operations{
		&migrations.OpCreateTable{
			Name:    stmt.Relation.Relname,
			Columns: columns,
		},
	}, nil
}

func convertColumnDef(col *pgq.ColumnDef) migrations.Column {
	// Convert the column type
	typeString := convertTypeName(col.TypeName)

	// Determine column nullability, uniqueness, and primary key status
	var notNull, unique, pk bool
	var defaultValue *string
	for _, constraint := range col.Constraints {
		if constraint.GetConstraint().GetContype() == pgq.ConstrType_CONSTR_NOTNULL {
			notNull = true
		}
		if constraint.GetConstraint().GetContype() == pgq.ConstrType_CONSTR_UNIQUE {
			unique = true
		}
		if constraint.GetConstraint().GetContype() == pgq.ConstrType_CONSTR_PRIMARY {
			pk = true
			notNull = true
		}
	}

	return migrations.Column{
		Name:     col.Colname,
		Type:     typeString,
		Nullable: !notNull,
		Unique:   unique,
		Default:  defaultValue,
		Pk:       pk,
	}
}
