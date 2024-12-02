// SPDX-License-Identifier: Apache-2.0

package sql2pgroll

import (
	"fmt"
	"strings"

	pgq "github.com/pganalyze/pg_query_go/v6"
	"github.com/xataio/pgroll/pkg/migrations"
)

// convertCreateStmt converts a CREATE TABLE statement to a pgroll operation.
func convertCreateStmt(stmt *pgq.CreateStmt) ([]migrations.Operation, error) {
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
	ignoredTypeParts := map[string]bool{
		"pg_catalog": true,
	}

	// Build the type name, including any schema qualifiers
	typeParts := make([]string, 0, len(col.GetTypeName().Names))
	for _, node := range col.GetTypeName().Names {
		typePart := node.GetString_().GetSval()
		if _, ok := ignoredTypeParts[typePart]; ok {
			continue
		}
		typeParts = append(typeParts, typePart)
	}

	// Build the type modifiers, such as precision and scale for numeric types
	var typeMods []string
	for _, node := range col.GetTypeName().Typmods {
		if x, ok := node.GetAConst().Val.(*pgq.A_Const_Ival); ok {
			typeMods = append(typeMods, fmt.Sprintf("%d", x.Ival.GetIval()))
		}
	}
	var typeModifier string
	if len(typeMods) > 0 {
		typeModifier = fmt.Sprintf("(%s)", strings.Join(typeMods, ","))
	}

	// Build the array bounds for array types
	var arrayBounds string
	for _, node := range col.GetTypeName().ArrayBounds {
		bound := node.GetInteger().GetIval()
		if bound == -1 {
			arrayBounds = "[]"
		} else {
			arrayBounds = fmt.Sprintf("%s[%d]", arrayBounds, bound)
		}
	}

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
		Type:     strings.Join(typeParts, ".") + typeModifier + arrayBounds,
		Nullable: !notNull,
		Unique:   unique,
		Default:  defaultValue,
		Pk:       pk,
	}
}
