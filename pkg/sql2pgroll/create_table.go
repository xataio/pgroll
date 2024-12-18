// SPDX-License-Identifier: Apache-2.0

package sql2pgroll

import (
	"fmt"

	pgq "github.com/xataio/pg_query_go/v6"

	"github.com/xataio/pgroll/pkg/migrations"
)

// convertCreateStmt converts a CREATE TABLE statement to a pgroll operation.
func convertCreateStmt(stmt *pgq.CreateStmt) (migrations.Operations, error) {
	// Check if the statement can be converted
	if !canConvertCreateStatement(stmt) {
		return nil, nil
	}

	// Convert the column definitions
	columns := make([]migrations.Column, 0, len(stmt.TableElts))
	for _, elt := range stmt.TableElts {
		column, err := convertColumnDef(elt.GetColumnDef())
		if err != nil {
			return nil, fmt.Errorf("error converting column definition: %w", err)
		}
		columns = append(columns, *column)
	}

	return migrations.Operations{
		&migrations.OpCreateTable{
			Name:    stmt.Relation.Relname,
			Columns: columns,
		},
	}, nil
}

// canConvertCreateTableStatement returns true iff `stmt` can be converted to a
// pgroll operation.
func canConvertCreateStatement(stmt *pgq.CreateStmt) bool {
	switch {
	// Temporary and unlogged tables are not supported
	case stmt.GetRelation().GetRelpersistence() != "p":
		return false
	// CREATE TABLE IF NOT EXISTS is not supported
	case stmt.GetIfNotExists():
		return false
	// Table inheritance is not supported
	case len(stmt.GetInhRelations()) != 0:
		return false
	// Paritioned tables are not supported
	case stmt.GetPartspec() != nil:
		return false
	default:
		return true
	}
}

func convertColumnDef(col *pgq.ColumnDef) (*migrations.Column, error) {
	// Convert the column type
	typeString, err := pgq.DeparseTypeName(col.TypeName)
	if err != nil {
		return nil, fmt.Errorf("error deparsing column type: %w", err)
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

	return &migrations.Column{
		Name:     col.Colname,
		Type:     typeString,
		Nullable: !notNull,
		Unique:   unique,
		Default:  defaultValue,
		Pk:       pk,
	}, nil
}
