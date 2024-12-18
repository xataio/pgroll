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

	// Convert the table elements - table elements can be:
	// - Column definitions
	// - Constraints
	// - LIKE clauses (not supported)
	var columns []migrations.Column
	for _, elt := range stmt.TableElts {
		switch elt.Node.(type) {
		case *pgq.Node_ColumnDef:
			column, err := convertColumnDef(elt.GetColumnDef())
			if err != nil {
				return nil, fmt.Errorf("error converting column definition: %w", err)
			}
			if column == nil {
				return nil, nil
			}
			columns = append(columns, *column)
		default:
			return nil, nil
		}
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
	case
		// Temporary and unlogged tables are not supported
		stmt.GetRelation().GetRelpersistence() != "p",
		// CREATE TABLE IF NOT EXISTS is not supported
		stmt.GetIfNotExists(),
		// Table inheritance is not supported
		len(stmt.GetInhRelations()) != 0,
		// Paritioned tables are not supported
		stmt.GetPartspec() != nil,
		// Specifying an access method is not supported
		stmt.GetAccessMethod() != "",
		// Specifying storage options is not supported
		len(stmt.GetOptions()) != 0,
		// ON COMMIT options are not supported
		stmt.GetOncommit() != pgq.OnCommitAction_ONCOMMIT_NOOP,
		// Setting a tablespace is not supported
		stmt.GetTablespacename() != "",
		// CREATE TABLE OF type_name is not supported
		stmt.GetOfTypename() != nil:
		return false
	default:
		return true
	}
}

func convertColumnDef(col *pgq.ColumnDef) (*migrations.Column, error) {
	if !canConvertColumnDef(col) {
		return nil, nil
	}

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

// canConvertColumnDef returns true iff `col` can be converted to a pgroll
// `Column` definition.
func canConvertColumnDef(col *pgq.ColumnDef) bool {
	switch {
	case
		col.GetStorageName() != "",
		// Column compression options are not supported
		col.GetCompression() != "",
		// Column collation options are not supported
		col.GetCollClause() != nil:
		return false
	default:
		return true
	}
}
