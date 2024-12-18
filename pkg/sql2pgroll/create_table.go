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
	// Specifying an access method is not supported
	case stmt.GetAccessMethod() != "":
		return false
	// Specifying storage options is not supported
	case len(stmt.GetOptions()) != 0:
		return false
	// ON COMMIT options are not supported
	case stmt.GetOncommit() != pgq.OnCommitAction_ONCOMMIT_NOOP:
		return false
	// Setting a tablespace is not supported
	case stmt.GetTablespacename() != "":
		return false
	// CREATE TABLE OF type_name is not supported
	case stmt.GetOfTypename() != nil:
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
	// Column storage options are not supported
	case col.GetStorageName() != "":
		return false
		// Column compression options are not supported
	case col.GetCompression() != "":
		return false
	default:
		return true
	}
}
