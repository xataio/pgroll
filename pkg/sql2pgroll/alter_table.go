// SPDX-License-Identifier: Apache-2.0

package sql2pgroll

import (
	"fmt"

	pgq "github.com/pganalyze/pg_query_go/v6"

	"github.com/xataio/pgroll/pkg/migrations"
)

const PlaceHolderSQL = "TODO: Implement SQL data migration"

// convertAlterTableStmt converts an ALTER TABLE statement to pgroll operations.
func convertAlterTableStmt(stmt *pgq.AlterTableStmt) (migrations.Operations, error) {
	if stmt.Objtype != pgq.ObjectType_OBJECT_TABLE {
		return nil, nil
	}

	var ops migrations.Operations
	for _, cmd := range stmt.Cmds {
		alterTableCmd := cmd.GetAlterTableCmd()
		if alterTableCmd == nil {
			continue
		}

		var op migrations.Operation
		var err error
		switch alterTableCmd.GetSubtype() {
		case pgq.AlterTableType_AT_SetNotNull:
			op, err = convertAlterTableSetNotNull(stmt, alterTableCmd, true)
		case pgq.AlterTableType_AT_DropNotNull:
			op, err = convertAlterTableSetNotNull(stmt, alterTableCmd, false)
		case pgq.AlterTableType_AT_AlterColumnType:
			op, err = convertAlterTableAlterColumnType(stmt, alterTableCmd)
		case pgq.AlterTableType_AT_AddConstraint:
			op, err = convertAlterTableAddConstraint(stmt, alterTableCmd)
		case pgq.AlterTableType_AT_DropColumn:
			op, err = convertAlterTableDropColumn(stmt, alterTableCmd)
		}

		if err != nil {
			return nil, err
		}

		if op == nil {
			return nil, nil
		}

		ops = append(ops, op)
	}

	return ops, nil
}

// convertAlterTableSetNotNull converts SQL statements like:
//
// `ALTER TABLE foo ALTER COLUMN a SET NOT NULL`
// `ALTER TABLE foo ALTER COLUMN a DROP NOT NULL`
//
// to an OpAlterColumn operation.
func convertAlterTableSetNotNull(stmt *pgq.AlterTableStmt, cmd *pgq.AlterTableCmd, notNull bool) (migrations.Operation, error) {
	return &migrations.OpAlterColumn{
		Table:    stmt.GetRelation().GetRelname(),
		Column:   cmd.GetName(),
		Nullable: ptr(!notNull),
		Up:       PlaceHolderSQL,
		Down:     PlaceHolderSQL,
	}, nil
}

// convertAlterTableAlterColumnType converts a SQL statement like:
//
// `ALTER TABLE foo ALTER COLUMN a SET DATA TYPE text`
//
// to an OpAlterColumn operation.
func convertAlterTableAlterColumnType(stmt *pgq.AlterTableStmt, cmd *pgq.AlterTableCmd) (migrations.Operation, error) {
	node, ok := cmd.GetDef().Node.(*pgq.Node_ColumnDef)
	if !ok {
		return nil, fmt.Errorf("expected column definition, got %T", cmd.GetDef().Node)
	}

	if !canConvertColumnForSetDataType(node.ColumnDef) {
		return nil, nil
	}

	return &migrations.OpAlterColumn{
		Table:  stmt.GetRelation().GetRelname(),
		Column: cmd.GetName(),
		Type:   ptr(convertTypeName(node.ColumnDef.GetTypeName())),
		Up:     PlaceHolderSQL,
		Down:   PlaceHolderSQL,
	}, nil
}

// convertAlterTableAddConstraint converts SQL statements like:
//
// `ALTER TABLE foo ADD CONSTRAINT bar UNIQUE (a)`
//
// To an OpCreateConstraint operation.
func convertAlterTableAddConstraint(stmt *pgq.AlterTableStmt, cmd *pgq.AlterTableCmd) (migrations.Operation, error) {
	node, ok := cmd.GetDef().Node.(*pgq.Node_Constraint)
	if !ok {
		return nil, fmt.Errorf("expected constraint definition, got %T", cmd.GetDef().Node)
	}

	var op migrations.Operation
	var err error
	switch node.Constraint.GetContype() {
	case pgq.ConstrType_CONSTR_UNIQUE:
		op, err = convertAlterTableAddUniqueConstraint(stmt, node.Constraint)
	default:
		return nil, nil
	}

	if err != nil {
		return nil, err
	}

	return op, nil
}

// convertAlterTableAddUniqueConstraint converts SQL statements like:
//
// `ALTER TABLE foo ADD CONSTRAINT bar UNIQUE (a)`
//
// to an OpCreateConstraint operation.
func convertAlterTableAddUniqueConstraint(stmt *pgq.AlterTableStmt, constraint *pgq.Constraint) (migrations.Operation, error) {
	if !canConvertUniqueConstraint(constraint) {
		return nil, nil
	}

	// Extract the columns covered by the unique constraint
	columns := make([]string, 0, len(constraint.GetKeys()))
	for _, keyNode := range constraint.GetKeys() {
		key, ok := keyNode.Node.(*pgq.Node_String_)
		if !ok {
			return nil, fmt.Errorf("expected string key, got %T", keyNode)
		}
		columns = append(columns, key.String_.GetSval())
	}

	// Build the up and down SQL placeholders for each column covered by the
	// constraint
	upDown := make(map[string]string, len(columns))
	for _, column := range columns {
		upDown[column] = PlaceHolderSQL
	}

	return &migrations.OpCreateConstraint{
		Type:    migrations.OpCreateConstraintTypeUnique,
		Name:    constraint.GetConname(),
		Table:   stmt.GetRelation().GetRelname(),
		Columns: columns,
		Down:    upDown,
		Up:      upDown,
	}, nil
}

// convertAlterTableDropColumn converts SQL statements like:
//
// `ALTER TABLE foo DROP COLUMN bar
//
// to an OpDropColumn operation.
func convertAlterTableDropColumn(stmt *pgq.AlterTableStmt, cmd *pgq.AlterTableCmd) (migrations.Operation, error) {
	switch cmd.Behavior {
	case pgq.DropBehavior_DROP_RESTRICT, pgq.DropBehavior_DROP_BEHAVIOR_UNDEFINED:
		// Supported
	case pgq.DropBehavior_DROP_CASCADE:
		// Fall back to SQL
		return nil, nil
	}

	// IF EXISTS not supported
	if cmd.MissingOk {
		return nil, nil
	}

	return &migrations.OpDropColumn{
		Table:  stmt.GetRelation().GetRelname(),
		Column: cmd.GetName(),
		Down:   PlaceHolderSQL,
	}, nil
}

// canConvertUniqueConstraint checks if the unique constraint `constraint` can
// be faithfully converted to an OpCreateConstraint operation without losing
// information.
func canConvertUniqueConstraint(constraint *pgq.Constraint) bool {
	if constraint.GetNullsNotDistinct() {
		return false
	}
	if len(constraint.GetIncluding()) > 0 {
		return false
	}
	if len(constraint.GetOptions()) > 0 {
		return false
	}
	if constraint.GetIndexspace() != "" {
		return false
	}
	return true
}

// canConvertColumnForSetDataType checks if `column` can be faithfully
// converted as part of an OpAlterColumn operation to set a new type for the
// column.
func canConvertColumnForSetDataType(column *pgq.ColumnDef) bool {
	if column.GetCollClause() != nil {
		return false
	}
	if column.GetRawDefault() != nil {
		return false
	}
	return true
}

func ptr[T any](x T) *T {
	return &x
}
