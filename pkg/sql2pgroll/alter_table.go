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

func convertAlterTableSetNotNull(stmt *pgq.AlterTableStmt, cmd *pgq.AlterTableCmd, notNull bool) (migrations.Operation, error) {
	return &migrations.OpAlterColumn{
		Table:    stmt.GetRelation().GetRelname(),
		Column:   cmd.GetName(),
		Nullable: ptr(!notNull),
		Up:       PlaceHolderSQL,
		Down:     PlaceHolderSQL,
	}, nil
}

func convertAlterTableAlterColumnType(stmt *pgq.AlterTableStmt, cmd *pgq.AlterTableCmd) (migrations.Operation, error) {
	node, ok := cmd.GetDef().Node.(*pgq.Node_ColumnDef)
	if !ok {
		return nil, fmt.Errorf("expected column definition, got %T", cmd.GetDef().Node)
	}

	return &migrations.OpAlterColumn{
		Table:  stmt.GetRelation().GetRelname(),
		Column: cmd.GetName(),
		Type:   ptr(convertTypeName(node.ColumnDef.GetTypeName())),
		Up:     PlaceHolderSQL,
		Down:   PlaceHolderSQL,
	}, nil
}

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

func convertAlterTableAddUniqueConstraint(stmt *pgq.AlterTableStmt, constraint *pgq.Constraint) (migrations.Operation, error) {
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

func ptr[T any](x T) *T {
	return &x
}
