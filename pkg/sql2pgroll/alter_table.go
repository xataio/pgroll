// SPDX-License-Identifier: Apache-2.0

package sql2pgroll

import (
	"fmt"
	"strconv"

	"github.com/oapi-codegen/nullable"
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
		case pgq.AlterTableType_AT_ColumnDefault:
			op, err = convertAlterTableSetColumnDefault(stmt, alterTableCmd)
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
	case pgq.ConstrType_CONSTR_FOREIGN:
		op, err = convertAlterTableAddForeignKeyConstraint(stmt, node.Constraint)
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

func convertAlterTableAddForeignKeyConstraint(stmt *pgq.AlterTableStmt, constraint *pgq.Constraint) (migrations.Operation, error) {
	if !canConvertAlterTableAddForeignKeyConstraint(constraint) {
		return nil, nil
	}

	columns := make([]string, len(constraint.GetFkAttrs()))
	for i := range columns {
		columns[i] = constraint.GetFkAttrs()[i].GetString_().GetSval()
	}

	foreignColumns := make([]string, len(constraint.GetPkAttrs()))
	for i := range columns {
		foreignColumns[i] = constraint.GetPkAttrs()[i].GetString_().GetSval()
	}

	migs := make(map[string]string)
	for _, column := range columns {
		migs[column] = PlaceHolderSQL
	}

	var onDelete migrations.ForeignKeyReferenceOnDelete
	switch constraint.GetFkDelAction() {
	case "a":
		onDelete = migrations.ForeignKeyReferenceOnDeleteNOACTION
	case "c":
		onDelete = migrations.ForeignKeyReferenceOnDeleteCASCADE
	case "r":
		onDelete = migrations.ForeignKeyReferenceOnDeleteRESTRICT
	case "d":
		onDelete = migrations.ForeignKeyReferenceOnDeleteSETDEFAULT
	case "n":
		onDelete = migrations.ForeignKeyReferenceOnDeleteSETNULL
	default:
		return nil, fmt.Errorf("unknown delete action: %q", constraint.GetFkDelAction())
	}

	tableName := stmt.GetRelation().GetRelname()
	if stmt.GetRelation().GetSchemaname() != "" {
		tableName = stmt.GetRelation().GetSchemaname() + "." + tableName
	}

	foreignTable := constraint.GetPktable().GetRelname()
	if constraint.GetPktable().GetSchemaname() != "" {
		foreignTable = constraint.GetPktable().GetSchemaname() + "." + foreignTable
	}

	return &migrations.OpCreateConstraint{
		Columns: columns,
		Up:      migs,
		Down:    migs,
		Name:    constraint.GetConname(),
		References: &migrations.OpCreateConstraintReferences{
			Columns:  foreignColumns,
			OnDelete: onDelete,
			Table:    foreignTable,
		},
		Table: tableName,
		Type:  migrations.OpCreateConstraintTypeForeignKey,
	}, nil
}

func canConvertAlterTableAddForeignKeyConstraint(constraint *pgq.Constraint) bool {
	switch constraint.GetFkUpdAction() {
	case "r", "c", "n", "d":
		// RESTRICT, CASCADE, SET NULL, SET DEFAULT
		return false
	case "a":
		// NO ACTION, the default
		break
	}
	switch constraint.GetFkMatchtype() {
	case "f":
		// FULL
		return false
	case "s":
		// SIMPLE, the default
		break
	}
	return true
}

// convertAlterTableSetColumnDefault converts SQL statements like:
//
// `ALTER TABLE foo COLUMN bar SET DEFAULT 'foo'`
// `ALTER TABLE foo COLUMN bar SET DEFAULT 123`
// `ALTER TABLE foo COLUMN bar SET DEFAULT 123.456`
// `ALTER TABLE foo COLUMN bar SET DEFAULT true`
// `ALTER TABLE foo COLUMN bar SET DEFAULT B'0101'`
// `ALTER TABLE foo COLUMN bar SET DEFAULT null`
// `ALTER TABLE foo COLUMN bar DROP DEFAULT`
//
// to an OpAlterColumn operation.
func convertAlterTableSetColumnDefault(stmt *pgq.AlterTableStmt, cmd *pgq.AlterTableCmd) (migrations.Operation, error) {
	operation := &migrations.OpAlterColumn{
		Table:  stmt.GetRelation().GetRelname(),
		Column: cmd.GetName(),
		Down:   PlaceHolderSQL,
		Up:     PlaceHolderSQL,
	}

	if c := cmd.GetDef().GetAConst(); c != nil {
		if c.GetIsnull() {
			// The default can be set to null
			operation.Default = nullable.NewNullNullable[string]()
			return operation, nil
		}

		// We have a constant
		switch v := c.GetVal().(type) {
		case *pgq.A_Const_Sval:
			operation.Default = nullable.NewNullableWithValue(v.Sval.GetSval())
		case *pgq.A_Const_Ival:
			operation.Default = nullable.NewNullableWithValue(strconv.FormatInt(int64(v.Ival.Ival), 10))
		case *pgq.A_Const_Fval:
			operation.Default = nullable.NewNullableWithValue(v.Fval.Fval)
		case *pgq.A_Const_Boolval:
			operation.Default = nullable.NewNullableWithValue(strconv.FormatBool(v.Boolval.Boolval))
		case *pgq.A_Const_Bsval:
			operation.Default = nullable.NewNullableWithValue(v.Bsval.Bsval)
		default:
			return nil, fmt.Errorf("unknown constant type: %T", c.GetVal())
		}

		return operation, nil
	}

	if cmd.GetDef() != nil {
		// We're setting it to something other than a constant
		return nil, nil
	}

	// We're not setting it to anything, which is the case when we are dropping it
	if cmd.GetBehavior() == pgq.DropBehavior_DROP_RESTRICT {
		operation.Default = nullable.NewNullNullable[string]()
		return operation, nil
	}

	// Unknown case, fall back to raw SQL
	return nil, nil
}

func convertAlterTableDropColumn(stmt *pgq.AlterTableStmt, cmd *pgq.AlterTableCmd) (migrations.Operation, error) {
	if !canConvertDropColumn(cmd) {
		return nil, nil
	}

	return &migrations.OpDropColumn{
		Table:  stmt.GetRelation().GetRelname(),
		Column: cmd.GetName(),
		Down:   PlaceHolderSQL,
	}, nil
}

// canConvertDropColumn checks whether we can convert the command without losing any information.
func canConvertDropColumn(cmd *pgq.AlterTableCmd) bool {
	if cmd.MissingOk {
		return false
	}
	if cmd.Behavior == pgq.DropBehavior_DROP_CASCADE {
		return false
	}
	return true
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
