// SPDX-License-Identifier: Apache-2.0

package sql2pgroll

import (
	"fmt"
	"strings"

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
	// - Table constraints (not supported)
	// - LIKE clauses (not supported)
	var columns []migrations.Column
	for _, elt := range stmt.TableElts {
		switch elt.Node.(type) {
		case *pgq.Node_ColumnDef:
			column, err := convertColumnDef(stmt.Relation.GetRelname(), elt.GetColumnDef())
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

	var constraints []migrations.Constraint
	for _, c := range stmt.Constraints {
		constraint, err := convertConstraint(c.GetConstraint())
		if err != nil {
			return nil, fmt.Errorf("error converting table constraint: %w", err)
		}
		constraints = append(constraints, *constraint)
	}

	return migrations.Operations{
		&migrations.OpCreateTable{
			Name:        getQualifiedRelationName(stmt.GetRelation()),
			Columns:     columns,
			Constraints: constraints,
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

func convertColumnDef(tableName string, col *pgq.ColumnDef) (*migrations.Column, error) {
	if !canConvertColumnDef(col) {
		return nil, nil
	}

	// Deparse the column type
	typeString, err := pgq.DeparseTypeName(col.TypeName)
	if err != nil {
		return nil, fmt.Errorf("error deparsing column type: %w", err)
	}

	// Convert column constraints
	var notNull, pk, unique bool
	var check *migrations.CheckConstraint
	var foreignKey *migrations.ForeignKeyReference
	var defaultValue *string
	for _, c := range col.GetConstraints() {
		switch c.GetConstraint().GetContype() {
		case pgq.ConstrType_CONSTR_NULL:
			// named NULL constraints are not supported
			if isConstraintNamed(c.GetConstraint()) {
				return nil, nil
			}
			notNull = false
		case pgq.ConstrType_CONSTR_NOTNULL:
			// named NOT NULL constraints are not supported
			if isConstraintNamed(c.GetConstraint()) {
				return nil, nil
			}
			notNull = true
		case pgq.ConstrType_CONSTR_UNIQUE:
			// named UNIQUE constraints are not supported
			if isConstraintNamed(c.GetConstraint()) {
				return nil, nil
			}
			if !canConvertUniqueConstraint(c.GetConstraint()) {
				return nil, nil
			}
			unique = true
		case pgq.ConstrType_CONSTR_PRIMARY:
			// named PRIMARY KEY constraints are not supported
			if isConstraintNamed(c.GetConstraint()) {
				return nil, nil
			}
			if !canConvertPrimaryKeyConstraint(c.GetConstraint()) {
				return nil, nil
			}
			pk = true
			notNull = true
		case pgq.ConstrType_CONSTR_CHECK:
			check, err = convertInlineCheckConstraint(tableName, col.GetColname(), c.GetConstraint())
			if err != nil {
				return nil, fmt.Errorf("error converting inline check constraint: %w", err)
			}
			if check == nil {
				return nil, nil
			}
		case pgq.ConstrType_CONSTR_DEFAULT:
			// named DEFAULT constraints are not supported
			if isConstraintNamed(c.GetConstraint()) {
				return nil, nil
			}
			d, err := extractDefault(c.GetConstraint().GetRawExpr())
			if err != nil {
				return nil, fmt.Errorf("error deparsing default value: %w", err)
			}
			if !d.IsNull() {
				v := d.MustGet()
				defaultValue = &v
			}
		case pgq.ConstrType_CONSTR_FOREIGN:
			foreignKey, err = convertInlineForeignKeyConstraint(tableName, col.GetColname(), c.GetConstraint())
			if err != nil {
				return nil, fmt.Errorf("error converting inline foreign key constraint: %w", err)
			}
			if foreignKey == nil {
				return nil, nil
			}
		case
			pgq.ConstrType_CONSTR_ATTR_NOT_DEFERRABLE,
			pgq.ConstrType_CONSTR_ATTR_IMMEDIATE:
			// NOT DEFERRABLE and INITIALLY IMMEDIATE constraints are the default and
			// are supported, but no extra annotation is needed
			continue
		case pgq.ConstrType_CONSTR_GENERATED:
			// Generated columns are not supported
			return nil, nil
		case pgq.ConstrType_CONSTR_IDENTITY:
			// Identity columns are not supported
			return nil, nil
		case pgq.ConstrType_CONSTR_ATTR_DEFERRABLE:
			// Deferrable constraints are not supported
			return nil, nil
		case pgq.ConstrType_CONSTR_ATTR_DEFERRED:
			// Initially deferred deferred constraints are not supported
			return nil, nil
		default:
			// Any other type of constraint is not supported
			return nil, nil
		}
	}

	return &migrations.Column{
		Name:       col.GetColname(),
		Type:       typeString,
		Nullable:   !notNull,
		Pk:         pk,
		Check:      check,
		References: foreignKey,
		Default:    defaultValue,
		Unique:     unique,
	}, nil
}

func convertConstraint(c *pgq.Constraint) (*migrations.Constraint, error) {
	var constraintType migrations.ConstraintType
	var nullsNotDistinct *bool
	var exclude *migrations.ConstraintExclude

	switch c.Contype {
	case pgq.ConstrType_CONSTR_UNIQUE:
		constraintType = migrations.ConstraintTypeUnique
		nullsNotDistinct = ptr(c.NullsNotDistinct)
	case pgq.ConstrType_CONSTR_EXCLUSION:
		exclusionElemens := make([]string, len(c.Exclusions))
		for i, ex := range c.Exclusions {
			if len(ex.GetList().Items) != 2 {
				return nil, fmt.Errorf("unexpected number of elements in exclusion constraint: %d", len(ex.GetList().Items))
			}
			colName := ex.GetList().Items[0].GetIndexElem().Name
			opName := ex.GetList().Items[1].GetString_().Sval
			exclusionElemens[i] = fmt.Sprintf("%s WITH %s", colName, opName)
		}
		exclude = &migrations.ConstraintExclude{
			Elements:    strings.Join(exclusionElemens, ", "),
			IndexMethod: c.AccessMethod,
		}
	case pgq.ConstrType_CONSTR_PRIMARY:
		constraintType = migrations.ConstraintTypePrimaryKey
	case pgq.ConstrType_CONSTR_FOREIGN:
		constraintType = migrations.ConstraintTypeForeignKey
	case pgq.ConstrType_CONSTR_CHECK:
		constraintType = migrations.ConstraintTypeCheck
	default:
		return nil, fmt.Errorf("unsupported constraint type: %s", c.Contype)
	}

	including := make([]string, len(c.Including))
	for i, include := range c.Including {
		including[i] = include.GetString_().Sval
	}

	options := make([]string, len(c.Options))
	for i, option := range c.Options {
		var val string
		switch v := option.GetDefElem().Arg.GetNode().(type) {
		case *pgq.Node_Float:
			val = v.Float.GetFval()
		case *pgq.Node_Integer:
			val = fmt.Sprintf("%d", v.Integer.GetIval())
		case *pgq.Node_String_:
			val = v.String_.GetSval()
		case *pgq.Node_Boolean:
			val = v.Boolean.String()
		default:
			return nil, fmt.Errorf("unsupported storage parameter type: %T", v)
		}
		options[i] = fmt.Sprintf("%s = '%s'", option.GetDefElem().Defname, val)
	}
	var storageParameters *string
	if len(options) != 0 {
		storageParameters = ptr(strings.Join(options, ", "))
	}

	var tablespace *string
	if c.Indexspace != "" {
		tablespace = ptr(c.Indexspace)
	}

	return &migrations.Constraint{
		Name:              c.Conname,
		Type:              constraintType,
		NullsNotDistinct:  nullsNotDistinct,
		Deferable:         ptr(c.Deferrable),
		InitiallyDeferred: ptr(c.Initdeferred),
		NoInherit:         ptr(c.IsNoInherit),
		StorageParameters: storageParameters,
		Tablespace:        tablespace,
		IncludeColumns:    including,
		Exclude:           exclude,
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

// canConvertPrimaryKeyConstraint returns true iff `constraint` can be converted
// to a pgroll primary key constraint.
func canConvertPrimaryKeyConstraint(constraint *pgq.Constraint) bool {
	switch {
	case
		// Specifying an index tablespace is not supported
		constraint.GetIndexspace() != "",
		// Storage options are not supported
		len(constraint.GetOptions()) != 0:
		return false
	default:
		return true
	}
}

// convertInlineCheckConstraint converts an inline check constraint to a
// `CheckConstraint`.
func convertInlineCheckConstraint(tableName, columnName string, constraint *pgq.Constraint) (*migrations.CheckConstraint, error) {
	if !canConvertCheckConstraint(constraint) {
		return nil, nil
	}

	expr, err := pgq.DeparseExpr(constraint.GetRawExpr())
	if err != nil {
		return nil, fmt.Errorf("failed to deparse CHECK expression: %w", err)
	}

	name := fmt.Sprintf("%s_%s_check", tableName, columnName)
	if constraint.GetConname() != "" {
		name = constraint.GetConname()
	}

	return &migrations.CheckConstraint{
		Name:       name,
		Constraint: expr,
	}, nil
}

// convertInlineForeignKeyConstraint converts an inline foreign key constraint
// to a `ForeignKeyReference`.
func convertInlineForeignKeyConstraint(tableName, columnName string, constraint *pgq.Constraint) (*migrations.ForeignKeyReference, error) {
	if !canConvertForeignKeyConstraint(constraint) {
		return nil, nil
	}

	onDelete, err := parseOnDeleteAction(constraint.GetFkDelAction())
	if err != nil {
		return nil, err
	}

	name := fmt.Sprintf("%s_%s_fkey", tableName, columnName)
	if constraint.GetConname() != "" {
		name = constraint.GetConname()
	}

	return &migrations.ForeignKeyReference{
		Name:     name,
		OnDelete: onDelete,
		Column:   constraint.GetPkAttrs()[0].GetString_().GetSval(),
		Table:    getQualifiedRelationName(constraint.GetPktable()),
	}, nil
}

// isConstraintNamed returns true iff `constraint` has a name.
// Column constraints defined inline in CREATE TABLE statements can be either
// named or unnamed, for example:
// - CREATE TABLE t (a INT PRIMARY KEY);
// - CREATE TABLE t (a INT CONSTRAINT my_pk PRIMARY KEY);
// Likewise, table constraints can also be either named or unnamed, for example:
// - CREATE TABLE foo(a int, CONSTRAINT foo_check CHECK (a > 0)),
// - CREATE TABLE foo(a int, CHECK (a > 0)),
func isConstraintNamed(constraint *pgq.Constraint) bool {
	return constraint.GetConname() != ""
}
