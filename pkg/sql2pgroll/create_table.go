// SPDX-License-Identifier: Apache-2.0

package sql2pgroll

import (
	"fmt"
	"strings"

	pgq "github.com/xataio/pg_query_go/v6"

	"github.com/xataio/pgroll/pkg/migrations"
)

var referentialAction = map[string]migrations.ForeignKeyAction{
	"a": migrations.ForeignKeyActionNOACTION,
	"c": migrations.ForeignKeyActionCASCADE,
	"d": migrations.ForeignKeyActionSETDEFAULT,
	"n": migrations.ForeignKeyActionSETNULL,
	"r": migrations.ForeignKeyActionRESTRICT,
}

var matchTypes = map[string]migrations.ForeignKeyMatchType{
	"s": migrations.ForeignKeyMatchTypeSIMPLE,
	"p": migrations.ForeignKeyMatchTypePARTIAL,
	"f": migrations.ForeignKeyMatchTypeFULL,
}

// convertCreateStmt converts a CREATE TABLE statement to a pgroll operation.
func convertCreateStmt(stmt *pgq.CreateStmt) (migrations.Operations, error) {
	// Check if the statement can be converted
	if !canConvertCreateStatement(stmt) {
		return nil, nil
	}

	// Convert the table elements - table elements can be:
	// - Column definitions
	// - Table constraints
	// - LIKE clauses (not supported)
	var columns []migrations.Column
	var constraints []migrations.Constraint
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
		case *pgq.Node_Constraint:
			constraint, err := convertConstraint(elt.GetConstraint())
			if err != nil {
				return nil, fmt.Errorf("error converting table constraint: %w", err)
			}
			if constraint == nil {
				return nil, nil
			}
			constraints = append(constraints, *constraint)
		default:
			return nil, nil
		}
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
	var generated *migrations.ColumnGenerated
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
			if c.GetConstraint().GetRawExpr() != nil {
				generatorExpr, err := pgq.DeparseExpr(c.GetConstraint().GetRawExpr())
				if err != nil {
					return nil, fmt.Errorf("deparsing generated expression: %w", err)
				}
				generated = &migrations.ColumnGenerated{
					Expression: generatorExpr,
				}
			} else {
				return nil, nil
			}
			notNull = true
		case pgq.ConstrType_CONSTR_IDENTITY:
			var when migrations.ColumnGeneratedIdentityUserSpecifiedValues
			switch c.GetConstraint().GeneratedWhen {
			case "a":
				when = migrations.ColumnGeneratedIdentityUserSpecifiedValuesALWAYS
			case "d":
				when = migrations.ColumnGeneratedIdentityUserSpecifiedValuesBYDEFAULT
			default:
				return nil, nil
			}
			sequenceOptions := ""
			if c.GetConstraint().GetOptions() != nil {
				sequenceOptions, err = pgq.DeparseParenthesizedSeqOptList(c.GetConstraint().GetOptions())
				if err != nil {
					return nil, fmt.Errorf("parsing sequence options: %w", err)
				}
				sequenceOptions = sequenceOptions[1 : len(sequenceOptions)-1]
			}
			generated = &migrations.ColumnGenerated{
				Identity: &migrations.ColumnGeneratedIdentity{
					UserSpecifiedValues: when,
					SequenceOptions:     sequenceOptions,
				},
			}
			notNull = true
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
		Generated:  generated,
	}, nil
}

func convertConstraint(c *pgq.Constraint) (*migrations.Constraint, error) {
	var constraintType migrations.ConstraintType
	var nullsNotDistinct bool
	var checkExpr string
	var err error
	var references *migrations.TableForeignKeyReference
	var exclude *migrations.ConstraintExclude

	columns := make([]string, len(c.Keys))
	for i, key := range c.Keys {
		columns[i] = key.GetString_().Sval
	}

	switch c.Contype {
	case pgq.ConstrType_CONSTR_UNIQUE:
		constraintType = migrations.ConstraintTypeUnique
		nullsNotDistinct = c.NullsNotDistinct
	case pgq.ConstrType_CONSTR_CHECK:
		constraintType = migrations.ConstraintTypeCheck
		checkExpr, err = pgq.DeparseExpr(c.GetRawExpr())
		if err != nil {
			return nil, fmt.Errorf("deparsing check expression: %w", err)
		}
	case pgq.ConstrType_CONSTR_PRIMARY:
		constraintType = migrations.ConstraintTypePrimaryKey
	case pgq.ConstrType_CONSTR_FOREIGN:
		constraintType = migrations.ConstraintTypeForeignKey
		columns, references = convertFkConstraint(c)
	case pgq.ConstrType_CONSTR_EXCLUSION:
		constraintType = migrations.ConstraintTypeExclude
		exclude = &migrations.ConstraintExclude{
			IndexMethod: c.AccessMethod,
		}
		if c.GetWhereClause() != nil {
			whereClause, err := pgq.DeparseExpr(c.GetWhereClause())
			if err != nil {
				return nil, nil
			}
			exclude.Predicate = whereClause
		}
		exclusionElements := make([]string, len(c.Exclusions))
		for i, elem := range c.Exclusions {
			if elem.GetList() == nil && len(elem.GetList().Items) != 2 {
				return nil, nil
			}
			indexElem, err := pgq.DeparseIndexElem(elem.GetList().Items[0])
			if err != nil {
				return nil, nil
			}
			anyOp, err := pgq.DeparseAnyOperator(elem.GetList().Items[1].GetList().Items)
			if err != nil {
				return nil, nil
			}
			exclusionElements[i] = fmt.Sprintf("%s WITH %s", indexElem, anyOp)
		}
		exclude.Elements = strings.Join(exclusionElements, ", ")
	default:
		return nil, nil
	}

	including := make([]string, len(c.Including))
	for i, include := range c.Including {
		including[i] = include.GetString_().Sval
	}

	var storageParams string
	if len(c.GetOptions()) > 0 {
		storageParams, err = pgq.DeparseRelOptions(c.GetOptions())
		if err != nil {
			return nil, fmt.Errorf("parsing options: %w", err)
		}
		storageParams = storageParams[1 : len(storageParams)-1]
	}

	var indexParameters *migrations.ConstraintIndexParameters
	if storageParams != "" || c.Indexspace != "" || len(including) != 0 {
		indexParameters = &migrations.ConstraintIndexParameters{
			StorageParameters: storageParams,
			Tablespace:        c.Indexspace,
			IncludeColumns:    including,
		}
	}

	return &migrations.Constraint{
		Name:              c.Conname,
		Type:              constraintType,
		Columns:           columns,
		NullsNotDistinct:  nullsNotDistinct,
		NoInherit:         c.IsNoInherit,
		Deferrable:        c.Deferrable,
		InitiallyDeferred: c.Initdeferred,
		IndexParameters:   indexParameters,
		Check:             checkExpr,
		References:        references,
		Exclude:           exclude,
	}, nil
}

// convertFkConstraint converts a foreign key constraint to a table level foreign key constraint.
func convertFkConstraint(c *pgq.Constraint) ([]string, *migrations.TableForeignKeyReference) {
	referencedTable := getQualifiedRelationName(c.GetPktable())
	referencedColumns := make([]string, len(c.PkAttrs))
	for i, node := range c.PkAttrs {
		referencedColumns[i] = node.GetString_().Sval
	}
	matchType := migrations.ForeignKeyMatchTypeSIMPLE
	if c.GetFkMatchtype() != "" {
		matchType = matchTypes[c.FkMatchtype]
	}
	var columnsToSet []string
	onDelete := migrations.ForeignKeyActionNOACTION
	if c.GetFkDelAction() != "" {
		onDelete = referentialAction[c.GetFkDelAction()]
		if c.GetFkDelSetCols() != nil {
			columnsToSet = make([]string, len(c.FkDelSetCols))
			for i, node := range c.FkDelSetCols {
				columnsToSet[i] = node.GetString_().Sval
			}
		}
	}
	onUpdate := migrations.ForeignKeyActionNOACTION
	if c.GetFkUpdAction() != "" {
		onUpdate = referentialAction[c.GetFkUpdAction()]
	}
	var columns []string
	if c.GetFkAttrs() != nil {
		columns = make([]string, len(c.GetFkAttrs()))
		for i, node := range c.GetFkAttrs() {
			columns[i] = node.GetString_().Sval
		}
	}

	return columns, &migrations.TableForeignKeyReference{
		Table:              referencedTable,
		Columns:            referencedColumns,
		MatchType:          matchType,
		OnDelete:           onDelete,
		OnDeleteSetColumns: columnsToSet,
		OnUpdate:           onUpdate,
	}
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
		NoInherit:  constraint.GetIsNoInherit(),
	}, nil
}

// convertInlineForeignKeyConstraint converts an inline foreign key constraint
// to a `ForeignKeyReference`.
func convertInlineForeignKeyConstraint(tableName, columnName string, constraint *pgq.Constraint) (*migrations.ForeignKeyReference, error) {
	if !canConvertForeignKeyConstraint(constraint) {
		return nil, nil
	}

	onDelete := migrations.ForeignKeyActionNOACTION
	if constraint.GetFkDelAction() != "" {
		onDelete = referentialAction[constraint.FkDelAction]
	}

	onUpdate := migrations.ForeignKeyActionNOACTION
	if constraint.GetFkUpdAction() != "" {
		onUpdate = referentialAction[constraint.FkUpdAction]
	}

	matchType := migrations.ForeignKeyMatchTypeSIMPLE
	if constraint.GetFkMatchtype() != "" {
		matchType = matchTypes[constraint.FkMatchtype]
	}

	name := fmt.Sprintf("%s_%s_fkey", tableName, columnName)
	if constraint.GetConname() != "" {
		name = constraint.GetConname()
	}

	return &migrations.ForeignKeyReference{
		Name:              name,
		OnDelete:          onDelete,
		OnUpdate:          onUpdate,
		MatchType:         matchType,
		Column:            constraint.GetPkAttrs()[0].GetString_().GetSval(),
		Table:             getQualifiedRelationName(constraint.GetPktable()),
		Deferrable:        constraint.GetDeferrable(),
		InitiallyDeferred: constraint.GetInitdeferred(),
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
