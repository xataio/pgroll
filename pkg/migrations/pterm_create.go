// SPDX-License-Identifier: Apache-2.0

package migrations

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/pterm/pterm"
)

func (o *OpCreateTable) Create() {
	o.Name, _ = pterm.DefaultInteractiveTextInput.WithDefaultText("table").Show()

	addColumns, _ := pterm.DefaultInteractiveConfirm.
		WithDefaultText("Add columns").
		Show()
	for addColumns {
		o.Columns = append(o.Columns, getColumnFromCLI())

		addColumns, _ = pterm.DefaultInteractiveConfirm.
			WithDefaultText("Add more columns").
			Show()
	}

	addConstraints, _ := pterm.DefaultInteractiveConfirm.
		WithDefaultText("Add constraints").
		Show()
	for addConstraints {
		var c Constraint
		c.Name, _ = pterm.DefaultInteractiveTextInput.WithDefaultText("name").Show()
		columns, _ := pterm.DefaultInteractiveTextInput.WithDefaultText("columns").Show()
		c.Columns = strings.Split(columns, ",")
		constraintType, _ := pterm.DefaultInteractiveSelect.
			WithDefaultText("type").
			WithOptions([]string{"unique", "primary_key", "foreign_key", "check", "exclude"}).
			Show()
		c.Type = ConstraintType(constraintType)
		switch c.Type {
		case ConstraintTypeCheck:
			c.Check, _ = pterm.DefaultInteractiveTextInput.WithDefaultText("check").Show()
			c.NoInherit = getBooleanOptionForColumnAttr("no_inherit")
		case ConstraintTypeUnique:
			c.NullsNotDistinct = getBooleanOptionForColumnAttr("null_not_distinct")
		case ConstraintTypeForeignKey:
			var reference TableForeignKeyReference
			reference.Table, _ = pterm.DefaultInteractiveTextInput.WithDefaultText("references.table").Show()
			referencedColumns, _ := pterm.DefaultInteractiveTextInput.WithDefaultText("references.columns").Show()
			reference.Columns = strings.Split(referencedColumns, ",")
			c.References = &reference
		}
		o.Constraints = append(o.Constraints, c)

		addConstraints, _ = pterm.DefaultInteractiveConfirm.
			WithDefaultText("Add more constraints").
			Show()
	}

	comment, _ := pterm.DefaultInteractiveTextInput.WithDefaultText("comment").Show()
	if comment != "" {
		o.Comment = &comment
	}
}

func (o *OpCreateIndex) Create() {
	o.Name, _ = pterm.DefaultInteractiveTextInput.WithDefaultText("name").Show()
	o.Table, _ = pterm.DefaultInteractiveTextInput.WithDefaultText("table").Show()
	addColumns, _ := pterm.DefaultInteractiveConfirm.
		WithDefaultText("Add columns").
		WithDefaultValue(true).
		Show()
	columns := make([]IndexField, 0)
	for addColumns {
		var indexField IndexField
		indexField.Column, _ = pterm.DefaultInteractiveTextInput.WithDefaultText("name").Show()
		indexField.Collate, _ = pterm.DefaultInteractiveTextInput.WithDefaultText("collate").Show()
		nulls, _ := pterm.DefaultInteractiveSelect.
			WithDefaultText("null").
			WithOptions([]string{"", "FIRST", "LAST"}).
			WithDefaultOption("").
			Show()
		if nulls != "" {
			n := IndexFieldNulls(nulls)
			indexField.Nulls = &n
		}
		sort, _ := pterm.DefaultInteractiveSelect.
			WithDefaultText("sort").
			WithOptions([]string{"", "ASC", "DESC"}).
			WithDefaultOption("").
			Show()
		if sort != "" {
			indexField.Sort = IndexFieldSort(sort)
		}
		addOpclass, _ := pterm.DefaultInteractiveConfirm.
			WithDefaultText("Add opclass").
			WithDefaultValue(false).
			Show()
		if addOpclass {
			name, _ := pterm.DefaultInteractiveTextInput.WithDefaultText("name").Show()
			params, _ := pterm.DefaultInteractiveTextInput.WithDefaultText("params").Show()
			indexField.Opclass = &IndexFieldOpclass{Name: name, Params: strings.Split(params, ",")}
		}

		columns = append(columns, indexField)
		addColumns, _ = pterm.DefaultInteractiveConfirm.
			WithDefaultText("Add more columns").
			Show()
	}
	o.Columns = columns
	o.Unique = getBooleanOptionForColumnAttr("unique")
	indexMethod, _ := pterm.DefaultInteractiveSelect.
		WithDefaultText("method").
		WithDefaultOption("btree").
		WithOptions([]string{"btree", "hash", "gist", "spgist", "gin", "brin"}).
		Show()
	o.Method, _ = ParseCreateIndexMethod(indexMethod)
	o.Predicate, _ = pterm.DefaultInteractiveTextInput.WithDefaultText("predicate").Show()
	o.StorageParameters, _ = pterm.DefaultInteractiveTextInput.WithDefaultText("storage_parameters").Show()
}

func (o *OpCreateConstraint) Create() {
	o.Table, _ = pterm.DefaultInteractiveTextInput.WithDefaultText("table").Show()
	columnsStr, _ := pterm.DefaultInteractiveTextInput.WithDefaultText("columns").Show()
	o.Columns = strings.Split(columnsStr, ",")
	o.Name, _ = pterm.DefaultInteractiveTextInput.WithDefaultText("name").Show()
	constraintType, _ := pterm.DefaultInteractiveSelect.
		WithDefaultText("type").
		WithOptions([]string{"unique", "primary_key", "foreign_key", "check"}).
		Show()
	o.Type = OpCreateConstraintType(constraintType)
	switch o.Type {
	case OpCreateConstraintTypeCheck:
		check, _ := pterm.DefaultInteractiveTextInput.WithDefaultText("check").Show()
		if check != "" {
			o.Check = &check
		}
	case OpCreateConstraintTypeForeignKey:
		var reference TableForeignKeyReference
		reference.Table, _ = pterm.DefaultInteractiveTextInput.WithDefaultText("references.table").Show()
		referencedColumns, _ := pterm.DefaultInteractiveTextInput.WithDefaultText("references.columns").Show()
		reference.Columns = strings.Split(referencedColumns, ",")
		reference.OnDelete = getFkAction("on_delete")
		reference.OnUpdate = getFkAction("on_update")
		o.References = &reference
	}
	upMigrations := make(map[string]string, len(o.Columns))
	downMigrations := make(map[string]string, len(o.Columns))
	for _, columnName := range o.Columns {
		up, _ := pterm.DefaultInteractiveTextInput.WithDefaultText(fmt.Sprintf("up migration for %s", columnName)).Show()
		upMigrations[columnName] = up
		down, _ := pterm.DefaultInteractiveTextInput.WithDefaultText(fmt.Sprintf("down migration for %s", columnName)).Show()
		downMigrations[columnName] = down
	}
	o.Up = upMigrations
	o.Down = downMigrations
}

func (o *OpAddColumn) Create() {
	o.Table, _ = pterm.DefaultInteractiveTextInput.WithDefaultText("table").Show()
	o.Column = getColumnFromCLI()
	o.Up, _ = pterm.DefaultInteractiveTextInput.WithDefaultText("up").Show()
}

func (o *OpAlterColumn) Create() {
	o.Table, _ = pterm.DefaultInteractiveTextInput.WithDefaultText("table").Show()
	o.Column, _ = pterm.DefaultInteractiveTextInput.WithDefaultText("column").Show()
	newType, _ := pterm.DefaultInteractiveTextInput.WithDefaultText("type").Show()
	if newType != "" {
		o.Type = &newType
	}
	unique, _ := pterm.DefaultInteractiveTextInput.WithDefaultText("unique_constraint").Show()
	if unique != "" {
		o.Unique = &UniqueConstraint{Name: unique}
	}
	nullableStr, _ := pterm.DefaultInteractiveTextInput.WithDefaultText("nullable").Show()
	if nullableStr != "" {
		nullable, _ := strconv.ParseBool(nullableStr)
		o.Nullable = &nullable
	}
	addCheckConstraint, _ := pterm.DefaultInteractiveConfirm.
		WithDefaultText("Add check constraint").
		WithDefaultValue(false).
		Show()
	if addCheckConstraint {
		var c CheckConstraint
		c.Name, _ = pterm.DefaultInteractiveTextInput.WithDefaultText("name").Show()
		c.Constraint, _ = pterm.DefaultInteractiveTextInput.WithDefaultText("constraint").Show()
		c.NoInherit, _ = pterm.DefaultInteractiveConfirm.WithDefaultText("no_inherit").WithDefaultValue(false).Show()
		o.Check = &c
	}
	addForeignKey, _ := pterm.DefaultInteractiveConfirm.
		WithDefaultText("Add foreign key constraint").
		WithDefaultValue(false).
		Show()
	if addForeignKey {
		var r ForeignKeyReference
		r.Name, _ = pterm.DefaultInteractiveTextInput.WithDefaultText("name").Show()
		r.Table, _ = pterm.DefaultInteractiveTextInput.WithDefaultText("table").Show()
		r.Column, _ = pterm.DefaultInteractiveTextInput.WithDefaultText("column").Show()
		r.Deferrable, _ = pterm.DefaultInteractiveConfirm.WithDefaultText("deferrable").WithDefaultValue(false).Show()
		r.InitiallyDeferred, _ = pterm.DefaultInteractiveConfirm.WithDefaultText("initially_deferred").WithDefaultValue(false).Show()
		r.OnDelete = getFkAction("on_delete")
		r.OnUpdate = getFkAction("on_update")
		o.References = &r
	}
	o.Up, _ = pterm.DefaultInteractiveTextInput.WithDefaultText("up").Show()
	o.Down, _ = pterm.DefaultInteractiveTextInput.WithDefaultText("down").Show()
}

func (o *OpDropColumn) Create() {
	o.Table, _ = pterm.DefaultInteractiveTextInput.WithDefaultText("table").Show()
	o.Column, _ = pterm.DefaultInteractiveTextInput.WithDefaultText("column").Show()
	o.Down, _ = pterm.DefaultInteractiveTextInput.WithDefaultText("down").Show()
}

func (o *OpDropMultiColumnConstraint) Create() {
	o.Table, _ = pterm.DefaultInteractiveTextInput.WithDefaultText("table").Show()
	o.Name, _ = pterm.DefaultInteractiveTextInput.WithDefaultText("name").Show()
	o.Up = make(map[string]string)
	o.Down = make(map[string]string)
	addColumns, _ := pterm.DefaultInteractiveConfirm.WithDefaultValue(true).WithDefaultText("Add columns").Show()
	for addColumns {
		columnName, _ := pterm.DefaultInteractiveTextInput.WithDefaultText("column name").Show()
		up, _ := pterm.DefaultInteractiveTextInput.WithDefaultText("up").Show()
		down, _ := pterm.DefaultInteractiveTextInput.WithDefaultText("down").Show()
		o.Up[columnName] = up
		o.Down[columnName] = down
		addColumns, _ = pterm.DefaultInteractiveConfirm.WithDefaultValue(true).WithDefaultText("Add more columns").Show()
	}
}

func (o *OpDropIndex) Create() {
	o.Name, _ = pterm.DefaultInteractiveTextInput.WithDefaultText("name").Show()
}

func (o *OpDropTable) Create() {
	o.Name, _ = pterm.DefaultInteractiveTextInput.WithDefaultText("name").Show()
}

func (o *OpRawSQL) Create() {
	o.Up, _ = pterm.DefaultInteractiveTextInput.WithDefaultText("up").Show()
	o.Down, _ = pterm.DefaultInteractiveTextInput.WithDefaultText("down").Show()
	o.OnComplete, _ = pterm.DefaultInteractiveConfirm.WithDefaultValue(true).WithDefaultText("on_complete").Show()
}

func (o *OpRenameColumn) Create() {
	o.Table, _ = pterm.DefaultInteractiveTextInput.WithDefaultText("table").Show()
	o.From, _ = pterm.DefaultInteractiveTextInput.WithDefaultText("from").Show()
	o.To, _ = pterm.DefaultInteractiveTextInput.WithDefaultText("to").Show()
}

func (o *OpRenameConstraint) Create() {
	o.Table, _ = pterm.DefaultInteractiveTextInput.WithDefaultText("table").Show()
	o.From, _ = pterm.DefaultInteractiveTextInput.WithDefaultText("from").Show()
	o.To, _ = pterm.DefaultInteractiveTextInput.WithDefaultText("to").Show()
}

func (o *OpRenameTable) Create() {
	o.From, _ = pterm.DefaultInteractiveTextInput.WithDefaultText("from").Show()
	o.To, _ = pterm.DefaultInteractiveTextInput.WithDefaultText("to").Show()
}

func getFkAction(name string) ForeignKeyAction {
	action, _ := pterm.DefaultInteractiveSelect.
		WithDefaultText(name).
		WithOptions([]string{"CASCADE", "SET NULL", "RESTRICT", "NO ACTION"}).
		WithDefaultOption("NO ACTION").
		Show()
	return ForeignKeyAction(action)
}

func getColumnFromCLI() Column {
	var c Column
	c.Name, _ = pterm.DefaultInteractiveTextInput.WithDefaultText("name").Show()
	c.Type, _ = pterm.DefaultInteractiveTextInput.WithDefaultText("type").Show()
	comment, _ := pterm.DefaultInteractiveTextInput.WithDefaultText("comment").Show()
	if comment != "" {
		c.Comment = &comment
	}
	c.Pk = getBooleanOptionForColumnAttr("pk")
	c.Nullable = getBooleanOptionForColumnAttr("nullable")
	c.Unique = getBooleanOptionForColumnAttr("unique")

	if getBooleanOptionForColumnAttr("generated") {
		expression, _ := pterm.DefaultInteractiveTextInput.WithDefaultText("expression").Show()
		generated := ColumnGenerated{Expression: expression}
		if getBooleanOptionForColumnAttr("identity") {
			seqOptions, _ := pterm.DefaultInteractiveTextInput.
				WithDefaultText("sequence_options").
				Show()
			identityType, _ := pterm.DefaultInteractiveSelect.
				WithDefaultText("user_specified_values").
				WithOptions([]string{"ALWAYS", "BY DEFAULT"}).
				Show()
			if seqOptions != "" || identityType != "" {
				generated.Identity = &ColumnGeneratedIdentity{
					SequenceOptions:     seqOptions,
					UserSpecifiedValues: ColumnGeneratedIdentityUserSpecifiedValues(identityType),
				}
			}
		}
		c.Generated = &generated
	}
	return c
}

func getBooleanOptionForColumnAttr(name string) bool {
	val, _ := pterm.DefaultInteractiveContinue.
		WithDefaultText(name).
		WithOptions([]string{"true", "false"}).
		WithDefaultValueIndex(1).Show()
	boolVal, _ := strconv.ParseBool(val)
	return boolVal
}
