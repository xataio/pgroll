// SPDX-License-Identifier: Apache-2.0

package expect

import (
	"github.com/xataio/pgroll/pkg/migrations"
	"github.com/xataio/pgroll/pkg/sql2pgroll"
)

var AddColumnOp1 = &migrations.OpAddColumn{
	Table: "foo",
	Up:    sql2pgroll.PlaceHolderSQL,
	Column: migrations.Column{
		Name: "bar",
		Type: "int",
	},
}

var AddColumnOp2 = &migrations.OpAddColumn{
	Table: "schema.foo",
	Up:    sql2pgroll.PlaceHolderSQL,
	Column: migrations.Column{
		Name: "bar",
		Type: "int",
	},
}

func AddColumnOp1WithDefault(def *string) *migrations.OpAddColumn {
	return &migrations.OpAddColumn{
		Table: "foo",
		Up:    sql2pgroll.PlaceHolderSQL,
		Column: migrations.Column{
			Name:    "bar",
			Type:    "int",
			Default: def,
		},
	}
}

var AddColumnOp3 = &migrations.OpAddColumn{
	Table: "foo",
	Up:    sql2pgroll.PlaceHolderSQL,
	Column: migrations.Column{
		Name:     "bar",
		Type:     "int",
		Nullable: true,
	},
}

var AddColumnOp4 = &migrations.OpAddColumn{
	Table: "foo",
	Up:    sql2pgroll.PlaceHolderSQL,
	Column: migrations.Column{
		Name:   "bar",
		Type:   "int",
		Unique: true,
	},
}

var AddColumnOp5 = &migrations.OpAddColumn{
	Table: "foo",
	Up:    sql2pgroll.PlaceHolderSQL,
	Column: migrations.Column{
		Name: "bar",
		Type: "int",
		Pk:   true,
	},
}

var AddColumnOp6 = &migrations.OpAddColumn{
	Table: "foo",
	Up:    sql2pgroll.PlaceHolderSQL,
	Column: migrations.Column{
		Name: "bar",
		Type: "int",
		Check: &migrations.CheckConstraint{
			Constraint: "bar > 0",
			Name:       "",
		},
	},
}

var AddColumnOp7 = &migrations.OpAddColumn{
	Table: "foo",
	Up:    sql2pgroll.PlaceHolderSQL,
	Column: migrations.Column{
		Name: "bar",
		Type: "int",
		Check: &migrations.CheckConstraint{
			Constraint: "bar > 0",
			Name:       "check_bar",
		},
	},
}

func AddColumnOp8WithOnDeleteAction(action migrations.ForeignKeyReferenceOnDelete) *migrations.OpAddColumn {
	return &migrations.OpAddColumn{
		Table: "foo",
		Up:    sql2pgroll.PlaceHolderSQL,
		Column: migrations.Column{
			Name: "bar",
			Type: "int",
			References: &migrations.ForeignKeyReference{
				Column:   "bar",
				Name:     "fk_baz",
				OnDelete: action,
				Table:    "baz",
			},
		},
	}
}
