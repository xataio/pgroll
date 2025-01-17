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
		Name:     "bar",
		Type:     "int",
		Nullable: true,
	},
}

var AddColumnOp2 = &migrations.OpAddColumn{
	Table: "schema.foo",
	Up:    sql2pgroll.PlaceHolderSQL,
	Column: migrations.Column{
		Name:     "bar",
		Type:     "int",
		Nullable: true,
	},
}

func AddColumnOp1WithDefault(def *string) *migrations.OpAddColumn {
	return &migrations.OpAddColumn{
		Table: "foo",
		Up:    sql2pgroll.PlaceHolderSQL,
		Column: migrations.Column{
			Name:     "bar",
			Type:     "int",
			Default:  def,
			Nullable: true,
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
		Name:     "bar",
		Type:     "int",
		Unique:   true,
		Nullable: true,
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
		Name:     "bar",
		Type:     "int",
		Nullable: true,
		Check: &migrations.CheckConstraint{
			Constraint: "bar > 0",
			Name:       "foo_bar_check",
		},
	},
}

var AddColumnOp7 = &migrations.OpAddColumn{
	Table: "foo",
	Up:    sql2pgroll.PlaceHolderSQL,
	Column: migrations.Column{
		Name:     "bar",
		Type:     "int",
		Nullable: true,
		Check: &migrations.CheckConstraint{
			Constraint: "bar > 0",
			Name:       "check_bar",
		},
	},
}

var AddColumnOp8 = &migrations.OpAddColumn{
	Table: "foo",
	Up:    sql2pgroll.PlaceHolderSQL,
	Column: migrations.Column{
		Name:     "bar",
		Type:     "int",
		Nullable: false,
	},
}

var AddColumnOp9 = &migrations.OpAddColumn{
	Table: "foo",
	Up:    sql2pgroll.PlaceHolderSQL,
	Column: migrations.Column{
		Name: "bar",
		Type: "int",
		Generated: &migrations.ColumnGenerated{
			Identity: &migrations.ColumnGeneratedIdentity{
				UserSpecifiedValues: migrations.ColumnGeneratedIdentityUserSpecifiedValuesBYDEFAULT,
			},
		},
	},
}

var AddColumnOp10 = &migrations.OpAddColumn{
	Table: "foo",
	Up:    sql2pgroll.PlaceHolderSQL,
	Column: migrations.Column{
		Name: "bar",
		Type: "int",
		Generated: &migrations.ColumnGenerated{
			Expression: "123",
		},
	},
}

func AddColumnOp8WithOnDeleteAction(action migrations.ForeignKeyReferenceOnDelete) *migrations.OpAddColumn {
	return &migrations.OpAddColumn{
		Table: "foo",
		Up:    sql2pgroll.PlaceHolderSQL,
		Column: migrations.Column{
			Name:     "bar",
			Type:     "int",
			Nullable: true,
			References: &migrations.ForeignKeyReference{
				Column:   "bar",
				Name:     "fk_baz",
				OnDelete: action,
				Table:    "baz",
			},
		},
	}
}
