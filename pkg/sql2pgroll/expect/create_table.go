// SPDX-License-Identifier: Apache-2.0

package expect

import "github.com/xataio/pgroll/pkg/migrations"

var CreateTableOp1 = &migrations.OpCreateTable{
	Name: "foo",
	Columns: []migrations.Column{
		{
			Name:     "a",
			Type:     "int",
			Nullable: true,
		},
	},
}

var CreateTableOp2 = &migrations.OpCreateTable{
	Name: "foo",
	Columns: []migrations.Column{
		{
			Name: "a",
			Type: "int",
		},
	},
}

var CreateTableOp3 = &migrations.OpCreateTable{
	Name: "foo",
	Columns: []migrations.Column{
		{
			Name:     "a",
			Type:     "varchar(255)",
			Nullable: true,
		},
	},
}

var CreateTableOp4 = &migrations.OpCreateTable{
	Name: "foo",
	Columns: []migrations.Column{
		{
			Name:     "a",
			Type:     "numeric(10, 2)",
			Nullable: true,
		},
	},
}

var CreateTableOp5 = &migrations.OpCreateTable{
	Name: "foo",
	Columns: []migrations.Column{
		{
			Name:     "a",
			Type:     "int",
			Nullable: true,
			Unique:   true,
		},
	},
}

var CreateTableOp6 = &migrations.OpCreateTable{
	Name: "foo",
	Columns: []migrations.Column{
		{
			Name: "a",
			Type: "int",
			Pk:   true,
		},
	},
}

var CreateTableOp7 = &migrations.OpCreateTable{
	Name: "foo",
	Columns: []migrations.Column{
		{
			Name:     "a",
			Type:     "text[]",
			Nullable: true,
		},
	},
}

var CreateTableOp8 = &migrations.OpCreateTable{
	Name: "foo",
	Columns: []migrations.Column{
		{
			Name:     "a",
			Type:     "text[5]",
			Nullable: true,
		},
	},
}

var CreateTableOp9 = &migrations.OpCreateTable{
	Name: "foo",
	Columns: []migrations.Column{
		{
			Name:     "a",
			Type:     "text[5][3]",
			Nullable: true,
		},
	},
}

var CreateTableOp10 = &migrations.OpCreateTable{
	Name: "foo",
	Columns: []migrations.Column{
		{
			Name:     "a",
			Type:     "int",
			Nullable: true,
			Check: &migrations.CheckConstraint{
				Name:       "foo_a_check",
				Constraint: "a > 0",
			},
		},
	},
}

var CreateTableOp11 = &migrations.OpCreateTable{
	Name: "foo",
	Columns: []migrations.Column{
		{
			Name:     "a",
			Type:     "timestamptz",
			Nullable: true,
			Default:  ptr("now()"),
		},
	},
}

var CreateTableOp12 = &migrations.OpCreateTable{
	Name: "foo",
	Columns: []migrations.Column{
		{
			Name:     "a",
			Type:     "int",
			Nullable: true,
			References: &migrations.ForeignKeyReference{
				Name:     "foo_a_fkey",
				Table:    "bar",
				Column:   "b",
				OnDelete: migrations.ForeignKeyReferenceOnDeleteNOACTION,
			},
		},
	},
}

var CreateTableOp13 = &migrations.OpCreateTable{
	Name: "foo",
	Columns: []migrations.Column{
		{
			Name:     "a",
			Type:     "int",
			Nullable: true,
			References: &migrations.ForeignKeyReference{
				Name:     "foo_a_fkey",
				Table:    "bar",
				Column:   "b",
				OnDelete: migrations.ForeignKeyReferenceOnDeleteRESTRICT,
			},
		},
	},
}

var CreateTableOp14 = &migrations.OpCreateTable{
	Name: "foo",
	Columns: []migrations.Column{
		{
			Name:     "a",
			Type:     "int",
			Nullable: true,
			References: &migrations.ForeignKeyReference{
				Name:     "foo_a_fkey",
				Table:    "bar",
				Column:   "b",
				OnDelete: migrations.ForeignKeyReferenceOnDeleteSETNULL,
			},
		},
	},
}

var CreateTableOp15 = &migrations.OpCreateTable{
	Name: "foo",
	Columns: []migrations.Column{
		{
			Name:     "a",
			Type:     "int",
			Nullable: true,
			References: &migrations.ForeignKeyReference{
				Name:     "foo_a_fkey",
				Table:    "bar",
				Column:   "b",
				OnDelete: migrations.ForeignKeyReferenceOnDeleteSETDEFAULT,
			},
		},
	},
}

var CreateTableOp16 = &migrations.OpCreateTable{
	Name: "foo",
	Columns: []migrations.Column{
		{
			Name:     "a",
			Type:     "int",
			Nullable: true,
			References: &migrations.ForeignKeyReference{
				Name:     "foo_a_fkey",
				Table:    "bar",
				Column:   "b",
				OnDelete: migrations.ForeignKeyReferenceOnDeleteCASCADE,
			},
		},
	},
}

var CreateTableOp17 = &migrations.OpCreateTable{
	Name: "schema.foo",
	Columns: []migrations.Column{
		{
			Name:     "a",
			Type:     "int",
			Nullable: true,
		},
	},
}

var CreateTableOp18 = &migrations.OpCreateTable{
	Name: "foo",
	Columns: []migrations.Column{
		{
			Name:     "a",
			Type:     "int",
			Nullable: true,
			Check: &migrations.CheckConstraint{
				Name:       "my_check",
				Constraint: "a > 0",
			},
		},
	},
}

var CreateTableOp19 = &migrations.OpCreateTable{
	Name: "foo",
	Columns: []migrations.Column{
		{
			Name:     "a",
			Type:     "int",
			Nullable: true,
			References: &migrations.ForeignKeyReference{
				Name:     "my_fk",
				Table:    "bar",
				Column:   "b",
				OnDelete: migrations.ForeignKeyReferenceOnDeleteNOACTION,
			},
		},
	},
}

var CreateTableOp20 = &migrations.OpCreateTable{
	Name: "foo",
	Columns: []migrations.Column{
		{
			Name:     "a",
			Type:     "int",
			Nullable: true,
		},
	},
}

var CreateTableOp21 = &migrations.OpCreateTable{
	Name: "foo",
	Columns: []migrations.Column{
		{
			Name: "a",
			Type: "serial",
			Pk:   true,
		},
		{
			Name:    "b",
			Type:    "int",
			Default: ptr("100"),
			Check: &migrations.CheckConstraint{
				Name:       "foo_b_check",
				Constraint: "b > 0",
			},
			Nullable: true,
		},
		{
			Name:   "c",
			Type:   "text",
			Unique: true,
		},
	},
}

var CreateTableOp22 = &migrations.OpCreateTable{
	Name: "foo",
	Columns: []migrations.Column{
		{
			Name: "a",
			Type: "serial",
			Pk:   true,
		},
		{
			Name:     "b",
			Type:     "text",
			Nullable: true,
		},
		{
			Name:     "c",
			Type:     "text",
			Nullable: true,
		},
	},
	Constraints: []migrations.Constraint{
		{
			Type:              migrations.ConstraintTypeUnique,
			Columns:           []string{"b", "c"},
			NullsNotDistinct:  false,
			Deferrable:        false,
			InitiallyDeferred: false,
		},
	},
}

var CreateTableOp23 = &migrations.OpCreateTable{
	Name: "foo",
	Columns: []migrations.Column{
		{
			Name:     "b",
			Type:     "text",
			Nullable: true,
		},
		{
			Name:     "c",
			Type:     "text",
			Nullable: true,
		},
	},
	Constraints: []migrations.Constraint{
		{
			Type:              migrations.ConstraintTypeUnique,
			Columns:           []string{"b"},
			NullsNotDistinct:  false,
			Deferrable:        false,
			InitiallyDeferred: false,
			IndexParameters: &migrations.ConstraintIndexParameters{
				IncludeColumns:    []string{"c"},
				StorageParameters: "fillfactor=70",
				Tablespace:        "my_tablespace",
			},
		},
	},
}

var CreateTableOp24 = &migrations.OpCreateTable{
	Name: "foo",
	Columns: []migrations.Column{
		{
			Name:     "a",
			Type:     "int",
			Nullable: true,
		},
	},
	Constraints: []migrations.Constraint{
		{
			Type:              migrations.ConstraintTypeCheck,
			Check:             "a > 0",
			Columns:           []string{},
			NullsNotDistinct:  false,
			Deferrable:        false,
			InitiallyDeferred: false,
			NoInherit:         false,
		},
	},
}

var CreateTableOp25 = &migrations.OpCreateTable{
	Name: "foo",
	Columns: []migrations.Column{
		{
			Name:     "b",
			Type:     "text",
			Nullable: true,
		},
		{
			Name:     "c",
			Type:     "text",
			Nullable: true,
		},
	},
	Constraints: []migrations.Constraint{
		{
			Type:              migrations.ConstraintTypeCheck,
			Check:             "b = c",
			Columns:           []string{},
			NullsNotDistinct:  false,
			Deferrable:        false,
			InitiallyDeferred: false,
			NoInherit:         true,
		},
	},
}

var CreateTableOp26 = &migrations.OpCreateTable{
	Name: "foo",
	Columns: []migrations.Column{
		{
			Name:     "b",
			Type:     "text",
			Nullable: true,
		},
		{
			Name:     "c",
			Type:     "text",
			Nullable: true,
		},
	},
	Constraints: []migrations.Constraint{
		{
			Type:              migrations.ConstraintTypePrimaryKey,
			Columns:           []string{"b"},
			NullsNotDistinct:  false,
			Deferrable:        true,
			InitiallyDeferred: false,
			NoInherit:         false,
		},
	},
}

var CreateTableOp27 = &migrations.OpCreateTable{
	Name: "foo",
	Columns: []migrations.Column{
		{
			Name: "b",
			Type: "bigint",
			Pk:   true,
			Generated: &migrations.ColumnGenerated{
				Identity: &migrations.ColumnGeneratedIdentity{
					UserSpecifiedValues: migrations.ColumnGeneratedIdentityUserSpecifiedValuesALWAYS,
					SequenceOptions:     "start 1 increment 1 minvalue 1 maxvalue 1000 cycle  cache 1",
				},
			},
		},
	},
}

var CreateTableOp28 = &migrations.OpCreateTable{
	Name: "foo",
	Columns: []migrations.Column{
		{
			Name:     "a",
			Type:     "text",
			Nullable: true,
		},
		{
			Name: "b",
			Type: "text",
			Generated: &migrations.ColumnGenerated{
				Expression: "upper(a)",
			},
		},
	},
}
