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
				Name:      "foo_a_fkey",
				Table:     "bar",
				Column:    "b",
				MatchType: migrations.ForeignKeyMatchTypeSIMPLE,
				OnDelete:  migrations.ForeignKeyActionNOACTION,
				OnUpdate:  migrations.ForeignKeyActionNOACTION,
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
				Name:      "foo_a_fkey",
				Table:     "bar",
				Column:    "b",
				MatchType: migrations.ForeignKeyMatchTypeSIMPLE,
				OnDelete:  migrations.ForeignKeyActionRESTRICT,
				OnUpdate:  migrations.ForeignKeyActionNOACTION,
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
				Name:      "foo_a_fkey",
				Table:     "bar",
				Column:    "b",
				MatchType: migrations.ForeignKeyMatchTypeSIMPLE,
				OnDelete:  migrations.ForeignKeyActionSETNULL,
				OnUpdate:  migrations.ForeignKeyActionNOACTION,
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
				Name:      "foo_a_fkey",
				Table:     "bar",
				Column:    "b",
				MatchType: migrations.ForeignKeyMatchTypeSIMPLE,
				OnDelete:  migrations.ForeignKeyActionSETDEFAULT,
				OnUpdate:  migrations.ForeignKeyActionNOACTION,
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
				Name:      "foo_a_fkey",
				Table:     "bar",
				Column:    "b",
				MatchType: migrations.ForeignKeyMatchTypeSIMPLE,
				OnDelete:  migrations.ForeignKeyActionCASCADE,
				OnUpdate:  migrations.ForeignKeyActionNOACTION,
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
				Name:      "my_fk",
				Table:     "bar",
				Column:    "b",
				MatchType: migrations.ForeignKeyMatchTypeFULL,
				OnDelete:  migrations.ForeignKeyActionNOACTION,
				OnUpdate:  migrations.ForeignKeyActionNOACTION,
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
					SequenceOptions:     "START 1 INCREMENT 1 MINVALUE 1 MAXVALUE 1000 CYCLE CACHE 1 ",
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

var CreateTableOp29 = &migrations.OpCreateTable{
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
			Type:              migrations.ConstraintTypeForeignKey,
			Columns:           []string{"a"},
			Name:              "foo_fk",
			NullsNotDistinct:  false,
			Deferrable:        false,
			InitiallyDeferred: false,
			NoInherit:         false,
			References: &migrations.TableForeignKeyReference{
				Table:     "bar",
				Columns:   []string{"b"},
				OnDelete:  migrations.ForeignKeyActionNOACTION,
				OnUpdate:  migrations.ForeignKeyActionNOACTION,
				MatchType: migrations.ForeignKeyMatchTypeSIMPLE,
			},
		},
	},
}

var CreateTableOp30 = &migrations.OpCreateTable{
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
			Type:              migrations.ConstraintTypeForeignKey,
			Columns:           []string{"a"},
			NullsNotDistinct:  false,
			Deferrable:        false,
			InitiallyDeferred: false,
			NoInherit:         false,
			References: &migrations.TableForeignKeyReference{
				Table:     "bar",
				Columns:   []string{"b"},
				OnDelete:  migrations.ForeignKeyActionSETNULL,
				OnUpdate:  migrations.ForeignKeyActionCASCADE,
				MatchType: migrations.ForeignKeyMatchTypeSIMPLE,
			},
		},
	},
}

var CreateTableOp31 = &migrations.OpCreateTable{
	Name: "foo",
	Columns: []migrations.Column{
		{
			Name:     "a",
			Type:     "int",
			Nullable: true,
		},
		{
			Name:     "b",
			Type:     "int",
			Nullable: true,
		},
	},
	Constraints: []migrations.Constraint{
		{
			Type:              migrations.ConstraintTypeForeignKey,
			Columns:           []string{"a", "b"},
			NullsNotDistinct:  false,
			Deferrable:        false,
			InitiallyDeferred: false,
			NoInherit:         false,
			References: &migrations.TableForeignKeyReference{
				Table:              "bar",
				Columns:            []string{"c", "d"},
				OnDelete:           migrations.ForeignKeyActionSETNULL,
				OnDeleteSetColumns: []string{"b"},
				OnUpdate:           migrations.ForeignKeyActionNOACTION,
				MatchType:          migrations.ForeignKeyMatchTypeSIMPLE,
			},
		},
	},
}

var CreateTableOp32 = &migrations.OpCreateTable{
	Name: "foo",
	Columns: []migrations.Column{
		{
			Name:     "a",
			Type:     "int",
			Nullable: true,
		},
		{
			Name:     "b",
			Type:     "int",
			Nullable: true,
		},
		{
			Name:     "c",
			Type:     "int",
			Nullable: true,
		},
	},
	Constraints: []migrations.Constraint{
		{
			Type:              migrations.ConstraintTypeForeignKey,
			Columns:           []string{"a", "b", "c"},
			NullsNotDistinct:  false,
			Deferrable:        false,
			InitiallyDeferred: false,
			NoInherit:         false,
			References: &migrations.TableForeignKeyReference{
				Table:     "bar",
				Columns:   []string{"d", "e", "f"},
				OnDelete:  migrations.ForeignKeyActionSETNULL,
				OnUpdate:  migrations.ForeignKeyActionCASCADE,
				MatchType: migrations.ForeignKeyMatchTypeSIMPLE,
			},
		},
	},
}

var CreateTableOp33 = &migrations.OpCreateTable{
	Name: "foo",
	Columns: []migrations.Column{
		{
			Name:     "a",
			Type:     "int",
			Nullable: true,
		},
		{
			Name:     "b",
			Type:     "int",
			Nullable: true,
		},
		{
			Name:     "c",
			Type:     "int",
			Nullable: true,
		},
	},
	Constraints: []migrations.Constraint{
		{
			Type:              migrations.ConstraintTypeForeignKey,
			Columns:           []string{"a", "b", "c"},
			NullsNotDistinct:  false,
			Deferrable:        false,
			InitiallyDeferred: false,
			NoInherit:         false,
			References: &migrations.TableForeignKeyReference{
				Table:     "bar",
				Columns:   []string{"d", "e", "f"},
				OnDelete:  migrations.ForeignKeyActionNOACTION,
				OnUpdate:  migrations.ForeignKeyActionNOACTION,
				MatchType: migrations.ForeignKeyMatchTypeFULL,
			},
		},
	},
}

func CreateTableOp34(matchType migrations.ForeignKeyMatchType, onDelete, onUpdate migrations.ForeignKeyAction) *migrations.OpCreateTable {
	return &migrations.OpCreateTable{
		Name: "foo",
		Columns: []migrations.Column{
			{
				Name:     "a",
				Type:     "int",
				Nullable: true,
				References: &migrations.ForeignKeyReference{
					Name:      "foo_a_fkey",
					Column:    "b",
					Table:     "bar",
					MatchType: matchType,
					OnDelete:  onDelete,
					OnUpdate:  onUpdate,
				},
			},
		},
	}
}

var CreateTableOp35 = &migrations.OpCreateTable{
	Name: "foo",
	Columns: []migrations.Column{
		{
			Name:     "id",
			Type:     "int",
			Nullable: true,
		},
		{
			Name:     "s",
			Type:     "timestamptz",
			Nullable: true,
		},
		{
			Name:     "e",
			Type:     "timestamptz",
			Nullable: true,
		},
		{
			Name:     "canceled",
			Type:     "boolean",
			Nullable: true,
			Default:  ptr("false"),
		},
	},
	Constraints: []migrations.Constraint{
		{
			Type:              migrations.ConstraintTypeExclude,
			Columns:           []string{},
			NullsNotDistinct:  false,
			Deferrable:        false,
			InitiallyDeferred: false,
			NoInherit:         false,
			Exclude: &migrations.ConstraintExclude{
				IndexMethod: "gist",
				Predicate:   "NOT canceled",
				Elements:    "id WITH =, tstzrange(s, e) WITH &&",
			},
		},
	},
}

var CreateTableOp36 = &migrations.OpCreateTable{
	Name: "foo",
	Columns: []migrations.Column{
		{
			Name:     "id",
			Type:     "int",
			Nullable: true,
		},
		{
			Name:     "b",
			Type:     "text",
			Nullable: true,
		},
	},
	Constraints: []migrations.Constraint{
		{
			Type:              migrations.ConstraintTypeExclude,
			Columns:           []string{},
			NullsNotDistinct:  false,
			Deferrable:        true,
			InitiallyDeferred: true,
			NoInherit:         false,
			Exclude: &migrations.ConstraintExclude{
				IndexMethod: "btree",
				Elements:    "id WITH =",
			},
		},
	},
}
