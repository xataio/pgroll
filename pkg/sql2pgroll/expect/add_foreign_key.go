// SPDX-License-Identifier: Apache-2.0

package expect

import (
	"github.com/xataio/pgroll/pkg/migrations"
	"github.com/xataio/pgroll/pkg/sql2pgroll"
)

func AddForeignKeyOp1WithOnDelete(onDelete migrations.ForeignKeyAction) *migrations.OpCreateConstraint {
	return &migrations.OpCreateConstraint{
		Columns: []string{"a", "b"},
		Name:    "fk_bar_cd",
		References: &migrations.TableForeignKeyReference{
			Columns:  []string{"c", "d"},
			OnDelete: onDelete,
			Table:    "bar",
		},
		Table: "foo",
		Type:  migrations.OpCreateConstraintTypeForeignKey,
		Up: map[string]string{
			"a": sql2pgroll.PlaceHolderSQL,
			"b": sql2pgroll.PlaceHolderSQL,
		},
		Down: map[string]string{
			"a": sql2pgroll.PlaceHolderSQL,
			"b": sql2pgroll.PlaceHolderSQL,
		},
	}
}

var AddForeignKeyOp2 = &migrations.OpCreateConstraint{
	Columns: []string{"a"},
	Name:    "fk_bar_c",
	References: &migrations.TableForeignKeyReference{
		Columns:  []string{"c"},
		OnDelete: migrations.ForeignKeyActionNOACTION,
		Table:    "bar",
	},
	Table: "foo",
	Type:  migrations.OpCreateConstraintTypeForeignKey,
	Up: map[string]string{
		"a": sql2pgroll.PlaceHolderSQL,
	},
	Down: map[string]string{
		"a": sql2pgroll.PlaceHolderSQL,
	},
}

var AddForeignKeyOp3 = &migrations.OpCreateConstraint{
	Columns: []string{"a"},
	Name:    "fk_bar_c",
	References: &migrations.TableForeignKeyReference{
		Columns:  []string{"c"},
		OnDelete: migrations.ForeignKeyActionNOACTION,
		Table:    "schema_a.bar",
	},
	Table: "schema_a.foo",
	Type:  migrations.OpCreateConstraintTypeForeignKey,
	Up: map[string]string{
		"a": sql2pgroll.PlaceHolderSQL,
	},
	Down: map[string]string{
		"a": sql2pgroll.PlaceHolderSQL,
	},
}
