// SPDX-License-Identifier: Apache-2.0

package expect

import (
	"github.com/xataio/pgroll/pkg/migrations"
	"github.com/xataio/pgroll/pkg/sql2pgroll"
)

var AddForeignKeyOp1 = &migrations.OpCreateConstraint{
	Check:   nil,
	Columns: []string{"a", "b"},
	Name:    "fk_bar_cd",
	References: &migrations.OpCreateConstraintReferences{
		Columns:  []string{"c", "d"},
		OnDelete: migrations.ForeignKeyReferenceOnDeleteNOACTION,
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

var AddForeignKeyOp2 = &migrations.OpCreateConstraint{
	Check:   nil,
	Columns: []string{"a", "b"},
	Name:    "fk_bar_cd",
	References: &migrations.OpCreateConstraintReferences{
		Columns:  []string{"c", "d"},
		OnDelete: migrations.ForeignKeyReferenceOnDeleteRESTRICT,
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

var AddForeignKeyOp3 = &migrations.OpCreateConstraint{
	Check:   nil,
	Columns: []string{"a", "b"},
	Name:    "fk_bar_cd",
	References: &migrations.OpCreateConstraintReferences{
		Columns:  []string{"c", "d"},
		OnDelete: migrations.ForeignKeyReferenceOnDeleteSETDEFAULT,
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

var AddForeignKeyOp4 = &migrations.OpCreateConstraint{
	Check:   nil,
	Columns: []string{"a", "b"},
	Name:    "fk_bar_cd",
	References: &migrations.OpCreateConstraintReferences{
		Columns:  []string{"c", "d"},
		OnDelete: migrations.ForeignKeyReferenceOnDeleteSETNULL,
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
