// SPDX-License-Identifier: Apache-2.0

package expect

import (
	"github.com/xataio/pgroll/pkg/migrations"
	"github.com/xataio/pgroll/pkg/sql2pgroll"
)

func AddForeignKeyOp1WithOnDelete(onDelete migrations.ForeignKeyReferenceOnDelete) *migrations.OpCreateConstraint {
	return &migrations.OpCreateConstraint{
		Check:   nil,
		Columns: []string{"a", "b"},
		Name:    "fk_bar_cd",
		References: &migrations.OpCreateConstraintReferences{
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
