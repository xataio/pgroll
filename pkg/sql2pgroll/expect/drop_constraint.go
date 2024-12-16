// SPDX-License-Identifier: Apache-2.0

package expect

import (
	"github.com/xataio/pgroll/pkg/migrations"
	"github.com/xataio/pgroll/pkg/sql2pgroll"
)

func OpDropConstraintWithTable(table string) *migrations.OpDropMultiColumnConstraint {
	return &migrations.OpDropMultiColumnConstraint{
		Up: migrations.MultiColumnUpSQL{
			sql2pgroll.PlaceHolderColumnName: sql2pgroll.PlaceHolderSQL,
		},
		Down: migrations.MultiColumnDownSQL{
			sql2pgroll.PlaceHolderColumnName: sql2pgroll.PlaceHolderSQL,
		},
		Table: table,
		Name:  "constraint_foo",
	}
}
