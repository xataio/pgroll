// SPDX-License-Identifier: Apache-2.0

package expect

import (
	"github.com/xataio/pgroll/pkg/migrations"
	"github.com/xataio/pgroll/pkg/sql2pgroll"
)

var AlterTableOp1 = &migrations.OpAlterColumn{
	Table:    "foo",
	Column:   "a",
	Nullable: ptr(false),
	Up:       sql2pgroll.PlaceHolderSQL,
	Down:     sql2pgroll.PlaceHolderSQL,
}

var AlterTableOp2 = &migrations.OpAlterColumn{
	Table:    "foo",
	Column:   "a",
	Nullable: ptr(true),
	Up:       sql2pgroll.PlaceHolderSQL,
	Down:     sql2pgroll.PlaceHolderSQL,
}

var AlterTableOp3 = &migrations.OpAlterColumn{
	Table:  "foo",
	Column: "a",
	Type:   ptr("text"),
	Up:     sql2pgroll.PlaceHolderSQL,
	Down:   sql2pgroll.PlaceHolderSQL,
}

var AlterTableOp4 = &migrations.OpCreateConstraint{
	Type:    migrations.OpCreateConstraintTypeUnique,
	Name:    "bar",
	Table:   "foo",
	Columns: []string{"a"},
	Down:    map[string]string{"a": sql2pgroll.PlaceHolderSQL},
	Up:      map[string]string{"a": sql2pgroll.PlaceHolderSQL},
}

var AlterTableOp5 = &migrations.OpCreateConstraint{
	Type:    migrations.OpCreateConstraintTypeUnique,
	Name:    "bar",
	Table:   "foo",
	Columns: []string{"a", "b"},
	Down: map[string]string{
		"a": sql2pgroll.PlaceHolderSQL,
		"b": sql2pgroll.PlaceHolderSQL,
	},
	Up: map[string]string{
		"a": sql2pgroll.PlaceHolderSQL,
		"b": sql2pgroll.PlaceHolderSQL,
	},
}

var AlterTableOp6 = &migrations.OpAlterColumn{
	Table:  "foo",
	Column: "a",
	Name:   ptr("b"),
}

func ptr[T any](v T) *T {
	return &v
}
