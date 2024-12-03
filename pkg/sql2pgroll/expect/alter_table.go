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

func ptr[T any](v T) *T {
	return &v
}
