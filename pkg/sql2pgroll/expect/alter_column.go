// SPDX-License-Identifier: Apache-2.0

package expect

import (
	"github.com/xataio/pgroll/pkg/migrations"
	"github.com/xataio/pgroll/pkg/sql2pgroll"
)

var AlterColumnOp1 = &migrations.OpAlterColumn{
	Table:    "foo",
	Column:   "a",
	Nullable: ptr(false),
	Up:       sql2pgroll.PlaceHolderSQL,
	Down:     sql2pgroll.PlaceHolderSQL,
}

var AlterColumnOp2 = &migrations.OpAlterColumn{
	Table:    "foo",
	Column:   "a",
	Nullable: ptr(true),
	Up:       sql2pgroll.PlaceHolderSQL,
	Down:     sql2pgroll.PlaceHolderSQL,
}

var AlterColumnOp3 = &migrations.OpAlterColumn{
	Table:  "foo",
	Column: "a",
	Type:   ptr("text"),
	Up:     sql2pgroll.PlaceHolderSQL,
	Down:   sql2pgroll.PlaceHolderSQL,
}

var AlterColumnOp4 = &migrations.OpAlterColumn{
	Table:  "foo",
	Column: "a",
	Name:   ptr("b"),
}

func ptr[T any](v T) *T {
	return &v
}
