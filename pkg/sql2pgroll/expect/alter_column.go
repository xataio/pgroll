// SPDX-License-Identifier: Apache-2.0

package expect

import (
	"github.com/oapi-codegen/nullable"

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

var AlterColumnOp5 = &migrations.OpAlterColumn{
	Table:   "foo",
	Column:  "bar",
	Default: nullable.NewNullableWithValue("'baz'"),
	Up:      sql2pgroll.PlaceHolderSQL,
	Down:    sql2pgroll.PlaceHolderSQL,
}

var AlterColumnOp6 = &migrations.OpAlterColumn{
	Table:   "foo",
	Column:  "bar",
	Default: nullable.NewNullableWithValue("123"),
	Up:      sql2pgroll.PlaceHolderSQL,
	Down:    sql2pgroll.PlaceHolderSQL,
}

var AlterColumnOp7 = &migrations.OpAlterColumn{
	Table:   "foo",
	Column:  "bar",
	Default: nullable.NewNullNullable[string](),
	Up:      sql2pgroll.PlaceHolderSQL,
	Down:    sql2pgroll.PlaceHolderSQL,
}

var AlterColumnOp8 = &migrations.OpAlterColumn{
	Table:   "foo",
	Column:  "bar",
	Default: nullable.NewNullableWithValue("123.456"),
	Up:      sql2pgroll.PlaceHolderSQL,
	Down:    sql2pgroll.PlaceHolderSQL,
}

var AlterColumnOp9 = &migrations.OpAlterColumn{
	Table:   "foo",
	Column:  "bar",
	Default: nullable.NewNullableWithValue("true"),
	Up:      sql2pgroll.PlaceHolderSQL,
	Down:    sql2pgroll.PlaceHolderSQL,
}

var AlterColumnOp10 = &migrations.OpAlterColumn{
	Table:   "foo",
	Column:  "bar",
	Default: nullable.NewNullableWithValue("b'0101'"),
	Up:      sql2pgroll.PlaceHolderSQL,
	Down:    sql2pgroll.PlaceHolderSQL,
}

var AlterColumnOp11 = &migrations.OpAlterColumn{
	Table:   "foo",
	Column:  "bar",
	Default: nullable.NewNullableWithValue("now()"),
	Up:      sql2pgroll.PlaceHolderSQL,
	Down:    sql2pgroll.PlaceHolderSQL,
}

var AlterColumnOp12 = &migrations.OpAlterColumn{
	Table:   "foo",
	Column:  "bar",
	Default: nullable.NewNullableWithValue("(first_name || ' ') || last_name"),
	Up:      sql2pgroll.PlaceHolderSQL,
	Down:    sql2pgroll.PlaceHolderSQL,
}

func ptr[T any](v T) *T {
	return &v
}
