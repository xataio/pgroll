// SPDX-License-Identifier: Apache-2.0

package expect

import (
	"github.com/xataio/pgroll/pkg/migrations"
)

var CreateIndexOp1 = &migrations.OpCreateIndex{
	Name:    "idx_name",
	Table:   "foo",
	Columns: []string{"bar"},
	Method:  ptr(migrations.OpCreateIndexMethodBtree),
}

func CreateIndexOp1WithMethod(method string) *migrations.OpCreateIndex {
	parsed, err := migrations.ParseCreateIndexMethod(method)
	if err != nil {
		panic(err)
	}
	return &migrations.OpCreateIndex{
		Name:    "idx_name",
		Table:   "foo",
		Columns: []string{"bar"},
		Method:  ptr(parsed),
	}
}

var CreateIndexOp2 = &migrations.OpCreateIndex{
	Name:    "idx_name",
	Table:   "schema.foo",
	Columns: []string{"bar"},
	Method:  ptr(migrations.OpCreateIndexMethodBtree),
}

var CreateIndexOp3 = &migrations.OpCreateIndex{
	Name:    "idx_name",
	Table:   "foo",
	Columns: []string{"bar", "baz"},
	Method:  ptr(migrations.OpCreateIndexMethodBtree),
}

var CreateIndexOp4 = &migrations.OpCreateIndex{
	Name:    "idx_name",
	Table:   "foo",
	Columns: []string{"bar"},
	Method:  ptr(migrations.OpCreateIndexMethodBtree),
	Unique:  ptr(true),
}

var CreateIndexOp5 = &migrations.OpCreateIndex{
	Name:      "idx_name",
	Table:     "foo",
	Columns:   []string{"bar"},
	Method:    ptr(migrations.OpCreateIndexMethodBtree),
	Predicate: ptr("foo > 0"),
}

func CreateIndexOpWithStorageParam(param string) *migrations.OpCreateIndex {
	return &migrations.OpCreateIndex{
		Name:              "idx_name",
		Table:             "foo",
		Columns:           []string{"bar"},
		Method:            ptr(migrations.OpCreateIndexMethodBtree),
		StorageParameters: ptr(param),
	}
}
