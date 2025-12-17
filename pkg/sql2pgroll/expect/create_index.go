// SPDX-License-Identifier: Apache-2.0

package expect

import (
	"github.com/xataio/pgroll/pkg/migrations"
)

// newColumns creates OpCreateIndexColumns from a map for test fixtures.
func newColumns(m map[string]migrations.IndexField) migrations.OpCreateIndexColumns {
	cols := migrations.NewOpCreateIndexColumns()
	for name, settings := range m {
		cols.Set(name, settings)
	}
	return cols
}

var CreateIndexOp1 = &migrations.OpCreateIndex{
	Name:    "idx_name",
	Table:   "foo",
	Columns: newColumns(map[string]migrations.IndexField{"bar": {}}),
	Method:  migrations.OpCreateIndexMethodBtree,
}

func CreateIndexOp1WithMethod(method string) *migrations.OpCreateIndex {
	parsed, err := migrations.ParseCreateIndexMethod(method)
	if err != nil {
		panic(err)
	}
	return &migrations.OpCreateIndex{
		Name:    "idx_name",
		Table:   "foo",
		Columns: newColumns(map[string]migrations.IndexField{"bar": {}}),
		Method:  parsed,
	}
}

var CreateIndexOp2 = &migrations.OpCreateIndex{
	Name:    "idx_name",
	Table:   "schema.foo",
	Columns: newColumns(map[string]migrations.IndexField{"bar": {}}),
	Method:  migrations.OpCreateIndexMethodBtree,
}

var CreateIndexOp3 = &migrations.OpCreateIndex{
	Name:    "idx_name",
	Table:   "foo",
	Columns: newColumns(map[string]migrations.IndexField{"bar": {}, "baz": {}}),
	Method:  migrations.OpCreateIndexMethodBtree,
}

var CreateIndexOp4 = &migrations.OpCreateIndex{
	Name:    "idx_name",
	Table:   "foo",
	Columns: newColumns(map[string]migrations.IndexField{"bar": {}}),
	Method:  migrations.OpCreateIndexMethodBtree,
	Unique:  true,
}

var CreateIndexOp5 = &migrations.OpCreateIndex{
	Name:      "idx_name",
	Table:     "foo",
	Columns:   newColumns(map[string]migrations.IndexField{"bar": {}}),
	Method:    migrations.OpCreateIndexMethodBtree,
	Predicate: "foo > 0",
}

var CreateIndexOp6 = &migrations.OpCreateIndex{
	Name:   "idx_name",
	Table:  "foo",
	Method: migrations.OpCreateIndexMethodBtree,
	Columns: newColumns(map[string]migrations.IndexField{
		"bar": {
			Collate: "en_us",
		},
	}),
}

var CreateIndexOp7 = &migrations.OpCreateIndex{
	Name:   "idx_name",
	Table:  "foo",
	Method: migrations.OpCreateIndexMethodBtree,
	Columns: newColumns(map[string]migrations.IndexField{
		"bar": {
			Sort: migrations.IndexFieldSortDESC,
		},
	}),
}

var CreateIndexOp8 = &migrations.OpCreateIndex{
	Name:   "idx_name",
	Table:  "foo",
	Method: migrations.OpCreateIndexMethodBtree,
	Columns: newColumns(map[string]migrations.IndexField{
		"bar": {
			Nulls: ptr(migrations.IndexFieldNullsFIRST),
		},
	}),
}

var CreateIndexOp9 = &migrations.OpCreateIndex{
	Name:   "idx_name",
	Table:  "foo",
	Method: migrations.OpCreateIndexMethodBtree,
	Columns: newColumns(map[string]migrations.IndexField{
		"bar": {
			Nulls: ptr(migrations.IndexFieldNullsLAST),
		},
	}),
}

var CreateIndexOp10 = &migrations.OpCreateIndex{
	Name:   "idx_name",
	Table:  "foo",
	Method: migrations.OpCreateIndexMethodBtree,
	Columns: newColumns(map[string]migrations.IndexField{
		"bar": {
			Opclass: ptr(migrations.IndexFieldOpclass{
				Name:   "opclass",
				Params: []string{"test=test"},
			}),
		},
	}),
}

var CreateIndexOp11 = &migrations.OpCreateIndex{
	Name:   "idx_name",
	Table:  "foo",
	Method: migrations.OpCreateIndexMethodBtree,
	Columns: newColumns(map[string]migrations.IndexField{
		"bar": {
			Opclass: ptr(migrations.IndexFieldOpclass{
				Name:   "opclass1",
				Params: []string{},
			}),
		},
		"baz": {
			Opclass: ptr(migrations.IndexFieldOpclass{
				Name:   "opclass2",
				Params: []string{},
			}),
		},
	}),
}

var CreateIndexOp12 = &migrations.OpCreateIndex{
	Name:  "idx_name",
	Table: "foo",
	Columns: newColumns(map[string]migrations.IndexField{
		"bar": {
			Sort: migrations.IndexFieldSortASC,
		},
	}),
	Method: migrations.OpCreateIndexMethodBtree,
}

func CreateIndexOpWithStorageParam(param string) *migrations.OpCreateIndex {
	return &migrations.OpCreateIndex{
		Name:              "idx_name",
		Table:             "foo",
		Columns:           newColumns(map[string]migrations.IndexField{"bar": {}}),
		Method:            migrations.OpCreateIndexMethodBtree,
		StorageParameters: param,
	}
}
