// SPDX-License-Identifier: Apache-2.0

package expect

import (
	"github.com/xataio/pgroll/pkg/migrations"
)

// newColumns creates OpCreateIndexColumns from ordered column definitions for test fixtures.
// Accepts columns as alternating name (string) and settings (IndexField) pairs.
func newColumns(pairs ...interface{}) migrations.OpCreateIndexColumns {
	cols := migrations.NewOpCreateIndexColumns()
	for i := 0; i < len(pairs); i += 2 {
		name := pairs[i].(string)
		settings := pairs[i+1].(migrations.IndexField)
		cols.Set(name, settings)
	}
	return cols
}

var CreateIndexOp1 = &migrations.OpCreateIndex{
	Name:    "idx_name",
	Table:   "foo",
	Columns: newColumns("bar", migrations.IndexField{}),
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
		Columns: newColumns("bar", migrations.IndexField{}),
		Method:  parsed,
	}
}

var CreateIndexOp2 = &migrations.OpCreateIndex{
	Name:    "idx_name",
	Table:   "schema.foo",
	Columns: newColumns("bar", migrations.IndexField{}),
	Method:  migrations.OpCreateIndexMethodBtree,
}

var CreateIndexOp3 = &migrations.OpCreateIndex{
	Name:    "idx_name",
	Table:   "foo",
	Columns: newColumns("bar", migrations.IndexField{}, "baz", migrations.IndexField{}),
	Method:  migrations.OpCreateIndexMethodBtree,
}

var CreateIndexOp4 = &migrations.OpCreateIndex{
	Name:    "idx_name",
	Table:   "foo",
	Columns: newColumns("bar", migrations.IndexField{}),
	Method:  migrations.OpCreateIndexMethodBtree,
	Unique:  true,
}

var CreateIndexOp5 = &migrations.OpCreateIndex{
	Name:      "idx_name",
	Table:     "foo",
	Columns:   newColumns("bar", migrations.IndexField{}),
	Method:    migrations.OpCreateIndexMethodBtree,
	Predicate: "foo > 0",
}

var CreateIndexOp6 = &migrations.OpCreateIndex{
	Name:   "idx_name",
	Table:  "foo",
	Method: migrations.OpCreateIndexMethodBtree,
	Columns: newColumns("bar", migrations.IndexField{
		Collate: "en_us",
	}),
}

var CreateIndexOp7 = &migrations.OpCreateIndex{
	Name:   "idx_name",
	Table:  "foo",
	Method: migrations.OpCreateIndexMethodBtree,
	Columns: newColumns("bar", migrations.IndexField{
		Sort: migrations.IndexFieldSortDESC,
	}),
}

var CreateIndexOp8 = &migrations.OpCreateIndex{
	Name:   "idx_name",
	Table:  "foo",
	Method: migrations.OpCreateIndexMethodBtree,
	Columns: newColumns("bar", migrations.IndexField{
		Nulls: ptr(migrations.IndexFieldNullsFIRST),
	}),
}

var CreateIndexOp9 = &migrations.OpCreateIndex{
	Name:   "idx_name",
	Table:  "foo",
	Method: migrations.OpCreateIndexMethodBtree,
	Columns: newColumns("bar", migrations.IndexField{
		Nulls: ptr(migrations.IndexFieldNullsLAST),
	}),
}

var CreateIndexOp10 = &migrations.OpCreateIndex{
	Name:   "idx_name",
	Table:  "foo",
	Method: migrations.OpCreateIndexMethodBtree,
	Columns: newColumns("bar", migrations.IndexField{
		Opclass: ptr(migrations.IndexFieldOpclass{
			Name:   "opclass",
			Params: []string{"test=test"},
		}),
	}),
}

var CreateIndexOp11 = &migrations.OpCreateIndex{
	Name:   "idx_name",
	Table:  "foo",
	Method: migrations.OpCreateIndexMethodBtree,
	Columns: newColumns(
		"bar", migrations.IndexField{
			Opclass: ptr(migrations.IndexFieldOpclass{
				Name:   "opclass1",
				Params: []string{},
			}),
		},
		"baz", migrations.IndexField{
			Opclass: ptr(migrations.IndexFieldOpclass{
				Name:   "opclass2",
				Params: []string{},
			}),
		},
	),
}

var CreateIndexOp12 = &migrations.OpCreateIndex{
	Name:  "idx_name",
	Table: "foo",
	Columns: newColumns("bar", migrations.IndexField{
		Sort: migrations.IndexFieldSortASC,
	}),
	Method: migrations.OpCreateIndexMethodBtree,
}

func CreateIndexOpWithStorageParam(param string) *migrations.OpCreateIndex {
	return &migrations.OpCreateIndex{
		Name:              "idx_name",
		Table:             "foo",
		Columns:           newColumns("bar", migrations.IndexField{}),
		Method:            migrations.OpCreateIndexMethodBtree,
		StorageParameters: param,
	}
}
