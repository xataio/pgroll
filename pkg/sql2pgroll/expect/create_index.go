// SPDX-License-Identifier: Apache-2.0

package expect

import (
	"github.com/xataio/pgroll/pkg/migrations"
)

var CreateIndexOp1 = &migrations.OpCreateIndex{
	Name:    "idx_name",
	Table:   "foo",
	Columns: map[string]migrations.IndexElemSettings{"bar": {}},
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
		Columns: map[string]migrations.IndexElemSettings{"bar": {}},
		Method:  parsed,
	}
}

var CreateIndexOp2 = &migrations.OpCreateIndex{
	Name:    "idx_name",
	Table:   "schema.foo",
	Columns: map[string]migrations.IndexElemSettings{"bar": {}},
	Method:  migrations.OpCreateIndexMethodBtree,
}

var CreateIndexOp3 = &migrations.OpCreateIndex{
	Name:    "idx_name",
	Table:   "foo",
	Columns: map[string]migrations.IndexElemSettings{"bar": {}, "baz": {}},
	Method:  migrations.OpCreateIndexMethodBtree,
}

var CreateIndexOp4 = &migrations.OpCreateIndex{
	Name:    "idx_name",
	Table:   "foo",
	Columns: map[string]migrations.IndexElemSettings{"bar": {}},
	Method:  migrations.OpCreateIndexMethodBtree,
	Unique:  true,
}

var CreateIndexOp5 = &migrations.OpCreateIndex{
	Name:      "idx_name",
	Table:     "foo",
	Columns:   map[string]migrations.IndexElemSettings{"bar": {}},
	Method:    migrations.OpCreateIndexMethodBtree,
	Predicate: "foo > 0",
}

var CreateIndexOp6 = &migrations.OpCreateIndex{
	Name:    "idx_name",
	Table:   "foo",
	Method:  migrations.OpCreateIndexMethodBtree,
	Columns: map[string]migrations.IndexElemSettings{"bar": {Collate: "en_us"}},
}

var CreateIndexOp7 = &migrations.OpCreateIndex{
	Name:    "idx_name",
	Table:   "foo",
	Method:  migrations.OpCreateIndexMethodBtree,
	Columns: map[string]migrations.IndexElemSettings{"bar": {Sort: migrations.IndexElemSettingsSortDESC}},
}

var CreateIndexOp8 = &migrations.OpCreateIndex{
	Name:    "idx_name",
	Table:   "foo",
	Method:  migrations.OpCreateIndexMethodBtree,
	Columns: map[string]migrations.IndexElemSettings{"bar": {Nulls: ptr(migrations.IndexElemSettingsNullsFIRST)}},
}

var CreateIndexOp9 = &migrations.OpCreateIndex{
	Name:    "idx_name",
	Table:   "foo",
	Method:  migrations.OpCreateIndexMethodBtree,
	Columns: map[string]migrations.IndexElemSettings{"bar": {Nulls: ptr(migrations.IndexElemSettingsNullsLAST)}},
}

var CreateIndexOp10 = &migrations.OpCreateIndex{
	Name:    "idx_name",
	Table:   "foo",
	Method:  migrations.OpCreateIndexMethodBtree,
	Columns: map[string]migrations.IndexElemSettings{"bar": {Opclass: ptr(migrations.IndexElemSettingsOpclass{Name: "opclass", Params: map[string]any{"test": "test"}})}},
}

var CreateIndexOp11 = &migrations.OpCreateIndex{
	Name:   "idx_name",
	Table:  "foo",
	Method: migrations.OpCreateIndexMethodBtree,
	Columns: map[string]migrations.IndexElemSettings{
		"bar": {
			Opclass: ptr(migrations.IndexElemSettingsOpclass{
				Name:   "opclass1",
				Params: map[string]any{},
			}),
		},
		"baz": {
			Opclass: ptr(migrations.IndexElemSettingsOpclass{
				Name:   "opclass2",
				Params: map[string]any{},
			}),
		},
	},
}

var CreateIndexOp12 = &migrations.OpCreateIndex{
	Name:    "idx_name",
	Table:   "foo",
	Columns: map[string]migrations.IndexElemSettings{"bar": {Sort: migrations.IndexElemSettingsSortASC}},
	Method:  migrations.OpCreateIndexMethodBtree,
}

func CreateIndexOpWithStorageParam(param string) *migrations.OpCreateIndex {
	return &migrations.OpCreateIndex{
		Name:              "idx_name",
		Table:             "foo",
		Columns:           map[string]migrations.IndexElemSettings{"bar": {}},
		Method:            migrations.OpCreateIndexMethodBtree,
		StorageParameters: param,
	}
}
