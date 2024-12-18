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

var CreateIndexOp2 = &migrations.OpCreateIndex{
	Name:    "idx_name",
	Table:   "schema.foo",
	Columns: []string{"bar"},
	Method:  ptr(migrations.OpCreateIndexMethodBtree),
}
