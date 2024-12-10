package expect

import (
	"github.com/xataio/pgroll/pkg/migrations"
	"github.com/xataio/pgroll/pkg/sql2pgroll"
)

var DropColumnOp1 = &migrations.OpDropColumn{
	Table:  "foo",
	Column: "bar",
	Down:   sql2pgroll.PlaceHolderSQL,
}
