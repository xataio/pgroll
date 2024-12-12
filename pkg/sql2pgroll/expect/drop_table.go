package expect

import (
	"github.com/xataio/pgroll/pkg/migrations"
)

var DropTableOp1 = &migrations.OpDropTable{
	Name: "foo",
}

var DropTableOp2 = &migrations.OpDropTable{
	Name: "foo.bar",
}
