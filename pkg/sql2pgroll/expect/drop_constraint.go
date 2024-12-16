// SPDX-License-Identifier: Apache-2.0

package expect

import (
	"github.com/xataio/pgroll/pkg/migrations"
	"github.com/xataio/pgroll/pkg/sql2pgroll"
)

var OpDropConstraint1 = &migrations.OpDropMultiColumnConstraint{
	Up: migrations.MultiColumnUpSQL{
		"placeholder": sql2pgroll.PlaceHolderSQL,
	},
	Down: migrations.MultiColumnDownSQL{
		"placeholder": sql2pgroll.PlaceHolderSQL,
	},
	Table: "foo",
	Name:  "constraint_foo",
}
