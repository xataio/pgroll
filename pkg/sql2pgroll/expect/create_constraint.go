// SPDX-License-Identifier: Apache-2.0

package expect

import (
	"github.com/xataio/pgroll/pkg/migrations"
	"github.com/xataio/pgroll/pkg/sql2pgroll"
)

var CreateConstraintOp1 = &migrations.OpCreateConstraint{
	Type:    migrations.OpCreateConstraintTypeUnique,
	Name:    "bar",
	Table:   "foo",
	Columns: []string{"a"},
	Down:    map[string]string{"a": sql2pgroll.PlaceHolderSQL},
	Up:      map[string]string{"a": sql2pgroll.PlaceHolderSQL},
}

var CreateConstraintOp2 = &migrations.OpCreateConstraint{
	Type:    migrations.OpCreateConstraintTypeUnique,
	Name:    "bar",
	Table:   "foo",
	Columns: []string{"a", "b"},
	Down: map[string]string{
		"a": sql2pgroll.PlaceHolderSQL,
		"b": sql2pgroll.PlaceHolderSQL,
	},
	Up: map[string]string{
		"a": sql2pgroll.PlaceHolderSQL,
		"b": sql2pgroll.PlaceHolderSQL,
	},
}

var CreateConstraintOp3 = &migrations.OpCreateConstraint{
	Type:    migrations.OpCreateConstraintTypeCheck,
	Name:    "bar",
	Table:   "foo",
	Check:   ptr("age > 0"),
	Columns: []string{sql2pgroll.PlaceHolderColumnName},
	Up: map[string]string{
		sql2pgroll.PlaceHolderColumnName: sql2pgroll.PlaceHolderSQL,
	},
	Down: map[string]string{
		sql2pgroll.PlaceHolderColumnName: sql2pgroll.PlaceHolderSQL,
	},
}

var CreateConstraintOp4 = &migrations.OpCreateConstraint{
	Type:    migrations.OpCreateConstraintTypeCheck,
	Name:    "bar",
	Table:   "schema.foo",
	Check:   ptr("age > 0"),
	Columns: []string{sql2pgroll.PlaceHolderColumnName},
	Up: map[string]string{
		sql2pgroll.PlaceHolderColumnName: sql2pgroll.PlaceHolderSQL,
	},
	Down: map[string]string{
		sql2pgroll.PlaceHolderColumnName: sql2pgroll.PlaceHolderSQL,
	},
}

var CreateConstraintOp5 = &migrations.OpCreateConstraint{
	Type:      migrations.OpCreateConstraintTypeCheck,
	Name:      "bar",
	Table:     "foo",
	Check:     ptr("age > 0"),
	NoInherit: true,
	Columns:   []string{sql2pgroll.PlaceHolderColumnName},
	Up: map[string]string{
		sql2pgroll.PlaceHolderColumnName: sql2pgroll.PlaceHolderSQL,
	},
	Down: map[string]string{
		sql2pgroll.PlaceHolderColumnName: sql2pgroll.PlaceHolderSQL,
	},
}
