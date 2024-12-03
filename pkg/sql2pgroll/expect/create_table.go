// SPDX-License-Identifier: Apache-2.0

package expect

import "github.com/xataio/pgroll/pkg/migrations"

var CreateTableOp1 = &migrations.OpCreateTable{
	Name: "foo",
	Columns: []migrations.Column{
		{
			Name:     "a",
			Type:     "int4",
			Nullable: true,
		},
	},
}

var CreateTableOp2 = &migrations.OpCreateTable{
	Name: "foo",
	Columns: []migrations.Column{
		{
			Name: "a",
			Type: "int4",
		},
	},
}

var CreateTableOp3 = &migrations.OpCreateTable{
	Name: "foo",
	Columns: []migrations.Column{
		{
			Name:     "a",
			Type:     "varchar(255)",
			Nullable: true,
		},
	},
}

var CreateTableOp4 = &migrations.OpCreateTable{
	Name: "foo",
	Columns: []migrations.Column{
		{
			Name:     "a",
			Type:     "numeric(10,2)",
			Nullable: true,
		},
	},
}

var CreateTableOp5 = &migrations.OpCreateTable{
	Name: "foo",
	Columns: []migrations.Column{
		{
			Name:     "a",
			Type:     "int4",
			Nullable: true,
			Unique:   true,
		},
	},
}

var CreateTableOp6 = &migrations.OpCreateTable{
	Name: "foo",
	Columns: []migrations.Column{
		{
			Name: "a",
			Type: "int4",
			Pk:   true,
		},
	},
}

var CreateTableOp7 = &migrations.OpCreateTable{
	Name: "foo",
	Columns: []migrations.Column{
		{
			Name:     "a",
			Type:     "text[]",
			Nullable: true,
		},
	},
}

var CreateTableOp8 = &migrations.OpCreateTable{
	Name: "foo",
	Columns: []migrations.Column{
		{
			Name:     "a",
			Type:     "text[5]",
			Nullable: true,
		},
	},
}

var CreateTableOp9 = &migrations.OpCreateTable{
	Name: "foo",
	Columns: []migrations.Column{
		{
			Name:     "a",
			Type:     "text[5][3]",
			Nullable: true,
		},
	},
}
