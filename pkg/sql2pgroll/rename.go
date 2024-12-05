// SPDX-License-Identifier: Apache-2.0

package sql2pgroll

import (
	pgq "github.com/pganalyze/pg_query_go/v6"
	"github.com/xataio/pgroll/pkg/migrations"
)

func convertRenameStmt(stmt *pgq.RenameStmt) (migrations.Operations, error) {
	if stmt.GetRelationType() != pgq.ObjectType_OBJECT_TABLE {
		return nil, nil
	}
	if stmt.GetRenameType() != pgq.ObjectType_OBJECT_COLUMN {
		return nil, nil
	}

	return migrations.Operations{
		&migrations.OpAlterColumn{
			Table:  stmt.GetRelation().GetRelname(),
			Column: stmt.GetSubname(),
			Name:   ptr(stmt.GetNewname()),
		},
	}, nil
}
