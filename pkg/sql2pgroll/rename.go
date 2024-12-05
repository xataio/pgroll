// SPDX-License-Identifier: Apache-2.0

package sql2pgroll

import (
	pgq "github.com/pganalyze/pg_query_go/v6"
	"github.com/xataio/pgroll/pkg/migrations"
)

func convertRenameStmt(stmt *pgq.RenameStmt) (migrations.Operations, error) {
	switch stmt.GetRenameType() {
	case pgq.ObjectType_OBJECT_TABLE:
		return convertRenameTable(stmt)
	case pgq.ObjectType_OBJECT_COLUMN:
		return convertRenameColumn(stmt)
	}

	return nil, nil
}

func convertRenameColumn(stmt *pgq.RenameStmt) (migrations.Operations, error) {
	return migrations.Operations{
		&migrations.OpAlterColumn{
			Table:  stmt.GetRelation().GetRelname(),
			Column: stmt.GetSubname(),
			Name:   ptr(stmt.GetNewname()),
		},
	}, nil
}

func convertRenameTable(stmt *pgq.RenameStmt) (migrations.Operations, error) {
	return migrations.Operations{
		&migrations.OpRenameTable{
			From: stmt.GetRelation().GetRelname(),
			To:   stmt.GetNewname(),
		},
	}, nil
}
