// SPDX-License-Identifier: Apache-2.0

package sql2pgroll

import (
	pgq "github.com/pganalyze/pg_query_go/v6"

	"github.com/xataio/pgroll/pkg/migrations"
)

// convertRenameStmt converts RenameStmt nodes to pgroll operations.
func convertRenameStmt(stmt *pgq.RenameStmt) (migrations.Operations, error) {
	switch stmt.GetRenameType() {
	case pgq.ObjectType_OBJECT_TABLE:
		return convertRenameTable(stmt)
	case pgq.ObjectType_OBJECT_COLUMN:
		return convertRenameColumn(stmt)
	case pgq.ObjectType_OBJECT_TABCONSTRAINT:
		return convertRenameConstraint(stmt)
	default:
		return nil, nil
	}
}

// convertRenameColumn converts SQL statements like:
//
// `ALTER TABLE foo RENAME COLUMN a TO b`
// `ALTER TABLE foo RENAME a TO b`
//
// to an OpAlterColumn operation.
func convertRenameColumn(stmt *pgq.RenameStmt) (migrations.Operations, error) {
	return migrations.Operations{
		&migrations.OpAlterColumn{
			Table:  stmt.GetRelation().GetRelname(),
			Column: stmt.GetSubname(),
			Name:   ptr(stmt.GetNewname()),
		},
	}, nil
}

// convertRenameTable converts SQL statements like:
//
// `ALTER TABLE foo RENAME TO bar`
//
// to an OpRenameTable operation.
func convertRenameTable(stmt *pgq.RenameStmt) (migrations.Operations, error) {
	return migrations.Operations{
		&migrations.OpRenameTable{
			From: stmt.GetRelation().GetRelname(),
			To:   stmt.GetNewname(),
		},
	}, nil
}

// convertRenameConstraint converts SQL statements like:
//
// `ALTER TABLE foo RENAME CONSTRAINT a TO b`
//
// to an OpRenameConstraint operation.
func convertRenameConstraint(stmt *pgq.RenameStmt) (migrations.Operations, error) {
	return migrations.Operations{
		&migrations.OpRenameConstraint{
			Table: stmt.GetRelation().GetRelname(),
			From:  stmt.GetSubname(),
			To:    stmt.GetNewname(),
		},
	}, nil
}
