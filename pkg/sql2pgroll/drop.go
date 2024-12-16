// SPDX-License-Identifier: Apache-2.0

package sql2pgroll

import (
	"strings"

	pgq "github.com/xataio/pg_query_go/v6"

	"github.com/xataio/pgroll/pkg/migrations"
)

// convertDropStatement converts supported drop statements to pgroll operations
func convertDropStatement(stmt *pgq.DropStmt) (migrations.Operations, error) {
	switch stmt.RemoveType {
	case pgq.ObjectType_OBJECT_INDEX:
		return convertDropIndexStatement(stmt)
	case pgq.ObjectType_OBJECT_TABLE:
		return convertDropTableStatement(stmt)

	}
	return nil, nil
}

// convertDropIndexStatement converts simple DROP INDEX statements to pgroll operations
func convertDropIndexStatement(stmt *pgq.DropStmt) (migrations.Operations, error) {
	if !canConvertDropIndex(stmt) {
		return nil, nil
	}
	items := stmt.GetObjects()[0].GetList().GetItems()
	parts := make([]string, len(items))
	for i, item := range items {
		parts[i] = item.GetString_().GetSval()
	}

	return migrations.Operations{
		&migrations.OpDropIndex{
			Name: strings.Join(parts, "."),
		},
	}, nil
}

// canConvertDropIndex checks whether we can convert the statement without losing any information.
func canConvertDropIndex(stmt *pgq.DropStmt) bool {
	if len(stmt.Objects) > 1 {
		return false
	}
	if stmt.Behavior == pgq.DropBehavior_DROP_CASCADE {
		return false
	}
	return true
}

// convertDropTableStatement converts simple DROP TABLE statements to pgroll operations
func convertDropTableStatement(stmt *pgq.DropStmt) (migrations.Operations, error) {
	if !canConvertDropTable(stmt) {
		return nil, nil
	}

	items := stmt.GetObjects()[0].GetList().GetItems()
	parts := make([]string, len(items))
	for i, item := range items {
		parts[i] = item.GetString_().GetSval()
	}

	return migrations.Operations{
		&migrations.OpDropTable{
			Name: strings.Join(parts, "."),
		},
	}, nil
}

// canConvertDropTable checks whether we can convert the statement without losing any information.
func canConvertDropTable(stmt *pgq.DropStmt) bool {
	return stmt.Behavior != pgq.DropBehavior_DROP_CASCADE
}
