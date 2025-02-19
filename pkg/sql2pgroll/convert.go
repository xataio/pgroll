// SPDX-License-Identifier: Apache-2.0

package sql2pgroll

import (
	"fmt"

	pgq "github.com/xataio/pg_query_go/v6"

	"github.com/xataio/pgroll/pkg/migrations"
)

// Convert converts a SQL statement to a slice of pgroll operations.
func Convert(sql string) (migrations.Operations, error) {
	tree, err := pgq.Parse(sql)
	if err != nil {
		return nil, fmt.Errorf("parse error: %w", err)
	}

	var migOps migrations.Operations
	stmts := tree.GetStmts()
	for i, stmt := range stmts {
		if stmt.GetStmt() == nil {
			continue
		}
		node := stmts[i].GetStmt().GetNode()
		var ops migrations.Operations
		var err error
		switch node := (node).(type) {
		case *pgq.Node_CreateStmt:
			ops, err = convertCreateStmt(node.CreateStmt)
		case *pgq.Node_AlterTableStmt:
			ops, err = convertAlterTableStmt(node.AlterTableStmt)
		case *pgq.Node_RenameStmt:
			ops, err = convertRenameStmt(node.RenameStmt)
		case *pgq.Node_DropStmt:
			ops, err = convertDropStatement(node.DropStmt)
		case *pgq.Node_IndexStmt:
			ops, err = convertCreateIndexStmt(node.IndexStmt)
		default:
			// SQL statement cannot be transformed to pgroll operation
			// so we will use raw SQL operation
			ops = makeRawSQLOperation(sql, i)
		}
		if err != nil {
			return nil, err
		}
		if ops == nil {
			ops = makeRawSQLOperation(sql, i)
		}
		migOps = append(migOps, ops...)
	}
	return migOps, nil
}

func makeRawSQLOperation(sql string, idx int) migrations.Operations {
	stmts, err := pgq.SplitWithParser(sql, true)
	if err != nil {
		return migrations.Operations{
			&migrations.OpRawSQL{Up: sql},
		}
	}
	return migrations.Operations{
		&migrations.OpRawSQL{Up: stmts[idx]},
	}
}
