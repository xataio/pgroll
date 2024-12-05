// SPDX-License-Identifier: Apache-2.0

package sql2pgroll

import (
	"fmt"

	pgq "github.com/pganalyze/pg_query_go/v6"
	"github.com/xataio/pgroll/pkg/migrations"
)

var ErrStatementCount = fmt.Errorf("expected exactly one statement")

// Convert converts a SQL statement to a slice of pgroll operations.
func Convert(sql string) (migrations.Operations, error) {
	ops, err := convert(sql)
	if err != nil {
		return nil, err
	}

	if ops == nil {
		return makeRawSQLOperation(sql), nil
	}

	return ops, nil
}

func convert(sql string) (migrations.Operations, error) {
	tree, err := pgq.Parse(sql)
	if err != nil {
		return nil, fmt.Errorf("parse error: %w", err)
	}

	stmts := tree.GetStmts()
	if len(stmts) != 1 {
		return nil, fmt.Errorf("%w: got %d statements", ErrStatementCount, len(stmts))
	}
	node := stmts[0].GetStmt().GetNode()

	switch node := (node).(type) {
	case *pgq.Node_CreateStmt:
		return convertCreateStmt(node.CreateStmt)
	case *pgq.Node_AlterTableStmt:
		return convertAlterTableStmt(node.AlterTableStmt)
	case *pgq.Node_RenameStmt:
		return convertRenameStmt(node.RenameStmt)
	default:
		return makeRawSQLOperation(sql), nil
	}
}

func makeRawSQLOperation(sql string) migrations.Operations {
	return migrations.Operations{
		&migrations.OpRawSQL{Up: sql},
	}
}
