// SPDX-License-Identifier: Apache-2.0

package sql2pgroll

import (
	"fmt"

	pgq "github.com/xataio/pg_query_go/v6"

	"github.com/xataio/pgroll/pkg/migrations"
)

// convertCreateIndexStmt converts CREATE INDEX statements into pgroll operations.
func convertCreateIndexStmt(stmt *pgq.IndexStmt) (migrations.Operations, error) {
	tableName := getQualifiedRelationName(stmt.GetRelation())
	var columns []string

	for _, param := range stmt.GetIndexParams() {
		if colName := param.GetIndexElem().GetName(); colName != "" {
			columns = append(columns, colName)
		}
	}
	method, err := migrations.ParseCreateIndexMethod(stmt.GetAccessMethod())
	if err != nil {
		return nil, fmt.Errorf("parse create index method: %w", err)
	}

	return migrations.Operations{
		&migrations.OpCreateIndex{
			Table:   tableName,
			Columns: columns,
			Name:    stmt.GetIdxname(),
			Method:  &method,
		},
	}, nil
}
