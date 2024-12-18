// SPDX-License-Identifier: Apache-2.0

package sql2pgroll

import (
	"fmt"

	pgq "github.com/xataio/pg_query_go/v6"

	"github.com/xataio/pgroll/pkg/migrations"
)

// convertCreateIndexStmt converts CREATE INDEX statements into pgroll operations.
func convertCreateIndexStmt(stmt *pgq.IndexStmt) (migrations.Operations, error) {
	if !canConvertCreateIndexStmt(stmt) {
		return nil, nil
	}

	// Get the qualified table name
	tableName := getQualifiedRelationName(stmt.GetRelation())
	var columns []string

	// Get the columns on which the index is defined
	for _, param := range stmt.GetIndexParams() {
		if colName := param.GetIndexElem().GetName(); colName != "" {
			columns = append(columns, colName)
		}
	}

	// Parse the access method
	method, err := migrations.ParseCreateIndexMethod(stmt.GetAccessMethod())
	if err != nil {
		return nil, fmt.Errorf("parse create index method: %w", err)
	}

	// Get index uniqueness
	unique := false
	if stmt.GetUnique() {
		unique = true
	}

	// Deparse WHERE clause
	var predicate string
	if where := stmt.GetWhereClause(); where != nil {
		predicate, err = pgq.DeparseExpr(where)
		if err != nil {
			return nil, fmt.Errorf("parsing where clause: %w", err)
		}
	}

	// Deparse storage parameters
	var storageParams string
	if len(stmt.GetOptions()) > 0 {
		storageParams, err = pgq.DeparseRelOptions(stmt.GetOptions())
		if err != nil {
			return nil, fmt.Errorf("parsing options: %w", err)
		}
		// strip outer parentheses
		storageParams = storageParams[1 : len(storageParams)-1]
	}

	return migrations.Operations{
		&migrations.OpCreateIndex{
			Table:             tableName,
			Columns:           columns,
			Name:              stmt.GetIdxname(),
			Method:            method,
			Unique:            unique,
			Predicate:         predicate,
			StorageParameters: storageParams,
		},
	}, nil
}

func canConvertCreateIndexStmt(stmt *pgq.IndexStmt) bool {
	// Tablespaces are not supported
	if stmt.GetTableSpace() != "" {
		return false
	}
	// Indexes with INCLUDE are not supported
	if stmt.GetIndexIncludingParams() != nil {
		return false
	}
	// Indexes created with ONLY are not supported
	if !stmt.GetRelation().GetInh() {
		return false
	}
	// Indexes with NULLS NOT DISTINCT are not supported
	if stmt.GetNullsNotDistinct() {
		return false
	}
	// IF NOT EXISTS is unsupported
	if stmt.GetIfNotExists() {
		return false
	}
	// Indexes defined on expressions are not supported
	for _, node := range stmt.GetIndexParams() {
		if node.GetIndexElem().GetExpr() != nil {
			return false
		}
	}

	for _, param := range stmt.GetIndexParams() {
		// Indexes with non-default collations are not supported
		if param.GetIndexElem().GetCollation() != nil {
			return false
		}
		// Indexes with non-default ordering are not supported
		ordering := param.GetIndexElem().GetOrdering()
		if ordering != pgq.SortByDir_SORTBY_DEFAULT && ordering != pgq.SortByDir_SORTBY_ASC {
			return false
		}
		// Indexes with non-default nulls ordering are not supported
		if param.GetIndexElem().GetNullsOrdering() != pgq.SortByNulls_SORTBY_NULLS_DEFAULT {
			return false
		}
		// Indexes with opclasses are not supported
		if param.GetIndexElem().GetOpclass() != nil || param.GetIndexElem().GetOpclassopts() != nil {
			return false
		}
	}

	return true
}
