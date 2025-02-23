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

	// Get the columns on which the index is defined
	var elements, columns []string
	if extraIndexElemConfig(stmt.GetIndexParams()) {
		elements = make([]string, len(stmt.GetIndexParams()))
		for i, param := range stmt.GetIndexParams() {
			indexElem, err := pgq.DeparseIndexElem(param)
			if err != nil {
				return nil, fmt.Errorf("parsing index element: %w", err)
			}
			elements[i] = indexElem
		}
	} else {
		columns = make([]string, len(stmt.GetIndexParams()))
		for i, param := range stmt.GetIndexParams() {
			if colName := param.GetIndexElem().GetName(); colName != "" {
				columns[i] = colName
			}
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
			Elements:          elements,
		},
	}, nil
}

func extraIndexElemConfig(elems []*pgq.Node) bool {
	for _, param := range elems {
		if param.GetIndexElem().GetCollation() != nil {
			return true
		}
		ordering := param.GetIndexElem().GetOrdering()
		if ordering != pgq.SortByDir_SORTBY_DEFAULT && ordering != pgq.SortByDir_SORTBY_ASC {
			return true
		}
		if param.GetIndexElem().GetNullsOrdering() != pgq.SortByNulls_SORTBY_NULLS_DEFAULT {
			return true
		}
		if param.GetIndexElem().GetOpclass() != nil || param.GetIndexElem().GetOpclassopts() != nil {
			return true
		}
	}
	return false
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

	return true
}
