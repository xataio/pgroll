// SPDX-License-Identifier: Apache-2.0

package sql2pgroll

import (
	"fmt"
	"strings"

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

	// Get the columns and their settings on which the index is defined
	columns := make(map[string]migrations.IndexField, len(stmt.GetIndexParams()))
	for _, param := range stmt.GetIndexParams() {
		if colName := param.GetIndexElem().GetName(); colName != "" {
			var indexField migrations.IndexField
			// Deparse collation name
			collate, err := pgq.DeparseAnyName(param.GetIndexElem().GetCollation())
			if err != nil {
				return nil, nil
			}
			indexField.Collate = collate

			// Deparse operator class name
			opclassName, err := pgq.DeparseAnyName(param.GetIndexElem().GetOpclass())
			if err != nil {
				return nil, nil
			}
			if opclassName != "" {
				// if operator class is set, deparse operator class options as well
				opclassOpts := make([]string, 0)
				opts, err := pgq.DeparseRelOptions(param.GetIndexElem().GetOpclassopts())
				if err != nil {
					return nil, nil
				}
				if opts != "()" {
					for _, opt := range strings.Split(opts[1:len(opts)-1], ",") {
						opclassOpts = append(opclassOpts, strings.TrimSpace(opt))
					}
				}
				indexField.Opclass = &migrations.IndexFieldOpclass{
					Name:   opclassName,
					Params: opclassOpts,
				}
			}

			// Deparse index field sort
			if param.GetIndexElem().GetOrdering() != pgq.SortByDir_SORTBY_DEFAULT {
				switch param.GetIndexElem().GetOrdering() {
				case pgq.SortByDir_SORTBY_ASC:
					indexField.Sort = migrations.IndexFieldSortASC
				case pgq.SortByDir_SORTBY_DESC:
					indexField.Sort = migrations.IndexFieldSortDESC
				default:
					return nil, nil
				}
			}

			// Deparse index field nulls ordering
			if param.GetIndexElem().GetNullsOrdering() != pgq.SortByNulls_SORTBY_NULLS_DEFAULT {
				switch param.GetIndexElem().GetNullsOrdering() {
				case pgq.SortByNulls_SORTBY_NULLS_FIRST:
					indexField.Nulls = ptr(migrations.IndexFieldNullsFIRST)
				case pgq.SortByNulls_SORTBY_NULLS_LAST:
					indexField.Nulls = ptr(migrations.IndexFieldNullsLAST)
				default:
					return nil, nil
				}
			}

			columns[colName] = indexField
		}
	}

	// Parse the access method
	method, err := migrations.ParseCreateIndexMethod(stmt.GetAccessMethod())
	if err != nil {
		return nil, fmt.Errorf("parse create index method: %w", err)
	}

	// Get index uniqueness
	unique := stmt.GetUnique()

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
	// Indexes defined on expressions are not supported
	for _, node := range stmt.GetIndexParams() {
		if node.GetIndexElem().GetExpr() != nil {
			return false
		}
	}

	return true
}
