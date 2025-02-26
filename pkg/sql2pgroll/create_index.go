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
	columns := make(map[string]migrations.IndexElemSettings, len(stmt.GetIndexParams()))
	for _, param := range stmt.GetIndexParams() {
		if colName := param.GetIndexElem().GetName(); colName != "" {
			var indexElemSettings migrations.IndexElemSettings
			collate, err := pgq.DeparseAnyName(param.GetIndexElem().GetCollation())
			if err != nil {
				return nil, nil
			}
			indexElemSettings.Collate = collate
			opclassName, err := pgq.DeparseAnyName(param.GetIndexElem().GetOpclass())
			if err != nil {
				return nil, nil
			}
			if opclassName != "" {
				opclassOpts := make(map[string]any, 0)
				opts, err := pgq.DeparseRelOptions(param.GetIndexElem().GetOpclassopts())
				if err != nil {
					return nil, nil
				}
				for _, opt := range strings.Split(opts[1:len(opts)-1], ",") {
					optKV := strings.SplitN(opt, "=", 2)
					if len(optKV) != 2 {
						continue
					}
					opclassOpts[optKV[0]] = optKV[1]
				}
				indexElemSettings.Opclass = &migrations.IndexElemSettingsOpclass{
					Name:   opclassName,
					Params: migrations.IndexElemSettingsOpclassParams(opclassOpts),
				}
			}

			if param.GetIndexElem().GetOrdering() != pgq.SortByDir_SORTBY_DEFAULT {
				switch param.GetIndexElem().GetOrdering() {
				case pgq.SortByDir_SORTBY_ASC:
					indexElemSettings.Sort = migrations.IndexElemSettingsSortASC
				case pgq.SortByDir_SORTBY_DESC:
					indexElemSettings.Sort = migrations.IndexElemSettingsSortDESC
				default:
					return nil, nil
				}
			}
			if param.GetIndexElem().GetNullsOrdering() != pgq.SortByNulls_SORTBY_NULLS_DEFAULT {
				switch param.GetIndexElem().GetNullsOrdering() {
				case pgq.SortByNulls_SORTBY_NULLS_FIRST:
					indexElemSettings.Nulls = ptr(migrations.IndexElemSettingsNullsFIRST)
				case pgq.SortByNulls_SORTBY_NULLS_LAST:
					indexElemSettings.Nulls = ptr(migrations.IndexElemSettingsNullsLAST)
				default:
					return nil, nil
				}
			}

			columns[colName] = indexElemSettings
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

	return true
}
