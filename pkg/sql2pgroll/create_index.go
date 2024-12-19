// SPDX-License-Identifier: Apache-2.0

package sql2pgroll

import (
	"fmt"
	"strconv"
	"strings"

	pgq "github.com/xataio/pg_query_go/v6"

	"github.com/xataio/pgroll/pkg/migrations"
)

// convertCreateIndexStmt converts CREATE INDEX statements into pgroll operations.
func convertCreateIndexStmt(stmt *pgq.IndexStmt) (migrations.Operations, error) {
	if !canConvertCreateIndexStmt(stmt) {
		return nil, nil
	}

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

	var unique *bool
	if stmt.GetUnique() {
		unique = ptr(true)
	}

	var predicate *string
	if where := stmt.GetWhereClause(); where != nil {
		deparsed, err := pgq.DeparseExpr(where)
		if err != nil {
			return nil, fmt.Errorf("parsing where clause: %w", err)
		}
		predicate = &deparsed
	}

	var storageParams *string
	for _, option := range stmt.GetOptions() {
		// TODO: It may be easier to deparse this in pgq, but for now it is not supported
		k := option.GetDefElem().GetDefname()
		var v string
		switch k {
		case "fillfactor", "gin_pending_list_limit", "pages_per_range":
			v = strconv.Itoa(int(option.GetDefElem().GetArg().GetInteger().GetIval()))
		case "deduplicate_items", "fastupdate", "autosummarize":
			// Even though it is a boolean, the parser returns it as a string
			v = option.GetDefElem().GetArg().GetString_().GetSval()
		case "buffering":
			// Annoyingly when the value is ON, it comes through as a string. But when the value is AUTO or OFF
			// it is parsed as a type.
			v = option.GetDefElem().GetArg().GetString_().GetSval()
			if v == "" {
				var names []string
				for _, node := range option.GetDefElem().GetArg().GetTypeName().GetNames() {
					names = append(names, node.GetString_().GetSval())
				}
				v = strings.Join(names, " ")
			}
		}
		kv := fmt.Sprintf("%s = %s", k, v)
		storageParams = &kv
	}

	return migrations.Operations{
		&migrations.OpCreateIndex{
			Table:             tableName,
			Columns:           columns,
			Name:              stmt.GetIdxname(),
			Method:            &method,
			Unique:            unique,
			Predicate:         predicate,
			StorageParameters: storageParams,
		},
	}, nil
}

func canConvertCreateIndexStmt(stmt *pgq.IndexStmt) bool {
	if len(stmt.GetOptions()) > 1 {
		return false
	}
	for _, param := range stmt.GetIndexParams() {
		if param.GetIndexElem().GetCollation() != nil {
			return false
		}
		ordering := param.GetIndexElem().GetOrdering()
		if ordering != pgq.SortByDir_SORTBY_DEFAULT && ordering != pgq.SortByDir_SORTBY_ASC {
			return false
		}
		if param.GetIndexElem().GetNullsOrdering() != pgq.SortByNulls_SORTBY_NULLS_DEFAULT {
			return false
		}
	}
	if stmt.GetTableSpace() != "" {
		return false
	}
	if stmt.GetIndexIncludingParams() != nil {
		return false
	}

	return true
}
