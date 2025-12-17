// SPDX-License-Identifier: Apache-2.0

package migrations

import (
	"context"
	"fmt"

	"github.com/lib/pq"

	"github.com/xataio/pgroll/pkg/db"
	"github.com/xataio/pgroll/pkg/schema"
)

var (
	_ Operation  = (*OpCreateIndex)(nil)
	_ Createable = (*OpCreateIndex)(nil)
)

func (o *OpCreateIndex) Start(ctx context.Context, l Logger, conn db.DB, s *schema.Schema) (*StartResult, error) {
	l.LogOperationStart(o)

	// Check for deprecation warnings/errors
	if err := o.checkDeprecation(l); err != nil {
		return nil, err
	}

	table := s.GetTable(o.Table)
	if table == nil {
		return nil, TableDoesNotExistError{Name: o.Table}
	}

	// Build ordered columns array with physical column names
	cols := make([]IndexColumn, 0, len(o.Columns))
	for _, col := range o.Columns {
		physicalName := table.PhysicalColumnNamesFor(col.Name)
		cols = append(cols, IndexColumn{
			Name:    physicalName[0],
			Collate: col.Collate,
			Nulls:   col.Nulls,
			Opclass: col.Opclass,
			Sort:    col.Sort,
		})
	}

	dbActions := []DBAction{
		NewCreateIndexConcurrentlyAction(
			conn,
			table.Name,
			o.Name,
			string(o.Method),
			o.Unique,
			cols,
			o.StorageParameters,
			o.Predicate,
		),
	}

	return &StartResult{Actions: dbActions}, nil
}

func (o *OpCreateIndex) Complete(l Logger, conn db.DB, s *schema.Schema) ([]DBAction, error) {
	l.LogOperationComplete(o)

	// No-op
	return nil, nil
}

func (o *OpCreateIndex) Rollback(l Logger, conn db.DB, s *schema.Schema) ([]DBAction, error) {
	l.LogOperationRollback(o)

	// drop the index concurrently
	return []DBAction{NewDropIndexAction(conn, o.Name)}, nil
}

func (o *OpCreateIndex) Validate(ctx context.Context, s *schema.Schema) error {
	if o.Name == "" {
		return FieldRequiredError{Name: "name"}
	}

	if err := ValidateIdentifierLength(o.Name); err != nil {
		return err
	}

	if len(o.Columns) == 0 {
		return FieldRequiredError{Name: "columns"}
	}

	table := s.GetTable(o.Table)
	if table == nil {
		return TableDoesNotExistError{Name: o.Table}
	}

	// Validate column existence
	for _, col := range o.Columns {
		if table.GetColumn(col.Name) == nil {
			return ColumnDoesNotExistError{Table: o.Table, Name: col.Name}
		}
	}

	// Index names must be unique across the entire schema.
	for _, table := range s.Tables {
		_, ok := table.Indexes[o.Name]
		if ok {
			return IndexAlreadyExistsError{Name: o.Name}
		}
	}

	return nil
}

func quoteColumnNames(columns []string) (quoted []string) {
	for _, col := range columns {
		quoted = append(quoted, pq.QuoteIdentifier(col))
	}
	return quoted
}

// ParseCreateIndexMethod parsed index methods into OpCreateIndexMethod
func ParseCreateIndexMethod(method string) (OpCreateIndexMethod, error) {
	switch method {
	case "btree":
		return OpCreateIndexMethodBtree, nil
	case "hash":
		return OpCreateIndexMethodHash, nil
	case "gist":
		return OpCreateIndexMethodGist, nil
	case "spgist":
		return OpCreateIndexMethodSpgist, nil
	case "gin":
		return OpCreateIndexMethodGin, nil
	case "brin":
		return OpCreateIndexMethodBrin, nil
	default:
		return OpCreateIndexMethodBtree, fmt.Errorf("unknown method: %s", method)
	}
}
