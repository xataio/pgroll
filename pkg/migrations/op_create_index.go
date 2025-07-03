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

	table := s.GetTable(o.Table)
	if table == nil {
		return nil, TableDoesNotExistError{Name: o.Table}
	}

	cols := make(map[string]IndexField, len(o.Columns))
	for name, settings := range map[string]IndexField(o.Columns) {
		physicalName := table.PhysicalColumnNamesFor(name)
		cols[physicalName[0]] = settings
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

	table := s.GetTable(o.Table)
	if table == nil {
		return TableDoesNotExistError{Name: o.Table}
	}

	for column := range map[string]IndexField(o.Columns) {
		if table.GetColumn(column) == nil {
			return ColumnDoesNotExistError{Table: o.Table, Name: column}
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
