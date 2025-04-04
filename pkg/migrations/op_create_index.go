// SPDX-License-Identifier: Apache-2.0

package migrations

import (
	"context"
	"fmt"
	"strings"

	"github.com/lib/pq"

	"github.com/xataio/pgroll/pkg/db"
	"github.com/xataio/pgroll/pkg/schema"
)

var _ Operation = (*OpCreateIndex)(nil)

func (o *OpCreateIndex) Start(ctx context.Context, conn db.DB, latestSchema string, s *schema.Schema) (*schema.Table, error) {
	table := s.GetTable(o.Table)
	if table == nil {
		return nil, TableDoesNotExistError{Name: o.Table}
	}

	// create index concurrently
	stmtFmt := "CREATE INDEX CONCURRENTLY %s ON %s"
	if o.Unique {
		stmtFmt = "CREATE UNIQUE INDEX CONCURRENTLY %s ON %s"
	}
	stmt := fmt.Sprintf(stmtFmt,
		pq.QuoteIdentifier(o.Name),
		pq.QuoteIdentifier(table.Name))

	if o.Method != "" {
		stmt += fmt.Sprintf(" USING %s", string(o.Method))
	}

	colSQLs := make([]string, 0, len(o.Columns))
	for columnName, settings := range map[string]IndexField(o.Columns) {
		physicalName := table.PhysicalColumnNamesFor(columnName)
		colSQL := pq.QuoteIdentifier(physicalName[0])
		// deparse collations
		if settings.Collate != "" {
			colSQL += " COLLATE " + settings.Collate
		}
		// deparse operator classes and their parameters
		if settings.Opclass != nil {
			colSQL += " " + settings.Opclass.Name
			if len(settings.Opclass.Params) > 0 {
				colSQL += " " + strings.Join(settings.Opclass.Params, ", ")
			}
		}
		// deparse sort order of the index column
		if settings.Sort != "" {
			colSQL += " " + string(settings.Sort)
		}
		// deparse nulls order of the index column
		if settings.Nulls != nil {
			colSQL += " " + string(*settings.Nulls)
		}
		colSQLs = append(colSQLs, colSQL)
	}
	stmt += fmt.Sprintf(" (%s)", strings.Join(colSQLs, ", "))

	if o.Unique && o.NullsNotDistinct {
		stmt += " NULLS NOT DISTINCT"
	}

	if o.StorageParameters != "" {
		stmt += fmt.Sprintf(" WITH (%s)", o.StorageParameters)
	}

	if o.Predicate != "" {
		stmt += fmt.Sprintf(" WHERE %s", o.Predicate)
	}

	_, err := conn.ExecContext(ctx, stmt)
	return nil, err
}

func (o *OpCreateIndex) Complete(ctx context.Context, conn db.DB, s *schema.Schema) error {
	// No-op
	return nil
}

func (o *OpCreateIndex) Rollback(ctx context.Context, conn db.DB, s *schema.Schema) error {
	// drop the index concurrently
	_, err := conn.ExecContext(ctx, fmt.Sprintf("DROP INDEX CONCURRENTLY IF EXISTS %s",
		pq.QuoteIdentifier(o.Name)))

	return err
}

func (o *OpCreateIndex) Validate(ctx context.Context, s *schema.Schema) error {
	if o.Name == "" {
		return FieldRequiredError{Name: "name"}
	}

	if err := ValidateIdentifierLength(o.Name); err != nil {
		return err
	}

	if o.NullsNotDistinct && !o.Unique {
		return NullsNotDistinctOnNotUniqueIndexError{Name: o.Name}
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
