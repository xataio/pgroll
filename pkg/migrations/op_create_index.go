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

func (o *OpCreateIndex) Start(ctx context.Context, conn db.DB, latestSchema string, tr SQLTransformer, s *schema.Schema, cbs ...CallbackFn) (*schema.Table, error) {
	// check if table creation is completed
	fmt.Println("OpCreateIndex Start")
	fmt.Println("Table Name: ", o.Table)
	tableName, ok := s.GetTemporaryResourceName(o.Table)
	if !ok {
		fmt.Println("Table does not exist")
		tableName = o.Table
	}
	fmt.Println("Table Name: ", tableName)

	// create index concurrently
	stmt := fmt.Sprintf("CREATE INDEX CONCURRENTLY %s ON %s",
		pq.QuoteIdentifier(o.Name),
		pq.QuoteIdentifier(tableName))

	if o.Method != nil {
		stmt += fmt.Sprintf(" USING %s", string(*o.Method))
	}

	stmt += fmt.Sprintf(" (%s)", strings.Join(quoteColumnNames(o.Columns), ", "))

	if o.StorageParameters != nil {
		stmt += fmt.Sprintf(" WITH (%s)", *o.StorageParameters)
	}

	if o.Predicate != nil {
		stmt += fmt.Sprintf(" WHERE %s", *o.Predicate)
	}

	_, err := conn.ExecContext(ctx, stmt)
	return nil, err
}

func (o *OpCreateIndex) Complete(ctx context.Context, conn db.DB, tr SQLTransformer, s *schema.Schema) error {
	// No-op
	return nil
}

func (o *OpCreateIndex) Rollback(ctx context.Context, conn db.DB, tr SQLTransformer) error {
	// drop the index concurrently
	_, err := conn.ExecContext(ctx, fmt.Sprintf("DROP INDEX CONCURRENTLY IF EXISTS %s",
		pq.QuoteIdentifier(o.Name)))

	return err
}

func (o *OpCreateIndex) Validate(ctx context.Context, s *schema.Schema) error {
	fmt.Println("OpCreateIndex Validate")
	if o.Name == "" {
		return FieldRequiredError{Name: "name"}
	}

	if err := ValidateIdentifierLength(o.Name); err != nil {
		return err
	}

	table := s.GetTable(o.Table)
	if table == nil {
		tableName, ok := s.GetTemporaryResourceName(o.Table)
		if !ok {
			return TableDoesNotExistError{Name: o.Table}
		}
		table = s.GetTable(tableName)
	}

	for _, column := range o.Columns {
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

func (o *OpCreateIndex) DeriveSchema(ctx context.Context, s *schema.Schema) error {
	// TODO
	//s.GetTable(o.Table).Indexes[o.Name] = schema.Index{
	//	Name:      o.Name,
	//	Columns:   o.Columns,
	//	Predicate: o.Predicate,
	//}
	return nil
}

func quoteColumnNames(columns []string) (quoted []string) {
	for _, col := range columns {
		quoted = append(quoted, pq.QuoteIdentifier(col))
	}
	return quoted
}
