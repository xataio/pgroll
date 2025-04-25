package migrations

import (
	"context"

	"github.com/xataio/pgroll/pkg/db"
	"github.com/xataio/pgroll/pkg/schema"
)

var _ Operation = (*OpCreateIndex)(nil)

func (o *OpCreateIndex) Start(ctx context.Context, conn db.DB, latestSchema string, s *schema.Schema) (*schema.Table, error) {
	table := s.GetTable(o.Table)
	if table == nil {
		return nil, TableDoesNotExistError{Name: o.Table}
	}

	createIndex := NewCreateIndexAction(conn, table.Name, o.Name, o.Columns, o.Method, o.Unique, o.StorageParameters, o.Predicate)
	err := createIndex.Execute(ctx)
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
