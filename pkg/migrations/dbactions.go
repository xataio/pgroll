// SPDX-License-Identifier: Apache-2.0

package migrations

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/lib/pq"
	"github.com/xataio/pgroll/pkg/db"
)

// DBAction is an interface for common database actions
// pgroll runs during migrations.
type DBAction interface {
	Execute(context.Context) error
}

// dropColumnAction is a DBAction that drops one or more columns from a table.
type dropColumnAction struct {
	conn db.DB

	table   string
	columns []string
}

func NewDropColumnAction(conn db.DB, table string, columns ...string) *dropColumnAction {
	return &dropColumnAction{
		conn:    conn,
		table:   table,
		columns: columns,
	}
}

func (a *dropColumnAction) Execute(ctx context.Context) error {
	_, err := a.conn.ExecContext(ctx, fmt.Sprintf("ALTER TABLE IF EXISTS %s %s",
		pq.QuoteIdentifier(a.table),
		a.dropMultipleColumns()))
	return err
}

func (a *dropColumnAction) dropMultipleColumns() string {
	cols := make([]string, len(a.columns))
	for i, col := range a.columns {
		cols[i] = "DROP COLUMN IF EXISTS " + pq.QuoteIdentifier(col)
	}
	return strings.Join(cols, ", ")
}

// renameColumnAction is a DBAction that renames a column in a table.
type renameColumnAction struct {
	conn db.DB

	table string
	from  string
	to    string
}

func NewRenameColumnAction(conn db.DB, table, from, to string) *renameColumnAction {
	return &renameColumnAction{
		conn:  conn,
		table: table,
		from:  from,
		to:    to,
	}
}

func (a *renameColumnAction) Execute(ctx context.Context) error {
	_, err := a.conn.ExecContext(ctx, fmt.Sprintf("ALTER TABLE IF EXISTS %s RENAME COLUMN %s TO %s",
		pq.QuoteIdentifier(a.table),
		pq.QuoteIdentifier(a.from),
		pq.QuoteIdentifier(a.to)))
	return err
}

// renameConstraintAction is a DBAction that renames a constraint in a table.
type renameConstraintAction struct {
	conn  db.DB
	table string
	from  string
	to    string
}

func NewRenameConstraintAction(conn db.DB, table, from, to string) *renameConstraintAction {
	return &renameConstraintAction{
		conn:  conn,
		table: table,
		from:  from,
		to:    to,
	}
}

func (a *renameConstraintAction) Execute(ctx context.Context) error {
	_, err := a.conn.ExecContext(ctx, fmt.Sprintf("ALTER TABLE IF EXISTS %s RENAME CONSTRAINT %s TO %s",
		pq.QuoteIdentifier(a.table),
		pq.QuoteIdentifier(a.from),
		pq.QuoteIdentifier(a.to)))
	return err
}

// dropFunctionAction is a DBAction that drops a function and all of its dependencies (cascade).
type dropFunctionAction struct {
	conn      db.DB
	functions []string
}

func NewDropFunctionAction(conn db.DB, functions ...string) *dropFunctionAction {
	return &dropFunctionAction{
		conn:      conn,
		functions: functions,
	}
}

func (a *dropFunctionAction) Execute(ctx context.Context) error {
	functions := make([]string, len(a.functions))
	for idx, fn := range a.functions {
		functions[idx] = pq.QuoteIdentifier(fn)
	}
	_, err := a.conn.ExecContext(ctx, fmt.Sprintf("DROP FUNCTION IF EXISTS %s CASCADE",
		strings.Join(functions, ",")))
	return err
}

// commentColumnAction is a DBAction that adds a comment to a column in a table.
type commentColumnAction struct {
	conn    db.DB
	table   string
	column  string
	comment *string
}

func NewCommentColumnAction(conn db.DB, table, column string, comment *string) *commentColumnAction {
	return &commentColumnAction{
		conn:    conn,
		table:   table,
		column:  column,
		comment: comment,
	}
}

func (a *commentColumnAction) Execute(ctx context.Context) error {
	commentSQL := fmt.Sprintf("COMMENT ON COLUMN %s.%s IS %s",
		pq.QuoteIdentifier(a.table),
		pq.QuoteIdentifier(a.column),
		commentToSQL(a.comment))

	_, err := a.conn.ExecContext(ctx, commentSQL)
	return err
}

// commentTableAction is a DBAction that adds a comment to a table.
type commentTableAction struct {
	conn    db.DB
	table   string
	comment *string
}

func NewCommentTableAction(conn db.DB, table string, comment *string) *commentTableAction {
	return &commentTableAction{
		conn:    conn,
		table:   table,
		comment: comment,
	}
}

func (a *commentTableAction) Execute(ctx context.Context) error {
	commentSQL := fmt.Sprintf("COMMENT ON TABLE %s IS %s",
		pq.QuoteIdentifier(a.table),
		commentToSQL(a.comment))

	_, err := a.conn.ExecContext(ctx, commentSQL)
	return err
}

func commentToSQL(comment *string) string {
	if comment == nil {
		return "NULL"
	}
	return pq.QuoteLiteral(*comment)
}

type createUniqueIndexConcurrentlyAction struct {
	conn        db.DB
	schemaName  string
	indexName   string
	tableName   string
	columnNames []string
}

func NewCreateUniqueIndexConcurrentlyAction(conn db.DB, schemaName, indexName, tableName string, columnNames ...string) *createUniqueIndexConcurrentlyAction {
	return &createUniqueIndexConcurrentlyAction{
		conn:        conn,
		schemaName:  schemaName,
		indexName:   indexName,
		tableName:   tableName,
		columnNames: columnNames,
	}
}

func (a *createUniqueIndexConcurrentlyAction) Execute(ctx context.Context) error {
	quotedQualifiedIndexName := pq.QuoteIdentifier(a.indexName)
	if a.schemaName != "" {
		quotedQualifiedIndexName = fmt.Sprintf("%s.%s", pq.QuoteIdentifier(a.schemaName), pq.QuoteIdentifier(a.indexName))
	}
	for range 5 {
		// Add a unique index to the new column
		// Indexes are created in the same schema with the table automatically. Instead of the qualified one, just pass the index name.
		createIndexSQL := a.getCreateUniqueIndexConcurrentlySQL()
		if _, err := a.conn.ExecContext(ctx, createIndexSQL); err != nil {
			return fmt.Errorf("failed to add unique index %q: %w", a.indexName, err)
		}

		// Make sure Postgres is done creating the index
		isInProgress, err := a.isIndexInProgress(ctx, quotedQualifiedIndexName)
		if err != nil {
			return err
		}

		ticker := time.NewTicker(500 * time.Millisecond)
		defer ticker.Stop()
		for isInProgress {
			<-ticker.C
			isInProgress, err = a.isIndexInProgress(ctx, quotedQualifiedIndexName)
			if err != nil {
				return err
			}
		}

		// Check pg_index to see if it's valid or not. Break if it's valid.
		isValid, err := a.isIndexValid(ctx, quotedQualifiedIndexName)
		if err != nil {
			return err
		}

		if isValid {
			// success
			return nil
		}

		// If not valid, since Postgres has already given up validating the index,
		// it will remain invalid forever. Drop it and try again.
		_, err = a.conn.ExecContext(ctx, fmt.Sprintf("DROP INDEX IF EXISTS %s", quotedQualifiedIndexName))
		if err != nil {
			return fmt.Errorf("failed to drop index: %w", err)
		}
	}

	// ran out of retries, return an error
	return fmt.Errorf("failed to create unique index %q", a.indexName)
}

func (a *createUniqueIndexConcurrentlyAction) getCreateUniqueIndexConcurrentlySQL() string {
	// create unique index concurrently
	qualifiedTableName := pq.QuoteIdentifier(a.tableName)
	if a.schemaName != "" {
		qualifiedTableName = fmt.Sprintf("%s.%s", pq.QuoteIdentifier(a.schemaName), pq.QuoteIdentifier(a.tableName))
	}

	indexQuery := fmt.Sprintf(
		"CREATE UNIQUE INDEX CONCURRENTLY IF NOT EXISTS %s ON %s (%s)",
		pq.QuoteIdentifier(a.indexName),
		qualifiedTableName,
		strings.Join(quoteColumnNames(a.columnNames), ", "),
	)

	return indexQuery
}

func (a *createUniqueIndexConcurrentlyAction) isIndexInProgress(ctx context.Context, quotedQualifiedIndexName string) (bool, error) {
	rows, err := a.conn.QueryContext(ctx, `SELECT EXISTS(
			SELECT * FROM pg_catalog.pg_stat_progress_create_index
			WHERE index_relid = $1::regclass
			)`, quotedQualifiedIndexName)
	if err != nil {
		return false, fmt.Errorf("getting index in progress with name %q: %w", quotedQualifiedIndexName, err)
	}
	if rows == nil {
		// if rows == nil && err != nil, then it means we have queried a `FakeDB`.
		// In that case, we can safely return false.
		return false, nil
	}
	var isInProgress bool
	if err := db.ScanFirstValue(rows, &isInProgress); err != nil {
		return false, fmt.Errorf("scanning index in progress with name %q: %w", quotedQualifiedIndexName, err)
	}

	return isInProgress, nil
}

func (a *createUniqueIndexConcurrentlyAction) isIndexValid(ctx context.Context, quotedQualifiedIndexName string) (bool, error) {
	rows, err := a.conn.QueryContext(ctx, `SELECT indisvalid
		FROM pg_catalog.pg_index
		WHERE indexrelid = $1::regclass`,
		quotedQualifiedIndexName)
	if err != nil {
		return false, fmt.Errorf("getting index with name %q: %w", quotedQualifiedIndexName, err)
	}
	if rows == nil {
		// if rows == nil && err != nil, then it means we have queried a fake db.
		// In that case, we can safely return true.
		return true, nil
	}
	var isValid bool
	if err := db.ScanFirstValue(rows, &isValid); err != nil {
		return false, fmt.Errorf("scanning index with name %q: %w", quotedQualifiedIndexName, err)
	}

	return isValid, nil
}

// createTableAction is a DBAction that creates a table.
type createTableAction struct {
	conn        db.DB
	table       string
	columns     string
	constraints string
}

func NewCreateTableAction(conn db.DB, table, columns, constraints string) *createTableAction {
	return &createTableAction{
		conn:        conn,
		table:       table,
		columns:     columns,
		constraints: constraints,
	}
}

func (a *createTableAction) Execute(ctx context.Context) error {
	_, err := a.conn.ExecContext(ctx, fmt.Sprintf("CREATE TABLE %s (%s %s)",
		pq.QuoteIdentifier(a.table),
		a.columns,
		a.constraints))
	return err
}

// dropIndexAction is a DBAction that drops an index.
type dropIndexAction struct {
	conn db.DB
	name string
}

func NewDropIndexAction(conn db.DB, name string) *dropIndexAction {
	return &dropIndexAction{
		conn: conn,
		name: name,
	}
}

func (a *dropIndexAction) Execute(ctx context.Context) error {
	_, err := a.conn.ExecContext(ctx, fmt.Sprintf("DROP INDEX CONCURRENTLY IF EXISTS %s",
		pq.QuoteIdentifier(a.name)))
	return err
}

// DropTableAction is a DBAction that drops a table.
type DropTableAction struct {
	conn  db.DB
	table string
}

func NewDropTableAction(conn db.DB, table string) *DropTableAction {
	return &DropTableAction{
		conn:  conn,
		table: table,
	}
}

func (a *DropTableAction) Execute(ctx context.Context) error {
	_, err := a.conn.ExecContext(ctx, fmt.Sprintf("DROP TABLE IF EXISTS %s",
		pq.QuoteIdentifier(a.table)))
	return err
}

// validateConstraintAction is a DBAction that validates a constraint in a table.
type validateConstraintAction struct {
	conn       db.DB
	table      string
	constraint string
}

func NewValidateConstraintAction(conn db.DB, table, constraint string) *validateConstraintAction {
	return &validateConstraintAction{
		conn:       conn,
		table:      table,
		constraint: constraint,
	}
}

func (a *validateConstraintAction) Execute(ctx context.Context) error {
	_, err := a.conn.ExecContext(ctx, fmt.Sprintf("ALTER TABLE IF EXISTS %s VALIDATE CONSTRAINT %s",
		pq.QuoteIdentifier(a.table),
		pq.QuoteIdentifier(a.constraint)))
	return err
}

// CreateCheckConstraintAction creates a check constraint on a table.
type CreateCheckConstraintAction struct {
	conn           db.DB
	table          string
	columns        []string
	constraint     string
	check          string
	noInherit      bool
	skipValidation bool
}

func NewCreateCheckConstraintAction(conn db.DB, table, constraint, check string, columns []string, noInherit, skipValidation bool) *CreateCheckConstraintAction {
	return &CreateCheckConstraintAction{
		conn:           conn,
		table:          table,
		columns:        columns,
		check:          check,
		constraint:     constraint,
		noInherit:      noInherit,
		skipValidation: skipValidation,
	}
}

func (a *CreateCheckConstraintAction) Execute(ctx context.Context) error {
	sql := fmt.Sprintf("ALTER TABLE %s ADD ", pq.QuoteIdentifier(a.table))

	writer := &ConstraintSQLWriter{
		Name:           a.constraint,
		SkipValidation: a.skipValidation,
	}
	sql += writer.WriteCheck(rewriteCheckExpression(a.check, a.columns...), a.noInherit)
	_, err := a.conn.ExecContext(ctx, sql)
	return err
}

// In order for the `check` expression to be easy to write, migration authors specify
// the check expression as though it were being applied to the old column,
// On migration start, however, the check is actually applied to the new (temporary)
// column.
// This function naively rewrites the check expression to apply to the new column.
func rewriteCheckExpression(check string, columns ...string) string {
	for _, col := range columns {
		check = strings.ReplaceAll(check, col, TemporaryName(col))
	}
	return check
}

// createFKConstraintAction is a DBAction that creates a new foreign key constraint
type createFKConstraintAction struct {
	conn              db.DB
	table             string
	constraint        string
	columns           []string
	initiallyDeferred bool
	deferrable        bool
	reference         *TableForeignKeyReference
	skipValidation    bool
}

func NewCreateFKConstraintAction(conn db.DB, table, constraint string, columns []string, reference *TableForeignKeyReference, initiallyDeferred, deferrable, skipValidation bool) *createFKConstraintAction {
	return &createFKConstraintAction{
		conn:              conn,
		table:             table,
		constraint:        constraint,
		columns:           columns,
		reference:         reference,
		initiallyDeferred: initiallyDeferred,
		deferrable:        deferrable,
		skipValidation:    skipValidation,
	}
}

func (a *createFKConstraintAction) Execute(ctx context.Context) error {
	sql := fmt.Sprintf("ALTER TABLE %s ADD ", pq.QuoteIdentifier(a.table))
	writer := &ConstraintSQLWriter{
		Name:              a.constraint,
		Columns:           a.columns,
		InitiallyDeferred: a.initiallyDeferred,
		Deferrable:        a.deferrable,
		SkipValidation:    a.skipValidation,
	}
	sql += writer.WriteForeignKey(
		a.reference.Table,
		a.reference.Columns,
		a.reference.OnDelete,
		a.reference.OnUpdate,
		a.reference.OnDeleteSetColumns,
		a.reference.MatchType)

	_, err := a.conn.ExecContext(ctx, sql)
	return err
}

type alterSequenceOwnerAction struct {
	conn  db.DB
	table string
	from  string
	to    string
}

func NewAlterSequenceOwnerAction(conn db.DB, table, from, to string) *alterSequenceOwnerAction {
	return &alterSequenceOwnerAction{
		conn:  conn,
		table: table,
		from:  from,
		to:    to,
	}
}

func (a *alterSequenceOwnerAction) Execute(ctx context.Context) error {
	sequence := getSequenceNameForColumn(ctx, a.conn, a.table, a.from)
	if sequence == "" {
		return nil
	}
	_, err := a.conn.ExecContext(ctx, fmt.Sprintf("ALTER SEQUENCE IF EXISTS %s OWNED BY %s.%s",
		sequence,
		pq.QuoteIdentifier(a.table),
		pq.QuoteIdentifier(a.to),
	))

	return err
}

func getSequenceNameForColumn(ctx context.Context, conn db.DB, tableName, columnName string) string {
	var sequenceName string
	query := fmt.Sprintf(`
		SELECT pg_get_serial_sequence('%s', '%s')
	`, pq.QuoteIdentifier(tableName), columnName)
	rows, err := conn.QueryContext(ctx, query)
	if err != nil {
		return ""
	}
	defer rows.Close()

	if err := db.ScanFirstValue(rows, &sequenceName); err != nil {
		return ""
	}

	return sequenceName
}

type dropConstraintAction struct {
	conn       db.DB
	table      string
	constraint string
}

func NewDropConstraintAction(conn db.DB, table, constraint string) *dropConstraintAction {
	return &dropConstraintAction{
		conn:       conn,
		table:      table,
		constraint: constraint,
	}
}

func (a *dropConstraintAction) Execute(ctx context.Context) error {
	_, err := a.conn.ExecContext(ctx, fmt.Sprintf("ALTER TABLE IF EXISTS %s DROP CONSTRAINT IF EXISTS %s",
		pq.QuoteIdentifier(a.table),
		pq.QuoteIdentifier(a.constraint)))
	return err
}

type setNotNullAction struct {
	conn   db.DB
	table  string
	column string
}

func NewSetNotNullAction(conn db.DB, table, column string) *setNotNullAction {
	return &setNotNullAction{
		conn:   conn,
		table:  table,
		column: column,
	}
}

func (a *setNotNullAction) Execute(ctx context.Context) error {
	_, err := a.conn.ExecContext(ctx, fmt.Sprintf("ALTER TABLE IF EXISTS %s ALTER COLUMN %s SET NOT NULL",
		pq.QuoteIdentifier(a.table),
		pq.QuoteIdentifier(a.column)))
	return err
}
