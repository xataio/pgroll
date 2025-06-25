// SPDX-License-Identifier: Apache-2.0

package migrations

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/lib/pq"

	"github.com/xataio/pgroll/pkg/db"
)

// DBAction is an interface for common database actions
// pgroll runs during migrations.
type DBAction interface {
	ID() string
	Execute(context.Context) error
}

type addColumnAction struct {
	conn   db.DB
	id     string
	table  string
	column Column
	withPK bool
}

func NewAddColumnAction(conn db.DB, table string, c Column, withPK bool) *addColumnAction {
	return &addColumnAction{
		conn:   conn,
		id:     fmt.Sprintf("add_column_%s_%s", table, c.Name),
		table:  table,
		column: c,
	}
}

func (a *addColumnAction) ID() string { return a.id }

func (a *addColumnAction) Execute(ctx context.Context) error {
	colSQL, err := ColumnSQLWriter{WithPK: a.withPK}.Write(a.column)
	if err != nil {
		return err
	}

	_, err = a.conn.ExecContext(ctx, fmt.Sprintf("ALTER TABLE %s ADD COLUMN %s",
		pq.QuoteIdentifier(a.table),
		colSQL,
	))
	return err
}

// dropColumnAction is a DBAction that drops one or more columns from a table.
type dropColumnAction struct {
	conn    db.DB
	id      string
	table   string
	columns []string
}

func NewDropColumnAction(conn db.DB, table string, columns ...string) *dropColumnAction {
	return &dropColumnAction{
		conn:    conn,
		id:      fmt.Sprintf("drop_column_%s_%s", table, strings.Join(columns, "_")),
		table:   table,
		columns: columns,
	}
}

func (a *dropColumnAction) ID() string { return a.id }

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

// renameTableAction is a DBAction that renames a table.
type renameTableAction struct {
	conn db.DB
	id   string
	from string
	to   string
}

func NewRenameTableAction(conn db.DB, from, to string) *renameTableAction {
	return &renameTableAction{
		conn: conn,
		id:   fmt.Sprintf("rename_table_%s_to_%s", from, to),
		from: from,
		to:   to,
	}
}

func (a *renameTableAction) ID() string { return a.id }

func (a *renameTableAction) Execute(ctx context.Context) error {
	_, err := a.conn.ExecContext(ctx, fmt.Sprintf("ALTER TABLE IF EXISTS %s RENAME TO %s",
		pq.QuoteIdentifier(a.from),
		pq.QuoteIdentifier(a.to)))
	return err
}

// renameColumnAction is a DBAction that renames a column in a table.
type renameColumnAction struct {
	conn  db.DB
	id    string
	table string
	from  string
	to    string
}

func NewRenameColumnAction(conn db.DB, table, from, to string) *renameColumnAction {
	return &renameColumnAction{
		conn:  conn,
		id:    fmt.Sprintf("rename_column_%s_%s_to_%s", table, from, to),
		table: table,
		from:  from,
		to:    to,
	}
}

func (a *renameColumnAction) ID() string { return a.id }

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
	id    string
	table string
	from  string
	to    string
}

func NewRenameConstraintAction(conn db.DB, table, from, to string) *renameConstraintAction {
	return &renameConstraintAction{
		conn:  conn,
		id:    fmt.Sprintf("rename_constraint_%s_%s_to_%s", table, from, to),
		table: table,
		from:  from,
		to:    to,
	}
}

func (a *renameConstraintAction) ID() string { return a.id }

func (a *renameConstraintAction) Execute(ctx context.Context) error {
	_, err := a.conn.ExecContext(ctx, fmt.Sprintf("ALTER TABLE IF EXISTS %s RENAME CONSTRAINT %s TO %s",
		pq.QuoteIdentifier(a.table),
		pq.QuoteIdentifier(a.from),
		pq.QuoteIdentifier(a.to)))
	return err
}

type addConstraintUsingUniqueIndexAction struct {
	conn       db.DB
	id         string
	table      string
	constraint string
	indexName  string
}

func NewAddConstraintUsingUniqueIndex(conn db.DB, table, constraint, indexName string) *addConstraintUsingUniqueIndexAction {
	return &addConstraintUsingUniqueIndexAction{
		conn:       conn,
		id:         fmt.Sprintf("add_constraint_using_unique_index_%s_%s", table, constraint),
		table:      table,
		constraint: constraint,
		indexName:  indexName,
	}
}

func (a *addConstraintUsingUniqueIndexAction) ID() string { return a.id }

func (a *addConstraintUsingUniqueIndexAction) Execute(ctx context.Context) error {
	_, err := a.conn.ExecContext(ctx, fmt.Sprintf("ALTER TABLE IF EXISTS %s ADD CONSTRAINT %s UNIQUE USING INDEX %s",
		pq.QuoteIdentifier(a.table),
		pq.QuoteIdentifier(a.constraint),
		pq.QuoteIdentifier(a.indexName)))
	return err
}

type addPrimaryKeyAction struct {
	conn      db.DB
	id        string
	table     string
	indexName string
}

func NewAddPrimaryKeyAction(conn db.DB, table, indexName string) *addPrimaryKeyAction {
	return &addPrimaryKeyAction{
		conn:      conn,
		id:        fmt.Sprintf("add_pk_%s_%s", table, indexName),
		table:     table,
		indexName: indexName,
	}
}

func (a *addPrimaryKeyAction) ID() string { return a.id }

func (a *addPrimaryKeyAction) Execute(ctx context.Context) error {
	_, err := a.conn.ExecContext(ctx, fmt.Sprintf("ALTER TABLE %s ADD PRIMARY KEY USING INDEX %s",
		pq.QuoteIdentifier(a.table),
		pq.QuoteIdentifier(a.indexName),
	))
	return err
}

// dropFunctionAction is a DBAction that drops a function and all of its dependencies (cascade).
type dropFunctionAction struct {
	conn      db.DB
	id        string
	functions []string
}

func NewDropFunctionAction(conn db.DB, functions ...string) *dropFunctionAction {
	return &dropFunctionAction{
		conn:      conn,
		id:        fmt.Sprintf("drop_function_%s", strings.Join(functions, "_")),
		functions: functions,
	}
}

func (a *dropFunctionAction) ID() string { return a.id }

func (a *dropFunctionAction) Execute(ctx context.Context) error {
	functions := make([]string, len(a.functions))
	for idx, fn := range a.functions {
		functions[idx] = pq.QuoteIdentifier(fn)
	}
	_, err := a.conn.ExecContext(ctx, fmt.Sprintf("DROP FUNCTION IF EXISTS %s CASCADE",
		strings.Join(functions, ",")))
	return err
}

type createIndexConcurrentlyAction struct {
	conn              db.DB
	id                string
	table             string
	name              string
	method            string
	unique            bool
	columns           map[string]IndexField
	storageParameters string
	predicate         string
}

func NewCreateIndexConcurrentlyAction(conn db.DB, table, name, method string, unique bool, columns map[string]IndexField, storageParameters, predicate string) *createIndexConcurrentlyAction {
	return &createIndexConcurrentlyAction{
		conn:              conn,
		id:                fmt.Sprintf("create_index_concurrently_%s_%s", table, name),
		table:             table,
		name:              name,
		method:            method,
		unique:            unique,
		columns:           columns,
		storageParameters: storageParameters,
		predicate:         predicate,
	}
}

func (a *createIndexConcurrentlyAction) ID() string { return a.id }

func (a *createIndexConcurrentlyAction) Execute(ctx context.Context) error {
	stmtFmt := "CREATE INDEX CONCURRENTLY %s ON %s"
	if a.unique {
		stmtFmt = "CREATE UNIQUE INDEX CONCURRENTLY %s ON %s"
	}
	stmt := fmt.Sprintf(stmtFmt,
		pq.QuoteIdentifier(a.name),
		pq.QuoteIdentifier(a.table))

	if a.method != "" {
		stmt += fmt.Sprintf(" USING %s", a.method)
	}

	colSQLs := make([]string, 0, len(a.columns))
	for columnName, settings := range a.columns {
		colSQL := pq.QuoteIdentifier(columnName)
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

	if a.storageParameters != "" {
		stmt += fmt.Sprintf(" WITH (%s)", a.storageParameters)
	}

	if a.predicate != "" {
		stmt += fmt.Sprintf(" WHERE %s", a.predicate)
	}
	_, err := a.conn.ExecContext(ctx, stmt)
	return err
}

// commentColumnAction is a DBAction that adds a comment to a column in a table.
type commentColumnAction struct {
	conn    db.DB
	id      string
	table   string
	column  string
	comment *string
}

func NewCommentColumnAction(conn db.DB, table, column string, comment *string) *commentColumnAction {
	return &commentColumnAction{
		conn:    conn,
		id:      fmt.Sprintf("comment_column_%s_%s", table, column),
		table:   table,
		column:  column,
		comment: comment,
	}
}

func (a *commentColumnAction) ID() string { return a.id }

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
	id      string
	table   string
	comment *string
}

func NewCommentTableAction(conn db.DB, table string, comment *string) *commentTableAction {
	return &commentTableAction{
		conn:    conn,
		id:      fmt.Sprintf("comment_table_%s", table),
		table:   table,
		comment: comment,
	}
}

func (a *commentTableAction) ID() string { return a.id }

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
	id          string
	schemaName  string
	indexName   string
	tableName   string
	columnNames []string
}

func NewCreateUniqueIndexConcurrentlyAction(conn db.DB, schemaName, indexName, tableName string, columnNames ...string) *createUniqueIndexConcurrentlyAction {
	return &createUniqueIndexConcurrentlyAction{
		conn:        conn,
		id:          fmt.Sprintf("create_unique_index_concurrently_%s_%s", indexName, tableName),
		schemaName:  schemaName,
		indexName:   indexName,
		tableName:   tableName,
		columnNames: columnNames,
	}
}

func (a *createUniqueIndexConcurrentlyAction) ID() string { return a.id }

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
	id          string
	table       string
	columns     string
	constraints string
}

func NewCreateTableAction(conn db.DB, table, columns, constraints string) *createTableAction {
	return &createTableAction{
		conn:        conn,
		id:          fmt.Sprintf("create_table_%s", table),
		table:       table,
		columns:     columns,
		constraints: constraints,
	}
}

func (a *createTableAction) ID() string { return a.id }

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
	id   string
	name string
}

func NewDropIndexAction(conn db.DB, name string) *dropIndexAction {
	return &dropIndexAction{
		conn: conn,
		id:   fmt.Sprintf("drop_index_%s", name),
		name: name,
	}
}

func (a *dropIndexAction) ID() string { return a.id }

func (a *dropIndexAction) Execute(ctx context.Context) error {
	_, err := a.conn.ExecContext(ctx, fmt.Sprintf("DROP INDEX CONCURRENTLY IF EXISTS %s",
		pq.QuoteIdentifier(a.name)))
	return err
}

// DropTableAction is a DBAction that drops a table.
type DropTableAction struct {
	conn  db.DB
	id    string
	table string
}

func NewDropTableAction(conn db.DB, table string) *DropTableAction {
	return &DropTableAction{
		conn:  conn,
		id:    fmt.Sprintf("drop_table_%s", table),
		table: table,
	}
}

func (a *DropTableAction) ID() string { return a.id }

func (a *DropTableAction) Execute(ctx context.Context) error {
	_, err := a.conn.ExecContext(ctx, fmt.Sprintf("DROP TABLE IF EXISTS %s",
		pq.QuoteIdentifier(a.table)))
	return err
}

// validateConstraintAction is a DBAction that validates a constraint in a table.
type validateConstraintAction struct {
	conn       db.DB
	id         string
	table      string
	constraint string
}

func NewValidateConstraintAction(conn db.DB, table, constraint string) *validateConstraintAction {
	return &validateConstraintAction{
		conn:       conn,
		id:         fmt.Sprintf("validate_constraint_%s_%s", table, constraint),
		table:      table,
		constraint: constraint,
	}
}

func (a *validateConstraintAction) ID() string { return a.id }

func (a *validateConstraintAction) Execute(ctx context.Context) error {
	_, err := a.conn.ExecContext(ctx, fmt.Sprintf("ALTER TABLE IF EXISTS %s VALIDATE CONSTRAINT %s",
		pq.QuoteIdentifier(a.table),
		pq.QuoteIdentifier(a.constraint)))
	return err
}

// CreateCheckConstraintAction creates a check constraint on a table.
type CreateCheckConstraintAction struct {
	conn           db.DB
	id             string
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
		id:             fmt.Sprintf("create_check_constraint_%s_%s", table, constraint),
		table:          table,
		columns:        columns,
		check:          check,
		constraint:     constraint,
		noInherit:      noInherit,
		skipValidation: skipValidation,
	}
}

func (a *CreateCheckConstraintAction) ID() string { return a.id }

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
	id                string
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
		id:                fmt.Sprintf("create_fk_constraint_%s_%s", table, constraint),
		table:             table,
		constraint:        constraint,
		columns:           columns,
		reference:         reference,
		initiallyDeferred: initiallyDeferred,
		deferrable:        deferrable,
		skipValidation:    skipValidation,
	}
}

func (a *createFKConstraintAction) ID() string { return a.id }

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
	id    string
	table string
	from  string
	to    string
}

func NewAlterSequenceOwnerAction(conn db.DB, table, from, to string) *alterSequenceOwnerAction {
	return &alterSequenceOwnerAction{
		conn:  conn,
		id:    fmt.Sprintf("alter_sequence_owner_%s_%s_to_%s", table, from, to),
		table: table,
		from:  from,
		to:    to,
	}
}

func (a *alterSequenceOwnerAction) ID() string { return a.id }

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
	id         string
	table      string
	constraint string
}

func NewDropConstraintAction(conn db.DB, table, constraint string) *dropConstraintAction {
	return &dropConstraintAction{
		conn:       conn,
		id:         fmt.Sprintf("drop_constraint_%s_%s", table, constraint),
		table:      table,
		constraint: constraint,
	}
}

func (a *dropConstraintAction) ID() string { return a.id }

func (a *dropConstraintAction) Execute(ctx context.Context) error {
	_, err := a.conn.ExecContext(ctx, fmt.Sprintf("ALTER TABLE IF EXISTS %s DROP CONSTRAINT IF EXISTS %s",
		pq.QuoteIdentifier(a.table),
		pq.QuoteIdentifier(a.constraint)))
	return err
}

type setNotNullAction struct {
	conn   db.DB
	id     string
	table  string
	column string
}

func NewSetNotNullAction(conn db.DB, table, column string) *setNotNullAction {
	return &setNotNullAction{
		conn:   conn,
		id:     fmt.Sprintf("set_not_null_%s_%s", table, column),
		table:  table,
		column: column,
	}
}

func (a *setNotNullAction) ID() string { return a.id }

func (a *setNotNullAction) Execute(ctx context.Context) error {
	_, err := a.conn.ExecContext(ctx, fmt.Sprintf("ALTER TABLE IF EXISTS %s ALTER COLUMN %s SET NOT NULL",
		pq.QuoteIdentifier(a.table),
		pq.QuoteIdentifier(a.column)))
	return err
}

type setDefaultAction struct {
	conn         db.DB
	id           string
	table        string
	column       string
	defaultValue string
}

func NewSetDefaultValueAction(conn db.DB, table, column, defaultValue string) *setDefaultAction {
	return &setDefaultAction{
		conn:         conn,
		id:           fmt.Sprintf("set_default_%s_%s", table, column),
		table:        table,
		column:       column,
		defaultValue: defaultValue,
	}
}

func (a *setDefaultAction) ID() string { return a.id }

func (a *setDefaultAction) Execute(ctx context.Context) error {
	_, err := a.conn.ExecContext(ctx, fmt.Sprintf("ALTER TABLE IF EXISTS %s ALTER COLUMN %s SET DEFAULT %s",
		pq.QuoteIdentifier(a.table),
		pq.QuoteIdentifier(a.column),
		a.defaultValue))
	return err
}

type dropDefaultAction struct {
	conn   db.DB
	id     string
	table  string
	column string
}

func NewDropDefaultValueAction(conn db.DB, table, column string) *dropDefaultAction {
	return &dropDefaultAction{
		conn:   conn,
		id:     fmt.Sprintf("drop_default_%s_%s", table, column),
		table:  table,
		column: column,
	}
}

func (a *dropDefaultAction) ID() string { return a.id }

func (a *dropDefaultAction) Execute(ctx context.Context) error {
	_, err := a.conn.ExecContext(ctx, fmt.Sprintf("ALTER TABLE IF EXISTS %s ALTER COLUMN %s DROP DEFAULT",
		pq.QuoteIdentifier(a.table),
		pq.QuoteIdentifier(a.column)))
	return err
}

type rawSQLAction struct {
	conn db.DB
	id   string
	sql  string
}

func NewRawSQLAction(conn db.DB, sql string) *rawSQLAction {
	return &rawSQLAction{
		conn: conn,
		id:   fmt.Sprintf("raw_sql_%s", uuid.NewString()),
		sql:  sql,
	}
}

func (a *rawSQLAction) ID() string { return a.id }

func (a *rawSQLAction) Execute(ctx context.Context) error {
	_, err := a.conn.ExecContext(ctx, a.sql)
	return err
}

type setReplicaIdentityAction struct {
	conn     db.DB
	id       string
	table    string
	identity string
	index    string
}

func NewSetReplicaIdentityAction(conn db.DB, table string, identityType, index string) *setReplicaIdentityAction {
	identity := strings.ToUpper(identityType)
	return &setReplicaIdentityAction{
		conn:     conn,
		id:       fmt.Sprintf("set_replica_%s_%s", identity, index),
		table:    table,
		identity: identity,
		index:    index,
	}
}

func (a *setReplicaIdentityAction) ID() string { return a.id }

func (a *setReplicaIdentityAction) Execute(ctx context.Context) error {
	// build the correct form of the `SET REPLICA IDENTITY` statement based on the`identity type
	identitySQL := a.identity
	if identitySQL == "INDEX" {
		identitySQL = fmt.Sprintf("USING INDEX %s", pq.QuoteIdentifier(a.index))
	}

	// set the replica identity on the underlying table
	_, err := a.conn.ExecContext(ctx, fmt.Sprintf("ALTER TABLE %s REPLICA IDENTITY %s",
		pq.QuoteIdentifier(a.table),
		identitySQL))
	return err
}

type alterReferencesAction struct {
	conn   db.DB
	id     string
	table  string
	column string
}

func NewAlterReferencesAction(conn db.DB, table, column string) *alterReferencesAction {
	return &alterReferencesAction{
		conn:   conn,
		id:     fmt.Sprintf("alter_references_%s_%s", table, column),
		table:  table,
		column: column,
	}
}

func (a *alterReferencesAction) ID() string {
	return a.id
}

func (a *alterReferencesAction) Execute(ctx context.Context) error {
	definitions, err := a.constraintDefinitions(ctx)
	if err != nil {
		return err
	}
	for _, def := range definitions {
		// Drop the existing constraint
		_, err := a.conn.ExecContext(ctx, fmt.Sprintf("ALTER TABLE %s DROP CONSTRAINT %s",
			pq.QuoteIdentifier(def.table),
			pq.QuoteIdentifier(def.name),
		))
		if err != nil {
			return fmt.Errorf("dropping constraint %s on %s: %w", def.name, def.table, err)
		}

		// Recreate the constraint with the table and new column
		newDef := strings.ReplaceAll(def.def, a.column, pq.QuoteIdentifier(TemporaryName(a.column)))
		newDef = strings.ReplaceAll(newDef, a.table, pq.QuoteIdentifier(a.table))
		_, err = a.conn.ExecContext(ctx, fmt.Sprintf("ALTER TABLE %s ADD CONSTRAINT %s %s",
			pq.QuoteIdentifier(def.table),
			pq.QuoteIdentifier(def.name),
			newDef,
		))
		if err != nil {
			return fmt.Errorf("altering references for %s.%s: %w", a.table, a.column, err)
		}
	}
	return nil
}

type constraintDefinition struct {
	name  string
	table string
	def   string
}

func (a *alterReferencesAction) constraintDefinitions(ctx context.Context) ([]constraintDefinition, error) {
	rows, err := a.conn.QueryContext(ctx, fmt.Sprintf(`
SELECT conname, r.conrelid::regclass, pg_catalog.pg_get_constraintdef(r.oid, true) as condef
FROM pg_catalog.pg_constraint r
WHERE confrelid = %s::regclass AND r.contype = 'f'`,
		pq.QuoteIdentifier(a.table),
	))
	// No FK constraint for table
	if err != nil {
		return nil, nil
	}
	defer rows.Close()

	defs := make([]constraintDefinition, 0)
	for rows.Next() {
		var def constraintDefinition
		if err := rows.Scan(&def.name, &def.table, &def.def); err != nil {
			return nil, fmt.Errorf("scanning referencing constraints for %s.%s: %w", a.table, a.column, err)
		}
		defs = append(defs, def)
	}
	return defs, rows.Err()
}
