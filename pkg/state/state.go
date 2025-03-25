// SPDX-License-Identifier: Apache-2.0

package state

import (
	"context"
	"database/sql"
	_ "embed"
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	"github.com/lib/pq"

	"github.com/xataio/pgroll/pkg/migrations"
	"github.com/xataio/pgroll/pkg/schema"
)

//go:embed init.sql
var sqlInit string

type State struct {
	pgConn *sql.DB
	schema string
}

func New(ctx context.Context, pgURL, stateSchema string) (*State, error) {
	dsn, err := pq.ParseURL(pgURL)
	if err != nil {
		dsn = pgURL
	}

	dsn += " search_path=" + stateSchema

	conn, err := sql.Open("postgres", dsn)
	if err != nil {
		return nil, err
	}

	if err := conn.PingContext(ctx); err != nil {
		return nil, err
	}

	_, err = conn.ExecContext(ctx, "SET LOCAL pgroll.internal to 'TRUE'")
	if err != nil {
		return nil, fmt.Errorf("unable to set pgroll.internal to true: %w", err)
	}

	return &State{
		pgConn: conn,
		schema: stateSchema,
	}, nil
}

// Init initializes the required pg_roll schema to store the state
func (s *State) Init(ctx context.Context) error {
	tx, err := s.pgConn.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()

	// Try to obtain an advisory lock.
	// The key is an arbitrary number, used to distinguish the lock from other locks.
	// The lock is automatically released when the transaction is committed or rolled back.
	const key int64 = 0x2c03057fb9525b
	_, err = tx.ExecContext(ctx, "SELECT pg_advisory_xact_lock($1)", key)
	if err != nil {
		return err
	}

	// Perform pgroll state initialization
	q := strings.ReplaceAll(sqlInit, "placeholder", pq.QuoteIdentifier(s.schema))
	_, err = tx.ExecContext(ctx, q)
	if err != nil {
		return err
	}

	return tx.Commit()
}

func (s *State) IsInitialized(ctx context.Context) (bool, error) {
	var isInitialized bool
	err := s.pgConn.QueryRowContext(ctx,
		"SELECT EXISTS (SELECT 1 from pg_catalog.pg_namespace WHERE nspname = $1)",
		s.schema).Scan(&isInitialized)
	if err != nil {
		return false, err
	}

	return isInitialized, nil
}

func (s *State) Close() error {
	return s.pgConn.Close()
}

// Schema returns the schema name
func (s *State) Schema() string {
	return s.schema
}

// IsActiveMigrationPeriod returns true if there is an active migration
func (s *State) IsActiveMigrationPeriod(ctx context.Context, schema string) (bool, error) {
	var isActive bool
	err := s.pgConn.QueryRowContext(ctx, fmt.Sprintf("SELECT %s.is_active_migration_period($1)", pq.QuoteIdentifier(s.schema)), schema).Scan(&isActive)
	if err != nil {
		return false, err
	}

	return isActive, nil
}

// GetActiveMigration returns the name & raw content of the active migration (if any), errors out otherwise
func (s *State) GetActiveMigration(ctx context.Context, schema string) (*migrations.Migration, error) {
	var name, rawMigration string
	err := s.pgConn.QueryRowContext(ctx, fmt.Sprintf("SELECT name, migration FROM %s.migrations WHERE schema=$1 AND done=false", pq.QuoteIdentifier(s.schema)), schema).Scan(&name, &rawMigration)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, ErrNoActiveMigration
		}
		return nil, err
	}

	var migration migrations.Migration
	err = json.Unmarshal([]byte(rawMigration), &migration)
	if err != nil {
		return nil, fmt.Errorf("unable to unmarshal migration: %w", err)
	}

	return &migration, nil
}

// LatestVersion returns the name of the latest version schema, or nil if there
// is none. No active version occurs after initialization, but before the first
// migration is started.
func (s *State) LatestVersion(ctx context.Context, schema string) (*string, error) {
	var version *string
	err := s.pgConn.QueryRowContext(ctx,
		fmt.Sprintf("SELECT %s.latest_version($1)", pq.QuoteIdentifier(s.schema)),
		schema).Scan(&version)
	if err != nil {
		return nil, err
	}

	return version, nil
}

// PreviousVersion returns the name of the previous version schema
func (s *State) PreviousVersion(ctx context.Context, schema string, includeInferred bool) (*string, error) {
	var parent *string
	err := s.pgConn.QueryRowContext(ctx,
		fmt.Sprintf("SELECT %s.previous_version($1, $2)", pq.QuoteIdentifier(s.schema)),
		schema, includeInferred).Scan(&parent)
	if err != nil {
		return nil, err
	}

	return parent, nil
}

// Status returns the current migration status of the specified schema
func (s *State) Status(ctx context.Context, schema string) (*Status, error) {
	latestVersion, err := s.LatestVersion(ctx, schema)
	if err != nil {
		return nil, err
	}
	if latestVersion == nil {
		latestVersion = new(string)
	}

	isActive, err := s.IsActiveMigrationPeriod(ctx, schema)
	if err != nil {
		return nil, err
	}

	var status MigrationStatus
	if *latestVersion == "" {
		status = NoneMigrationStatus
	} else if isActive {
		status = InProgressMigrationStatus
	} else {
		status = CompleteMigrationStatus
	}

	return &Status{
		Schema:  schema,
		Version: *latestVersion,
		Status:  status,
	}, nil
}

// Start creates a new migration, storing its name and raw content
// this will effectively activate a new migration period, so `IsActiveMigrationPeriod` will return true
// until the migration is completed
// This method will return the current schema (before the migration is applied)
func (s *State) Start(ctx context.Context, schemaname string, migration *migrations.Migration) (*schema.Schema, error) {
	rawMigration, err := json.Marshal(migration)
	if err != nil {
		return nil, fmt.Errorf("unable to marshal migration: %w", err)
	}

	// create a new migration object and return the previous known schema
	// if there is no previous migration, read the schema from postgres
	stmt := fmt.Sprintf(`
		INSERT INTO %[1]s.migrations (schema, name, parent, migration) VALUES ($1, $2, %[1]s.latest_version($1), $3)
		RETURNING (
			SELECT COALESCE(
				(SELECT resulting_schema FROM %[1]s.migrations WHERE schema=$1 AND name=%[1]s.latest_version($1)),
				%[1]s.read_schema($1))
		)`, pq.QuoteIdentifier(s.schema))

	var rawSchema string
	err = s.pgConn.QueryRowContext(ctx, stmt, schemaname, migration.Name, rawMigration).Scan(&rawSchema)
	if err != nil {
		return nil, err
	}

	var unmarshalledSchema schema.Schema
	err = json.Unmarshal([]byte(rawSchema), &unmarshalledSchema)
	if err != nil {
		return nil, fmt.Errorf("unable to unmarshal schema: %w", err)
	}

	return &unmarshalledSchema, nil
}

// Complete marks a migration as completed
func (s *State) Complete(ctx context.Context, schema, name string) error {
	res, err := s.pgConn.ExecContext(ctx, fmt.Sprintf("UPDATE %[1]s.migrations SET done=$1, resulting_schema=(SELECT %[1]s.read_schema($2)) WHERE schema=$2 AND name=$3 AND done=$4", pq.QuoteIdentifier(s.schema)), true, schema, name, false)
	if err != nil {
		return err
	}

	rows, err := res.RowsAffected()
	if err != nil {
		return err
	}

	if rows == 0 {
		return fmt.Errorf("no migration found with name %s", name)
	}

	return err
}

// ReadSchema reads the schema for the specified schema name
func (s *State) ReadSchema(ctx context.Context, schemaName string) (*schema.Schema, error) {
	var rawSchema []byte
	err := s.pgConn.QueryRowContext(ctx, fmt.Sprintf("SELECT %s.read_schema($1)", pq.QuoteIdentifier(s.schema)), schemaName).Scan(&rawSchema)
	if err != nil {
		return nil, err
	}

	var sc schema.Schema
	err = json.Unmarshal(rawSchema, &sc)
	if err != nil {
		return nil, fmt.Errorf("unable to unmarshal schema: %w", err)
	}

	return &sc, nil
}

// SchemaAfterMigration reads the schema after the migration `version` was
// applied to `schemaName`
func (s *State) SchemaAfterMigration(ctx context.Context, schemaName, version string) (*schema.Schema, error) {
	sql := fmt.Sprintf("SELECT resulting_schema FROM %s.migrations WHERE schema=$1 AND name=$2", pq.QuoteIdentifier(s.schema))

	var rawSchema []byte
	err := s.pgConn.QueryRowContext(ctx, sql, schemaName, version).Scan(&rawSchema)
	if err != nil {
		return nil, err
	}

	var sc schema.Schema
	err = json.Unmarshal(rawSchema, &sc)
	if err != nil {
		return nil, fmt.Errorf("unable to unmarshal schema: %w", err)
	}

	return &sc, nil
}

// Rollback removes a migration from the state (we consider it rolled back, as if it never started)
func (s *State) Rollback(ctx context.Context, schema, name string) error {
	res, err := s.pgConn.ExecContext(ctx, fmt.Sprintf("DELETE FROM %s.migrations WHERE schema=$1 AND name=$2 AND done=$3", pq.QuoteIdentifier(s.schema)), schema, name, false)
	if err != nil {
		return err
	}

	rows, err := res.RowsAffected()
	if err != nil {
		return err
	}

	if rows == 0 {
		return fmt.Errorf("no migration found with name %s", name)
	}

	return nil
}
