package state

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/lib/pq"
)

const sqlInit = `
CREATE SCHEMA IF NOT EXISTS %[1]s;

CREATE TABLE IF NOT EXISTS %[1]s.migrations (
	name			TEXT PRIMARY KEY,
	migration		JSONB NOT NULL,
	created_at		TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
	updated_at		TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,

	parent			TEXT REFERENCES %[1]s.migrations(name) UNIQUE,
	done			BOOLEAN NOT NULL DEFAULT false,
	failed			BOOLEAN NOT NULL DEFAULT false
);

-- Only one migration can be active at a time
CREATE UNIQUE INDEX IF NOT EXISTS only_one_active ON %[1]s.migrations (done) WHERE done = false;


-- Helper functions

-- Are we in the middle of a migration?
CREATE OR REPLACE FUNCTION %[1]s.is_active_migration_period() RETURNS boolean
	AS $$ SELECT EXISTS (SELECT 1 FROM %[1]s.migrations WHERE done = false) $$
    LANGUAGE SQL
    STABLE;

-- Get the latest version name (this is the one with child migrations)
CREATE OR REPLACE FUNCTION %[1]s.latest_version() RETURNS text
AS $$ SELECT name FROM %[1]s.migrations WHERE NOT EXISTS (SELECT 1 FROM %[1]s.migrations AS other WHERE name = other.parent) $$
LANGUAGE SQL
STABLE;
`

type State struct {
	pgConn *sql.DB
	schema string
}

func New(ctx context.Context, pgURL, stateSchema string) (*State, error) {
	conn, err := sql.Open("postgres", pgURL)
	if err != nil {
		return nil, err
	}

	return &State{
		pgConn: conn,
		schema: stateSchema,
	}, nil
}

func (s *State) Init(ctx context.Context) error {
	// ensure pg-roll internal tables exist
	// TODO: eventually use migrations for this instead of hardcoding
	_, err := s.pgConn.ExecContext(ctx, fmt.Sprintf(sqlInit, pq.QuoteIdentifier(s.schema)))

	return err
}

func (s *State) Close() error {
	return s.pgConn.Close()
}

// IsActiveMigrationPeriod returns true if there is an active migration
func (s *State) IsActiveMigrationPeriod(ctx context.Context) (bool, error) {
	var isActive bool
	err := s.pgConn.QueryRowContext(ctx, fmt.Sprintf("SELECT %s.is_active_migration_period()", pq.QuoteIdentifier(s.schema))).Scan(&isActive)
	if err != nil {
		return false, err
	}

	return isActive, err
}

// GetActiveMigration returns the name & raw content of the active migration (if any), errors out otherwise
func (s *State) GetActiveMigration(ctx context.Context) (string, string, error) {
	var name, migration string
	err := s.pgConn.QueryRowContext(ctx, fmt.Sprintf("SELECT name, migration FROM %s.migrations WHERE done = false", pq.QuoteIdentifier(s.schema))).Scan(&name, &migration)
	if err != nil {
		return "", "", err
	}

	return name, migration, nil
}

// Start creates a new migration, storing it's name and raw content
// this will effectively activate a new migration period, so `IsActiveMigrationPeriod` will return true
// until the migration is completed
func (s *State) Start(ctx context.Context, name, rawMigration string) error {
	_, err := s.pgConn.ExecContext(ctx, fmt.Sprintf("INSERT INTO %s.migrations (name, migration) VALUES ($1, $2)", pq.QuoteIdentifier(s.schema)), name, rawMigration)

	// TODO handle constraint violations, ie to detect an active migration, or duplicated names
	return err
}

// Complete marks a migration as completed
func (s *State) Complete(ctx context.Context, name string) error {
	res, err := s.pgConn.ExecContext(ctx, fmt.Sprintf("UPDATE %s.migrations SET done=$1 WHERE NAME=$2 AND done=$3", pq.QuoteIdentifier(s.schema)), true, name, false)
	if err != nil {
		return err
	}
	// TODO handle constraint violations, ie trying to complete a migration that is not active

	rows, err := res.RowsAffected()
	if err != nil {
		return err
	}

	if rows == 0 {
		return fmt.Errorf("no migration found with name %s", name)
	}

	return err
}

// Rollback removes a migration from the state (we consider it rolled back, as if it never started)
func (s *State) Rollback(ctx context.Context, name string) error {
	res, err := s.pgConn.ExecContext(ctx, fmt.Sprintf("DELETE FROM %s.migrations WHERE NAME=$1 AND done=$2", pq.QuoteIdentifier(s.schema)), name, false)
	if err != nil {
		return err
	}
	// TODO handle constraint violations, ie trying to complete a migration that is not active

	rows, err := res.RowsAffected()
	if err != nil {
		return err
	}

	if rows == 0 {
		return fmt.Errorf("no migration found with name %s", name)
	}

	return nil
}
