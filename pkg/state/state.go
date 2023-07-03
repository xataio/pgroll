package state

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"

	"pg-roll/pkg/migrations"

	"github.com/lib/pq"
)

const sqlInit = `
CREATE SCHEMA IF NOT EXISTS %[1]s;

CREATE TABLE IF NOT EXISTS %[1]s.migrations (
	schema			NAME NOT NULL,
	name			TEXT NOT NULL,
	migration		JSONB NOT NULL,
	created_at		TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
	updated_at		TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,

	parent			TEXT,
	done			BOOLEAN NOT NULL DEFAULT false,
	failed			BOOLEAN NOT NULL DEFAULT false,

    PRIMARY KEY (schema, name),
	FOREIGN KEY	(schema, parent) REFERENCES %[1]s.migrations(schema, name)
);

-- Only one migration can be active at a time
CREATE UNIQUE INDEX IF NOT EXISTS only_one_active ON %[1]s.migrations (schema, name, done) WHERE done = false;

-- Only first migration can exist without parent
CREATE UNIQUE INDEX IF NOT EXISTS only_first_migration_without_parent ON %[1]s.migrations ((1)) WHERE parent IS NULL;

-- History is linear
CREATE UNIQUE INDEX IF NOT EXISTS history_is_linear ON %[1]s.migrations (schema, parent);

-- Helper functions

-- Are we in the middle of a migration?
CREATE OR REPLACE FUNCTION %[1]s.is_active_migration_period(schemaname NAME) RETURNS boolean
	AS $$ SELECT EXISTS (SELECT 1 FROM %[1]s.migrations WHERE schema=schemaname AND done=false) $$
    LANGUAGE SQL
    STABLE;

-- Get the latest version name (this is the one with child migrations)
CREATE OR REPLACE FUNCTION %[1]s.latest_version(schemaname NAME) RETURNS text
AS $$ SELECT p.name FROM %[1]s.migrations p WHERE NOT EXISTS (SELECT 1 FROM %[1]s.migrations c WHERE schema=schemaname AND c.parent=p.name) $$
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
func (s *State) IsActiveMigrationPeriod(ctx context.Context, schema string) (bool, error) {
	var isActive bool
	err := s.pgConn.QueryRowContext(ctx, fmt.Sprintf("SELECT %s.is_active_migration_period($1)", pq.QuoteIdentifier(s.schema)), schema).Scan(&isActive)
	if err != nil {
		return false, err
	}

	return isActive, err
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

// Start creates a new migration, storing its name and raw content
// this will effectively activate a new migration period, so `IsActiveMigrationPeriod` will return true
// until the migration is completed
func (s *State) Start(ctx context.Context, schema string, migration *migrations.Migration) error {
	rawMigration, err := json.Marshal(migration)
	if err != nil {
		return fmt.Errorf("unable to marshal migration: %w", err)
	}

	_, err = s.pgConn.ExecContext(ctx,
		fmt.Sprintf("INSERT INTO %[1]s.migrations (schema, name, parent, migration) VALUES ($1, $2, %[1]s.latest_version($1), $3)", pq.QuoteIdentifier(s.schema)),
		schema, migration.Name, rawMigration)

	return err
}

// Complete marks a migration as completed
func (s *State) Complete(ctx context.Context, schema, name string) error {
	res, err := s.pgConn.ExecContext(ctx, fmt.Sprintf("UPDATE %s.migrations SET done=$1 WHERE schema=$2 AND name=$3 AND done=$4", pq.QuoteIdentifier(s.schema)), true, schema, name, false)
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
