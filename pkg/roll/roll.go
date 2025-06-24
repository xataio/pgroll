// SPDX-License-Identifier: Apache-2.0

package roll

import (
	"context"
	"database/sql"
	"fmt"
	"strings"

	"github.com/lib/pq"

	"github.com/xataio/pgroll/pkg/db"
	"github.com/xataio/pgroll/pkg/migrations"
	"github.com/xataio/pgroll/pkg/state"
)

type PGVersion int

const (
	PGVersion15 PGVersion = 15

	// applicationName is the Postgres application_name set on the connection
	// used by the the Roll instance
	applicationName = "pgroll"
)

var (
	ErrMismatchedMigration          = fmt.Errorf("remote migration does not match local migration")
	ErrExistingSchemaWithoutHistory = fmt.Errorf("schema has existing tables but no migration history - baseline required")
)

type Roll struct {
	pgConn db.DB

	logger migrations.Logger

	// schema we are acting on
	schema string

	// disable pgroll version schemas creation and deletion
	disableVersionSchemas bool

	migrationHooks MigrationHooks
	state          *state.State
	pgVersion      PGVersion
	skipValidation bool
}

// New creates a new Roll instance
func New(ctx context.Context, pgURL, schema string, state *state.State, opts ...Option) (*Roll, error) {
	rollOpts := &options{}
	for _, o := range opts {
		o(rollOpts)
	}

	conn, err := setupConn(ctx, pgURL, schema, *rollOpts)
	if err != nil {
		return nil, err
	}

	logger := migrations.NewNoopLogger()
	if rollOpts.verbose {
		logger = migrations.NewLogger()
	}

	var pgMajorVersion PGVersion
	err = conn.QueryRowContext(ctx, "SELECT substring(split_part(version(), ' ', 2) from '^[0-9]+')").Scan(&pgMajorVersion)
	if err != nil {
		return nil, fmt.Errorf("unable to retrieve postgres version: %w", err)
	}

	return &Roll{
		pgConn:                &db.RDB{DB: conn},
		logger:                logger,
		schema:                schema,
		state:                 state,
		pgVersion:             pgMajorVersion,
		disableVersionSchemas: rollOpts.disableVersionSchemas,
		migrationHooks:        rollOpts.migrationHooks,
		skipValidation:        rollOpts.skipValidation,
	}, nil
}

func setupConn(ctx context.Context, pgURL, schema string, options options) (*sql.DB, error) {
	dsn, err := pq.ParseURL(pgURL)
	if err != nil {
		dsn = pgURL
	}

	searchPath := append([]string{schema}, options.searchPath...)
	dsn += fmt.Sprintf(" search_path=%s application_name=%s",
		strings.Join(searchPath, ","), applicationName)

	conn, err := sql.Open("postgres", dsn)
	if err != nil {
		return nil, err
	}

	if err := conn.PingContext(ctx); err != nil {
		return nil, err
	}

	_, err = conn.ExecContext(ctx, "SET pgroll.no_inferred_migrations TO 'TRUE'")
	if err != nil {
		return nil, fmt.Errorf("unable to set pgroll.no_inferred_migrations to true: %w", err)
	}

	if options.lockTimeoutMs > 0 {
		_, err = conn.ExecContext(ctx, fmt.Sprintf("SET lock_timeout to '%dms'", options.lockTimeoutMs))
		if err != nil {
			return nil, fmt.Errorf("unable to set lock_timeout: %w", err)
		}
	}

	if options.role != "" {
		_, err = conn.ExecContext(ctx, fmt.Sprintf("SET ROLE %s", options.role))
		if err != nil {
			return nil, fmt.Errorf("unable to set role to '%s': %w", options.role, err)
		}
	}

	return conn, nil
}

// Init initializes the Roll instance
func (m *Roll) Init(ctx context.Context) error {
	return m.state.Init(ctx)
}

// PGVersion returns the postgres version
func (m *Roll) PGVersion() PGVersion {
	return m.pgVersion
}

// PgConn returns the underlying database connection
func (m *Roll) PgConn() db.DB {
	return m.pgConn
}

// State returns the state instance the Roll instance is acting on
func (m *Roll) State() *state.State {
	return m.state
}

// Schema returns the schema the Roll instance is acting on
func (m *Roll) Schema() string {
	return m.schema
}

func (m *Roll) UseVersionSchema() bool {
	return !m.disableVersionSchemas
}

func (m *Roll) Close() error {
	err := m.state.Close()
	if err != nil {
		return err
	}

	return m.pgConn.Close()
}
