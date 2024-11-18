// SPDX-License-Identifier: Apache-2.0

package roll

import (
	"context"
	"database/sql"
	"fmt"
	"io/fs"
	"strings"
	"time"

	"github.com/lib/pq"

	"github.com/xataio/pgroll/pkg/db"
	"github.com/xataio/pgroll/pkg/migrations"
	"github.com/xataio/pgroll/pkg/state"
)

type PGVersion int

const (
	PGVersion15              PGVersion     = 15
	DefaultBackfillBatchSize int           = 1000
	DefaultBackfillDelay     time.Duration = 0
)

var ErrMismatchedMigration = fmt.Errorf("remote migration does not match local migration")

type Roll struct {
	pgConn db.DB

	// schema we are acting on
	schema string

	// disable pgroll version schemas creation and deletion
	disableVersionSchemas bool

	// disable creation of version schema for raw SQL migrations
	noVersionSchemaForRawSQL bool

	migrationHooks MigrationHooks
	state          *state.State
	pgVersion      PGVersion
	sqlTransformer migrations.SQLTransformer

	backfillBatchSize  int
	backfillBatchDelay time.Duration
	skipValidation     bool
}

// New creates a new Roll instance
func New(ctx context.Context, pgURL, schema string, state *state.State, opts ...Option) (*Roll, error) {
	rollOpts := &options{}
	for _, o := range opts {
		o(rollOpts)
	}
	if rollOpts.backfillBatchSize <= 0 {
		rollOpts.backfillBatchSize = DefaultBackfillBatchSize
	}

	conn, err := setupConn(ctx, pgURL, schema, *rollOpts)
	if err != nil {
		return nil, err
	}

	var pgMajorVersion PGVersion
	err = conn.QueryRowContext(ctx, "SELECT substring(split_part(version(), ' ', 2) from '^[0-9]+')").Scan(&pgMajorVersion)
	if err != nil {
		return nil, fmt.Errorf("unable to retrieve postgres version: %w", err)
	}

	var sqlTransformer migrations.SQLTransformer = migrations.SQLTransformerFunc(
		func(sql string) (string, error) { return sql, nil },
	)
	if rollOpts.sqlTransformer != nil {
		sqlTransformer = rollOpts.sqlTransformer
	}

	return &Roll{
		pgConn:                   &db.RDB{DB: conn},
		schema:                   schema,
		state:                    state,
		pgVersion:                pgMajorVersion,
		sqlTransformer:           sqlTransformer,
		disableVersionSchemas:    rollOpts.disableVersionSchemas,
		noVersionSchemaForRawSQL: rollOpts.noVersionSchemaForRawSQL,
		migrationHooks:           rollOpts.migrationHooks,
		backfillBatchSize:        rollOpts.backfillBatchSize,
		backfillBatchDelay:       rollOpts.backfillBatchDelay,
		skipValidation:           rollOpts.skipValidation,
	}, nil
}

func setupConn(ctx context.Context, pgURL, schema string, options options) (*sql.DB, error) {
	dsn, err := pq.ParseURL(pgURL)
	if err != nil {
		dsn = pgURL
	}

	searchPath := append([]string{schema}, options.searchPath...)
	dsn += " search_path=" + strings.Join(searchPath, ",")

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

// Status returns the current migration status
func (m *Roll) Status(ctx context.Context, schema string) (*state.Status, error) {
	return m.state.Status(ctx, schema)
}

func (m *Roll) Close() error {
	err := m.state.Close()
	if err != nil {
		return err
	}

	return m.pgConn.Close()
}

// UnappliedMigrations returns a slice of unapplied migrations from `dir`,
// lexicographically ordered by filename. Applying each of the returned
// migrations in order will bring the database up to date with `dir`.
//
// If the local order of migrations does not match the order of migrations in
// the schema history, an `ErrMismatchedMigration` error is returned.
func (m *Roll) UnappliedMigrations(ctx context.Context, dir fs.FS) ([]*migrations.Migration, error) {
	latestVersion, err := m.State().LatestVersion(ctx, m.Schema())
	if err != nil {
		return nil, fmt.Errorf("determining latest version: %w", err)
	}

	files, err := fs.Glob(dir, "*.json")
	if err != nil {
		return nil, fmt.Errorf("reading directory: %w", err)
	}

	history, err := m.State().SchemaHistory(ctx, m.Schema())
	if err != nil {
		return nil, fmt.Errorf("reading schema history: %w", err)
	}

	// Find the index of the first unapplied migration
	var idx int
	if latestVersion != nil {
		for _, file := range files {
			migration, err := openAndReadMigrationFile(dir, file)
			if err != nil {
				return nil, fmt.Errorf("reading migration file %q: %w", file, err)
			}

			remoteMigration := history[idx].Migration
			if remoteMigration.Name != migration.Name {
				return nil, fmt.Errorf("%w: remote=%q, local=%q", ErrMismatchedMigration, remoteMigration.Name, migration.Name)
			}

			idx++
			if migration.Name == *latestVersion {
				break
			}
		}
	}

	// Return all unapplied migrations
	migs := make([]*migrations.Migration, 0, len(files))
	for _, file := range files[idx:] {
		migration, err := openAndReadMigrationFile(dir, file)
		if err != nil {
			return nil, fmt.Errorf("reading migration file %q: %w", file, err)
		}
		migs = append(migs, migration)
	}

	return migs, nil
}

func openAndReadMigrationFile(dir fs.FS, filename string) (*migrations.Migration, error) {
	file, err := dir.Open(filename)
	if err != nil {
		return nil, err
	}

	migration, err := migrations.ReadMigration(file)
	if err != nil {
		return nil, err
	}

	return migration, nil
}
