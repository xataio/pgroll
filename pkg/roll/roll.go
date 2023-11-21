// SPDX-License-Identifier: Apache-2.0

package roll

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/lib/pq"

	"github.com/xataio/pgroll/pkg/state"
)

type PGVersion int

const PGVersion15 PGVersion = 15

type Roll struct {
	pgConn *sql.DB // TODO abstract sql connection

	// schema we are acting on
	schema string

	state     *state.State
	pgVersion PGVersion
}

func New(ctx context.Context, pgURL, schema string, lockTimeoutMs int, state *state.State) (*Roll, error) {
	dsn, err := pq.ParseURL(pgURL)
	if err != nil {
		dsn = pgURL
	}

	dsn += " search_path=" + schema

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

	_, err = conn.ExecContext(ctx, fmt.Sprintf("SET lock_timeout to '%dms'", lockTimeoutMs))
	if err != nil {
		return nil, fmt.Errorf("unable to set lock_timeout: %w", err)
	}

	var pgMajorVersion PGVersion
	err = conn.QueryRowContext(ctx, "SELECT split_part(split_part(version(), ' ', 2), '.', 1)").Scan(&pgMajorVersion)
	if err != nil {
		return nil, fmt.Errorf("unable to retrieve postgres version: %w", err)
	}

	return &Roll{
		pgConn:    conn,
		schema:    schema,
		state:     state,
		pgVersion: PGVersion(pgMajorVersion),
	}, nil
}

func (m *Roll) Init(ctx context.Context) error {
	return m.state.Init(ctx)
}

func (m *Roll) PGVersion() PGVersion {
	return m.pgVersion
}

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
