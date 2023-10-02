// SPDX-License-Identifier: Apache-2.0

package roll

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/lib/pq"

	"github.com/xataio/pgroll/pkg/state"
)

type Roll struct {
	pgConn *sql.DB // TODO abstract sql connection

	// schema we are acting on
	schema string

	state *state.State
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

	_, err = conn.ExecContext(ctx, "SET LOCAL pgroll.internal to 'TRUE'")
	if err != nil {
		return nil, fmt.Errorf("unable to set pgroll.internal to true: %w", err)
	}

	_, err = conn.ExecContext(ctx, fmt.Sprintf("SET lock_timeout to '%dms'", lockTimeoutMs))
	if err != nil {
		return nil, fmt.Errorf("unable to set lock_timeout: %w", err)
	}

	return &Roll{
		pgConn: conn,
		schema: schema,
		state:  state,
	}, nil
}

func (m *Roll) Init(ctx context.Context) error {
	return m.state.Init(ctx)
}

func (m *Roll) Close() error {
	err := m.state.Close()
	if err != nil {
		return err
	}

	return m.pgConn.Close()
}
