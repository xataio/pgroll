package roll

import (
	"context"
	"database/sql"

	"github.com/lib/pq"

	"pg-roll/pkg/state"
)

type Roll struct {
	pgConn *sql.DB // TODO abstract sql connection

	// schema we are acting on
	schema string

	state *state.State
}

func New(ctx context.Context, pgURL, schema string, state *state.State) (*Roll, error) {
	dsn, err := pq.ParseURL(pgURL)
	if err != nil {
		dsn = pgURL
	}

	dsn += " search_path=" + schema

	conn, err := sql.Open("postgres", dsn)
	if err != nil {
		return nil, err
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
