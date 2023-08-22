package migrations

import (
	"context"
	"database/sql"

	"github.com/xataio/pg-roll/pkg/schema"
)

var _ Operation = (*OpRawSQL)(nil)

type OpRawSQL struct {
	Up   string `json:"up"`
	Down string `json:"down,omitempty"`
}

func (o *OpRawSQL) Start(ctx context.Context, conn *sql.DB, schemaName, stateSchema string, s *schema.Schema) error {
	_, err := conn.ExecContext(ctx, o.Up)
	return err
}

func (o *OpRawSQL) Complete(ctx context.Context, conn *sql.DB) error {
	return nil
}

func (o *OpRawSQL) Rollback(ctx context.Context, conn *sql.DB) error {
	if o.Down != "" {
		_, err := conn.ExecContext(ctx, o.Down)
		return err
	}
	return nil
}

func (o *OpRawSQL) Validate(ctx context.Context, s *schema.Schema) error {
	if o.Up == "" {
		return EmptyMigration{}
	}

	return nil
}
