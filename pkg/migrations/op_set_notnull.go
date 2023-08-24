package migrations

import (
	"context"
	"database/sql"

	"github.com/xataio/pg-roll/pkg/schema"
)

type OpSetNotNull struct {
	Table  string  `json:"table"`
	Column string  `json:"column"`
	Up     *string `json:"up"`
}

var _ Operation = (*OpSetNotNull)(nil)

func (o *OpSetNotNull) Start(ctx context.Context, conn *sql.DB, schemaName string, stateSchema string, s *schema.Schema) error {
	return nil
}

func (o *OpSetNotNull) Complete(ctx context.Context, conn *sql.DB) error {
	return nil
}

func (o *OpSetNotNull) Rollback(ctx context.Context, conn *sql.DB) error {
	return nil
}

func (o *OpSetNotNull) Validate(ctx context.Context, s *schema.Schema) error {
	if o.Up == nil {
		return UpSQLRequiredError{}
	}
	return nil
}
