// SPDX-License-Identifier: Apache-2.0

package migrations

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/lib/pq"
	"github.com/xataio/pg-roll/pkg/schema"
)

type OpDropTable struct {
	Name string `json:"name"`
}

var _ Operation = (*OpDropTable)(nil)

func (o *OpDropTable) Start(ctx context.Context, conn *sql.DB, stateSchema string, s *schema.Schema) error {
	s.RemoveTable(o.Name)
	return nil
}

func (o *OpDropTable) Complete(ctx context.Context, conn *sql.DB) error {
	_, err := conn.ExecContext(ctx, fmt.Sprintf("DROP TABLE IF EXISTS %s", pq.QuoteIdentifier(o.Name)))

	return err
}

func (o *OpDropTable) Rollback(ctx context.Context, conn *sql.DB) error {
	return nil
}

func (o *OpDropTable) Validate(ctx context.Context, s *schema.Schema) error {
	table := s.GetTable(o.Name)

	if table == nil {
		return TableDoesNotExistError{Name: o.Name}
	}
	return nil
}
