// SPDX-License-Identifier: Apache-2.0

package migrations

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/xataio/pgroll/pkg/schema"
)

var _ Operation = (*OpDropIndex)(nil)

func (o *OpDropIndex) Start(ctx context.Context, conn *sql.DB, stateSchema string, tr SQLTransformer, s *schema.Schema, cbs ...CallbackFn) (*schema.Table, error) {
	// no-op
	return nil, nil
}

func (o *OpDropIndex) Complete(ctx context.Context, conn *sql.DB, s *schema.Schema) error {
	// drop the index concurrently
	_, err := conn.ExecContext(ctx, fmt.Sprintf("DROP INDEX CONCURRENTLY IF EXISTS %s", o.Name))

	return err
}

func (o *OpDropIndex) Rollback(ctx context.Context, conn *sql.DB, tr SQLTransformer) error {
	// no-op
	return nil
}

func (o *OpDropIndex) Validate(ctx context.Context, s *schema.Schema) error {
	for _, table := range s.Tables {
		_, ok := table.Indexes[o.Name]
		if ok {
			return nil
		}
	}
	return IndexDoesNotExistError{Name: o.Name}
}
