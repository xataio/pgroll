// SPDX-License-Identifier: Apache-2.0

package migrations

import (
	"context"

	"github.com/xataio/pgroll/pkg/db"
	"github.com/xataio/pgroll/pkg/schema"
)

var _ Operation = (*OpDropIndex)(nil)

func (o *OpDropIndex) Start(ctx context.Context, conn db.DB, latestSchema string, s *schema.Schema) (*schema.Table, error) {
	// no-op
	return nil, nil
}

func (o *OpDropIndex) Complete(ctx context.Context, conn db.DB, s *schema.Schema) error {
	return NewDropIndexAction(conn, o.Name).Execute(ctx)
}

func (o *OpDropIndex) Rollback(ctx context.Context, conn db.DB, s *schema.Schema) error {
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
