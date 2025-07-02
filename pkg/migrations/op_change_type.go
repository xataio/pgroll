// SPDX-License-Identifier: Apache-2.0

package migrations

import (
	"context"

	"github.com/xataio/pgroll/pkg/backfill"
	"github.com/xataio/pgroll/pkg/db"
	"github.com/xataio/pgroll/pkg/schema"
)

type OpChangeType struct {
	Table  string `json:"table"`
	Column string `json:"column"`
	Type   string `json:"type"`
	Up     string `json:"up"`
	Down   string `json:"down"`
}

var _ Operation = (*OpChangeType)(nil)

func (o *OpChangeType) Start(ctx context.Context, l Logger, conn db.DB, s *schema.Schema) (*StartOperation, error) {
	l.LogOperationStart(o)

	table := s.GetTable(o.Table)
	if table == nil {
		return nil, TableDoesNotExistError{Name: o.Table}
	}

	return &StartOperation{BackfillTask: backfill.NewTask(table)}, nil
}

func (o *OpChangeType) Complete(l Logger, conn db.DB, s *schema.Schema) ([]DBAction, error) {
	l.LogOperationComplete(o)

	return nil, nil
}

func (o *OpChangeType) Rollback(l Logger, conn db.DB, s *schema.Schema) ([]DBAction, error) {
	l.LogOperationRollback(o)

	return nil, nil
}

func (o *OpChangeType) Validate(ctx context.Context, s *schema.Schema) error {
	if o.Up == "" {
		return FieldRequiredError{Name: "up"}
	}

	if o.Down == "" {
		return FieldRequiredError{Name: "down"}
	}
	return nil
}
