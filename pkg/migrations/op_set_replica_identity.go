// SPDX-License-Identifier: Apache-2.0

package migrations

import (
	"context"
	"slices"
	"strings"

	"github.com/xataio/pgroll/pkg/backfill"
	"github.com/xataio/pgroll/pkg/db"
	"github.com/xataio/pgroll/pkg/schema"
)

var _ Operation = (*OpSetReplicaIdentity)(nil)

func (o *OpSetReplicaIdentity) Start(ctx context.Context, l Logger, conn db.DB, latestSchema string, s *schema.Schema) (*backfill.Task, error) {
	l.LogOperationStart(o)

	return nil, NewSetReplicaIdentityAction(conn, o.Table, o.Identity.Type, o.Identity.Index).Execute(ctx)
}

func (o *OpSetReplicaIdentity) Complete(l Logger, conn db.DB, s *schema.Schema) ([]DBAction, error) {
	l.LogOperationComplete(o)

	// No-op
	return nil, nil
}

func (o *OpSetReplicaIdentity) Rollback(l Logger, conn db.DB, s *schema.Schema) ([]DBAction, error) {
	l.LogOperationRollback(o)

	// No-op
	return nil, nil
}

func (o *OpSetReplicaIdentity) Validate(ctx context.Context, s *schema.Schema) error {
	identityType := strings.ToUpper(o.Identity.Type)

	table := s.GetTable(o.Table)
	if table == nil {
		return TableDoesNotExistError{Name: o.Table}
	}

	identities := []string{"NOTHING", "DEFAULT", "INDEX", "FULL"}
	if !slices.Contains(identities, identityType) {
		return InvalidReplicaIdentityError{Table: o.Table, Identity: o.Identity.Type}
	}

	if identityType == "INDEX" {
		if _, ok := table.Indexes[o.Identity.Index]; !ok {
			return IndexDoesNotExistError{Name: o.Identity.Index}
		}
	}

	return nil
}
