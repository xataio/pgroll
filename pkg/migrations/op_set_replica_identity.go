// SPDX-License-Identifier: Apache-2.0

package migrations

import (
	"context"
	"database/sql"
	"fmt"
	"slices"
	"strings"

	"github.com/lib/pq"
	"github.com/xataio/pgroll/pkg/schema"
)

var _ Operation = (*OpSetReplicaIdentity)(nil)

func (o *OpSetReplicaIdentity) Start(ctx context.Context, conn *sql.DB, stateSchema string, s *schema.Schema, cbs ...CallbackFn) (*schema.Table, error) {
	// build the correct form of the `SET REPLICA IDENTITY` statement based on the`identity type
	identitySQL := strings.ToUpper(o.Identity.Type)
	if identitySQL == "INDEX" {
		identitySQL = fmt.Sprintf("USING INDEX %s", pq.QuoteIdentifier(o.Identity.Index))
	}

	// set the replica identity on the underlying table
	_, err := conn.ExecContext(ctx, fmt.Sprintf("ALTER TABLE %s REPLICA IDENTITY %s",
		pq.QuoteIdentifier(o.Table),
		identitySQL))
	return nil, err
}

func (o *OpSetReplicaIdentity) Complete(ctx context.Context, conn *sql.DB, s *schema.Schema) error {
	// No-op
	return nil
}

func (o *OpSetReplicaIdentity) Rollback(ctx context.Context, conn *sql.DB) error {
	// No-op
	return nil
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
