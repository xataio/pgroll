// SPDX-License-Identifier: Apache-2.0

package state

import (
	"context"
	"fmt"

	"github.com/lib/pq"
)

// LatestVersion returns the name of the latest version schema, or nil if there
// is none.
func (s *State) LatestVersion(ctx context.Context, schema string) (*string, error) {
	var version *string
	err := s.pgConn.QueryRowContext(ctx,
		fmt.Sprintf("SELECT %s.latest_version($1)", pq.QuoteIdentifier(s.schema)),
		schema).Scan(&version)
	if err != nil {
		return nil, err
	}

	return version, nil
}

// PreviousVersion returns the name of the previous version schema
func (s *State) PreviousVersion(ctx context.Context, schema string) (*string, error) {
	var parent *string
	err := s.pgConn.QueryRowContext(ctx,
		fmt.Sprintf("SELECT %s.previous_version($1)", pq.QuoteIdentifier(s.schema)),
		schema).Scan(&parent)
	if err != nil {
		return nil, err
	}

	return parent, nil
}

// LatestMigration returns the name of the latest migration, or nil if there
// is none.
func (s *State) LatestMigration(ctx context.Context, schema string) (*string, error) {
	var migration *string
	err := s.pgConn.QueryRowContext(ctx,
		fmt.Sprintf("SELECT %s.latest_migration($1)", pq.QuoteIdentifier(s.schema)),
		schema).Scan(&migration)
	if err != nil {
		return nil, err
	}

	return migration, nil
}

// PreviousMigration returns the name of the previous migration, or nil if
// there is none.
func (s *State) PreviousMigration(ctx context.Context, schema string) (*string, error) {
	var parent *string
	err := s.pgConn.QueryRowContext(ctx,
		fmt.Sprintf("SELECT %s.previous_migration($1)", pq.QuoteIdentifier(s.schema)),
		schema).Scan(&parent)
	if err != nil {
		return nil, err
	}

	return parent, nil
}
