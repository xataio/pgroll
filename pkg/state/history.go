// SPDX-License-Identifier: Apache-2.0

package state

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/lib/pq"
	"github.com/xataio/pgroll/pkg/migrations"
)

// HistoryEntry represents a single migration in the migration history
// of a schema
type HistoryEntry struct {
	Migration migrations.RawMigration
	CreatedAt time.Time
}

// SchemaHistory returns all migrations applied to a schema in ascending
// timestamp order
func (s *State) SchemaHistory(ctx context.Context, schema string) ([]HistoryEntry, error) {
	rows, err := s.pgConn.QueryContext(ctx,
		fmt.Sprintf(`SELECT name, migration, created_at
			FROM %[1]s.migrations
			WHERE schema=$1
			AND created_at > COALESCE(
			(
				SELECT MAX(created_at) FROM %[1]s.migrations
				WHERE schema = $1 AND migration_type = 'baseline'
			),
			'-infinity'::timestamptz
			) ORDER BY created_at`,
			pq.QuoteIdentifier(s.schema)), schema)
	if err != nil {
		return nil, err
	}

	var entries []HistoryEntry
	for rows.Next() {
		var name, rawMigration string
		var createdAt time.Time

		if err := rows.Scan(&name, &rawMigration, &createdAt); err != nil {
			return nil, fmt.Errorf("row scan: %w", err)
		}

		var mig migrations.RawMigration
		err = json.Unmarshal([]byte(rawMigration), &mig)
		if err != nil {
			return nil, err
		}

		entries = append(entries, HistoryEntry{
			Migration: mig,
			CreatedAt: createdAt,
		})
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterating rows: %w", err)
	}

	return entries, nil
}
