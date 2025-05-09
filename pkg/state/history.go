// SPDX-License-Identifier: Apache-2.0

package state

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/lib/pq"
	"github.com/xataio/pgroll/pkg/migrations"
	"github.com/xataio/pgroll/pkg/schema"
)

// HistoryEntry represents a single migration in the migration history
// of a schema
type HistoryEntry struct {
	Migration migrations.RawMigration
	CreatedAt time.Time
}

// BaselineMigration represents a baseline migration record
type BaselineMigration struct {
	Name           string
	CreatedAt      time.Time
	SchemaSnapshot schema.Schema
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

// LatestBaseline returns the most recent baseline migration for a schema,
// or nil if no baseline exists
func (s *State) LatestBaseline(ctx context.Context, schemaName string) (*BaselineMigration, error) {
	// Construct the SQL query to get the latest baseline migration
	query := fmt.Sprintf(`
		SELECT name, created_at, resulting_schema 
		FROM %s.migrations
		WHERE schema = $1
		AND migration_type = 'baseline'
		ORDER BY created_at DESC
		LIMIT 1`,
		pq.QuoteIdentifier(s.schema))

	// Execute the query
	var name string
	var createdAt time.Time
	var rawSchema []byte
	err := s.pgConn.QueryRowContext(ctx, query, schemaName).Scan(&name, &createdAt, &rawSchema)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, nil
		}
		return nil, fmt.Errorf("failed to query baseline migration: %w", err)
	}

	// Unmarshal the schema snapshot
	var schemaSnapshot schema.Schema
	err = json.Unmarshal(rawSchema, &schemaSnapshot)
	if err != nil {
		return nil, fmt.Errorf("failed to unmarshal baseline schema: %w", err)
	}

	return &BaselineMigration{
		Name:           name,
		CreatedAt:      createdAt,
		SchemaSnapshot: schemaSnapshot,
	}, nil
}
