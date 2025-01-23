// SPDX-License-Identifier: Apache-2.0

package migrations

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/lib/pq"
	"github.com/xataio/pgroll/pkg/db"
)

func createUniqueIndexConcurrently(ctx context.Context, conn db.DB, schemaName string, indexName string, tableName string, columnNames []string) error {
	quotedQualifiedIndexName := pq.QuoteIdentifier(indexName)
	if schemaName != "" {
		quotedQualifiedIndexName = fmt.Sprintf("%s.%s", pq.QuoteIdentifier(schemaName), pq.QuoteIdentifier(indexName))
	}
	for retryCount := 0; retryCount < 5; retryCount++ {
		// Add a unique index to the new column
		// Indexes are created in the same schema with the table automatically. Instead of the qualified one, just pass the index name.
		if err := executeCreateUniqueIndexConcurrently(ctx, conn, indexName, schemaName, tableName, columnNames); err != nil {
			return fmt.Errorf("failed to add unique index %q: %w", indexName, err)
		}

		// Make sure Postgres is done creating the index
		isInProgress, err := isIndexInProgress(ctx, conn, quotedQualifiedIndexName)
		if err != nil {
			return err
		}

		ticker := time.NewTicker(500 * time.Millisecond)
		defer ticker.Stop()
		for isInProgress {
			<-ticker.C
			isInProgress, err = isIndexInProgress(ctx, conn, quotedQualifiedIndexName)
			if err != nil {
				return err
			}
		}

		// Check pg_index to see if it's valid or not. Break if it's valid.
		isValid, err := isIndexValid(ctx, conn, quotedQualifiedIndexName)
		if err != nil {
			return err
		}

		if isValid {
			// success
			return nil
		}

		// If not valid, since Postgres has already given up validating the index,
		// it will remain invalid forever. Drop it and try again.
		_, err = conn.ExecContext(ctx, fmt.Sprintf("DROP INDEX IF EXISTS %s", quotedQualifiedIndexName))
		if err != nil {
			return fmt.Errorf("failed to drop index: %w", err)
		}
	}

	// ran out of retries, return an error
	return fmt.Errorf("failed to create unique index %q", indexName)
}

func executeCreateUniqueIndexConcurrently(ctx context.Context, conn db.DB, indexName string, schemaName string, tableName string, columnNames []string) error {
	// create unique index concurrently
	qualifiedTableName := pq.QuoteIdentifier(tableName)
	if schemaName != "" {
		qualifiedTableName = fmt.Sprintf("%s.%s", pq.QuoteIdentifier(schemaName), pq.QuoteIdentifier(tableName))
	}

	indexQuery := fmt.Sprintf(
		"CREATE UNIQUE INDEX CONCURRENTLY IF NOT EXISTS %s ON %s (%s)",
		indexName,
		qualifiedTableName,
		strings.Join(quoteColumnNames(columnNames), ", "),
	)

	_, err := conn.ExecContext(ctx, indexQuery)

	return err
}

func isIndexInProgress(ctx context.Context, conn db.DB, quotedQualifiedIndexName string) (bool, error) {
	rows, err := conn.QueryContext(ctx, `SELECT EXISTS(
			SELECT * FROM pg_catalog.pg_stat_progress_create_index
			WHERE index_relid = $1::regclass
			)`, quotedQualifiedIndexName)
	if err != nil {
		return false, fmt.Errorf("getting index in progress with name %q: %w", quotedQualifiedIndexName, err)
	}
	if rows == nil {
		// if rows == nil && err != nil, then it means we have queried a `FakeDB`.
		// In that case, we can safely return false.
		return false, nil
	}
	var isInProgress bool
	if err := db.ScanFirstValue(rows, &isInProgress); err != nil {
		return false, fmt.Errorf("scanning index in progress with name %q: %w", quotedQualifiedIndexName, err)
	}

	return isInProgress, nil
}

func isIndexValid(ctx context.Context, conn db.DB, quotedQualifiedIndexName string) (bool, error) {
	rows, err := conn.QueryContext(ctx, `SELECT indisvalid
		FROM pg_catalog.pg_index
		WHERE indexrelid = $1::regclass`,
		quotedQualifiedIndexName)
	if err != nil {
		return false, fmt.Errorf("getting index with name %q: %w", quotedQualifiedIndexName, err)
	}
	if rows == nil {
		// if rows == nil && err != nil, then it means we have queried a fake db.
		// In that case, we can safely return true.
		return true, nil
	}
	var isValid bool
	if err := db.ScanFirstValue(rows, &isValid); err != nil {
		return false, fmt.Errorf("scanning index with name %q: %w", quotedQualifiedIndexName, err)
	}

	return isValid, nil
}
