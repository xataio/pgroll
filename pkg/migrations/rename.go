// SPDX-License-Identifier: Apache-2.0

package migrations

import (
	"context"
	"database/sql"
	"fmt"
	"slices"

	"github.com/lib/pq"
	"github.com/xataio/pgroll/pkg/schema"
)

// RenameDuplicatedColumn renames a duplicated column to its original name and renames any foreign keys
// on the duplicated column to their original name.
func RenameDuplicatedColumn(ctx context.Context, conn *sql.DB, table *schema.Table, column *schema.Column) error {
	const (
		cRenameColumnSQL     = `ALTER TABLE IF EXISTS %s RENAME COLUMN %s TO %s`
		cRenameConstraintSQL = `ALTER TABLE IF EXISTS %s RENAME CONSTRAINT %s TO %s`
	)

	// Rename the old column to the new column name
	renameColumnSQL := fmt.Sprintf(cRenameColumnSQL,
		pq.QuoteIdentifier(table.Name),
		pq.QuoteIdentifier(TemporaryName(column.Name)),
		pq.QuoteIdentifier(column.Name))

	_, err := conn.ExecContext(ctx, renameColumnSQL)
	if err != nil {
		return fmt.Errorf("failed to rename duplicated column %q: %w", column.Name, err)
	}

	// Rename any foreign keys on the duplicated column from their temporary name
	// to their original name
	var renameConstraintSQL string
	for _, fk := range table.ForeignKeys {
		if !IsDuplicatedName(fk.Name) {
			continue
		}

		if slices.Contains(fk.Columns, TemporaryName(column.Name)) {
			renameConstraintSQL = fmt.Sprintf(cRenameConstraintSQL,
				pq.QuoteIdentifier(table.Name),
				pq.QuoteIdentifier(fk.Name),
				pq.QuoteIdentifier(StripDuplicationPrefix(fk.Name)),
			)

			_, err = conn.ExecContext(ctx, renameConstraintSQL)
			if err != nil {
				return fmt.Errorf("failed to rename column constraint %q: %w", fk.Name, err)
			}
		}
	}

	return nil
}
