// SPDX-License-Identifier: Apache-2.0

package migrations

import (
	"context"
	"fmt"
	"slices"

	"github.com/lib/pq"

	"github.com/xataio/pgroll/pkg/db"
	"github.com/xataio/pgroll/pkg/schema"
)

// RenameDuplicatedColumn
// * Renames a duplicated column to its original name
// * Renames any foreign keys on the duplicated column to their original name.
// * Validates and renames any temporary `CHECK` constraints on the duplicated column.
func RenameDuplicatedColumn(ctx context.Context, conn db.DB, table *schema.Table, column *schema.Column) error {
	const (
		cRenameColumnSQL           = `ALTER TABLE IF EXISTS %s RENAME COLUMN %s TO %s`
		cRenameConstraintSQL       = `ALTER TABLE IF EXISTS %s RENAME CONSTRAINT %s TO %s`
		cValidateConstraintSQL     = `ALTER TABLE IF EXISTS %s VALIDATE CONSTRAINT %s`
		cSetNotNullSQL             = `ALTER TABLE IF EXISTS %s ALTER COLUMN %s SET NOT NULL`
		cDropConstraintSQL         = `ALTER TABLE IF EXISTS %s DROP CONSTRAINT IF EXISTS %s`
		cCreateUniqueConstraintSQL = `ALTER TABLE IF EXISTS %s ADD CONSTRAINT %s UNIQUE USING INDEX %s`
		cRenameIndexSQL            = `ALTER INDEX IF EXISTS %s RENAME TO %s`
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
	for _, fk := range table.ForeignKeys {
		if !IsDuplicatedName(fk.Name) {
			continue
		}

		if slices.Contains(fk.Columns, TemporaryName(column.Name)) {
			renameConstraintSQL := fmt.Sprintf(cRenameConstraintSQL,
				pq.QuoteIdentifier(table.Name),
				pq.QuoteIdentifier(fk.Name),
				pq.QuoteIdentifier(StripDuplicationPrefix(fk.Name)),
			)

			_, err = conn.ExecContext(ctx, renameConstraintSQL)
			if err != nil {
				return fmt.Errorf("failed to rename foreign key constraint %q: %w", fk.Name, err)
			}
			delete(table.ForeignKeys, fk.Name)
		}
	}

	// Validate and rename any temporary `CHECK` constraints on the duplicated
	// column.
	for _, cc := range table.CheckConstraints {
		if !IsDuplicatedName(cc.Name) {
			continue
		}

		if slices.Contains(cc.Columns, TemporaryName(column.Name)) {
			validateConstraintSQL := fmt.Sprintf(cValidateConstraintSQL,
				pq.QuoteIdentifier(table.Name),
				pq.QuoteIdentifier(cc.Name),
			)

			_, err = conn.ExecContext(ctx, validateConstraintSQL)
			if err != nil {
				return fmt.Errorf("failed to validate check constraint %q: %w", cc.Name, err)
			}

			renameConstraintSQL := fmt.Sprintf(cRenameConstraintSQL,
				pq.QuoteIdentifier(table.Name),
				pq.QuoteIdentifier(cc.Name),
				pq.QuoteIdentifier(StripDuplicationPrefix(cc.Name)),
			)

			_, err = conn.ExecContext(ctx, renameConstraintSQL)
			if err != nil {
				return fmt.Errorf("failed to rename check constraint %q: %w", cc.Name, err)
			}
			delete(table.CheckConstraints, cc.Name)

			// If the constraint is a `NOT NULL` constraint, convert the duplicated
			// unchecked `NOT NULL` constraint into a `NOT NULL` attribute on the
			// column.
			if IsNotNullConstraintName(StripDuplicationPrefix(cc.Name)) {
				// Apply `NOT NULL` attribute to the column. This uses the validated constraint
				setNotNullSQL := fmt.Sprintf(cSetNotNullSQL,
					pq.QuoteIdentifier(table.Name),
					pq.QuoteIdentifier(column.Name),
				)

				_, err = conn.ExecContext(ctx, setNotNullSQL)
				if err != nil {
					return fmt.Errorf("failed to set column not null: %w", err)
				}

				// Drop the constraint
				dropConstraintSQL := fmt.Sprintf(cDropConstraintSQL,
					pq.QuoteIdentifier(table.Name),
					pq.QuoteIdentifier(NotNullConstraintName(column.Name)),
				)

				_, err = conn.ExecContext(ctx, dropConstraintSQL)
				if err != nil {
					return fmt.Errorf("failed to drop not null constraint: %w", err)
				}
			}
		}
	}

	// Rename any indexes on the duplicated column and use unique indexes to
	// create `UNIQUE` constraints.
	for _, idx := range table.Indexes {
		if !IsDuplicatedName(idx.Name) {
			continue
		}

		if slices.Contains(idx.Columns, TemporaryName(column.Name)) {
			// Rename the index to its original name
			renameIndexSQL := fmt.Sprintf(cRenameIndexSQL,
				pq.QuoteIdentifier(idx.Name),
				pq.QuoteIdentifier(StripDuplicationPrefix(idx.Name)),
			)

			_, err = conn.ExecContext(ctx, renameIndexSQL)
			if err != nil {
				return fmt.Errorf("failed to rename index %q: %w", idx.Name, err)
			}

			// Index no longer exists, remove it from the table
			delete(table.Indexes, idx.Name)
		}

		if _, ok := table.UniqueConstraints[StripDuplicationPrefix(idx.Name)]; idx.Unique && ok {
			// Create a unique constraint using the unique index
			createUniqueConstraintSQL := fmt.Sprintf(cCreateUniqueConstraintSQL,
				pq.QuoteIdentifier(table.Name),
				pq.QuoteIdentifier(StripDuplicationPrefix(idx.Name)),
				pq.QuoteIdentifier(StripDuplicationPrefix(idx.Name)),
			)

			_, err = conn.ExecContext(ctx, createUniqueConstraintSQL)
			if err != nil {
				return fmt.Errorf("failed to create unique constraint from index %q: %w", idx.Name, err)
			}
		}

	}

	return nil
}
