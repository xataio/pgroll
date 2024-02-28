// SPDX-License-Identifier: Apache-2.0

package roll

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/lib/pq"
	"github.com/xataio/pgroll/pkg/migrations"
	"github.com/xataio/pgroll/pkg/schema"
)

// Start will apply the required changes to enable supporting the new schema version
func (m *Roll) Start(ctx context.Context, migration *migrations.Migration, cbs ...migrations.CallbackFn) error {
	// check if there is an active migration, create one otherwise
	active, err := m.state.IsActiveMigrationPeriod(ctx, m.schema)
	if err != nil {
		return err
	}
	if active {
		return fmt.Errorf("a migration for schema %q is already in progress", m.schema)
	}

	// create a new active migration (guaranteed to be unique by constraints)
	newSchema, err := m.state.Start(ctx, m.schema, migration)
	if err != nil {
		return fmt.Errorf("unable to start migration: %w", err)
	}

	// validate migration
	err = migration.Validate(ctx, newSchema)
	if err != nil {
		if err := m.state.Rollback(ctx, m.schema, migration.Name); err != nil {
			fmt.Printf("failed to rollback migration: %s\n", err)
		}
		return fmt.Errorf("migration is invalid: %w", err)
	}

	// execute operations
	var tablesToBackfill []*schema.Table
	for _, op := range migration.Operations {
		table, err := op.Start(ctx, m.pgConn, m.state.Schema(), newSchema, cbs...)
		if err != nil {
			errRollback := m.Rollback(ctx)

			return errors.Join(
				fmt.Errorf("unable to execute start operation: %w", err),
				errRollback)
		}
		// refresh schema when the op is isolated and requires a refresh (for example raw sql)
		// we don't want to refresh the schema if the operation is not isolated as it would
		// override changes made by other operations
		if _, ok := op.(migrations.RequiresSchemaRefreshOperation); ok {
			if isolatedOp, ok := op.(migrations.IsolatedOperation); ok && isolatedOp.IsIsolated() {
				newSchema, err = m.state.ReadSchema(ctx, m.schema)
				if err != nil {
					return fmt.Errorf("unable to refresh schema: %w", err)
				}
			}
		}
		if table != nil {
			tablesToBackfill = append(tablesToBackfill, table)
		}
	}

	// perform backfill operations for those tables that require it
	for _, table := range tablesToBackfill {
		if err := migrations.Backfill(ctx, m.pgConn, table, cbs...); err != nil {
			return fmt.Errorf("unable to backfill table %q: %w", table.Name, err)
		}
	}

	if m.disableVersionSchemas {
		// skip creating version schemas
		return nil
	}

	// create views for the new version
	return m.ensureViews(ctx, newSchema, migration.Name)
}

func (m *Roll) ensureViews(ctx context.Context, schema *schema.Schema, version string) error {
	// create schema for the new version
	versionSchema := VersionedSchemaName(m.schema, version)
	_, err := m.pgConn.ExecContext(ctx, fmt.Sprintf("CREATE SCHEMA IF NOT EXISTS %s", pq.QuoteIdentifier(versionSchema)))
	if err != nil {
		return err
	}

	// create views in the new schema
	for name, table := range schema.Tables {
		err = m.ensureView(ctx, version, name, table)
		if err != nil {
			return fmt.Errorf("unable to create view: %w", err)
		}
	}

	return nil
}

// Complete will update the database schema to match the current version
func (m *Roll) Complete(ctx context.Context) error {
	// get current ongoing migration
	migration, err := m.state.GetActiveMigration(ctx, m.schema)
	if err != nil {
		return fmt.Errorf("unable to get active migration: %w", err)
	}

	// Drop the old schema
	if !m.disableVersionSchemas {
		prevVersion, err := m.state.PreviousVersion(ctx, m.schema)
		if err != nil {
			return fmt.Errorf("unable to get name of previous version: %w", err)
		}
		if prevVersion != nil {
			versionSchema := VersionedSchemaName(m.schema, *prevVersion)
			_, err = m.pgConn.ExecContext(ctx, fmt.Sprintf("DROP SCHEMA IF EXISTS %s CASCADE", pq.QuoteIdentifier(versionSchema)))
			if err != nil {
				return fmt.Errorf("unable to drop previous version: %w", err)
			}
		}
	}

	// read the current schema
	schema, err := m.state.ReadSchema(ctx, m.schema)
	if err != nil {
		return fmt.Errorf("unable to read schema: %w", err)
	}

	// execute operations
	refreshViews := false
	for _, op := range migration.Operations {
		err := op.Complete(ctx, m.pgConn, schema)
		if err != nil {
			return fmt.Errorf("unable to execute complete operation: %w", err)
		}

		if _, ok := op.(migrations.RequiresSchemaRefreshOperation); ok {
			refreshViews = true
		}
	}

	// recreate views for the new version (if some operations require it, ie SQL)
	if refreshViews && !m.disableVersionSchemas {
		schema, err = m.state.ReadSchema(ctx, m.schema)
		if err != nil {
			return fmt.Errorf("unable to read schema: %w", err)
		}

		err = m.ensureViews(ctx, schema, migration.Name)
		if err != nil {
			return err
		}
	}

	// mark as completed
	err = m.state.Complete(ctx, m.schema, migration.Name)
	if err != nil {
		return fmt.Errorf("unable to complete migration: %w", err)
	}

	return nil
}

func (m *Roll) Rollback(ctx context.Context) error {
	// get current ongoing migration
	migration, err := m.state.GetActiveMigration(ctx, m.schema)
	if err != nil {
		return fmt.Errorf("unable to get active migration: %w", err)
	}

	if !m.disableVersionSchemas {
		// delete the schema and view for the new version
		versionSchema := VersionedSchemaName(m.schema, migration.Name)
		_, err = m.pgConn.ExecContext(ctx, fmt.Sprintf("DROP SCHEMA IF EXISTS %s CASCADE", pq.QuoteIdentifier(versionSchema)))
		if err != nil {
			return err
		}
	}

	// execute operations
	for _, op := range migration.Operations {
		err := op.Rollback(ctx, m.pgConn)
		if err != nil {
			return fmt.Errorf("unable to execute rollback operation: %w", err)
		}
	}

	// roll back the migration
	err = m.state.Rollback(ctx, m.schema, migration.Name)
	if err != nil {
		return fmt.Errorf("unable to rollback migration: %w", err)
	}

	return nil
}

// create view creates a view for the new version of the schema
func (m *Roll) ensureView(ctx context.Context, version, name string, table schema.Table) error {
	columns := make([]string, 0, len(table.Columns))
	for k, v := range table.Columns {
		columns = append(columns, fmt.Sprintf("%s AS %s", pq.QuoteIdentifier(v.Name), pq.QuoteIdentifier(k)))
	}

	// Create view with security_invoker option for PG 15+
	//
	// This ensures that any row level security permissions on the underlying
	// table are respected. `security_invoker` views are not supported in PG 14
	// and below.
	withOptions := ""
	if m.PGVersion() >= PGVersion15 {
		withOptions = "WITH (security_invoker = true)"
	}

	_, err := m.pgConn.ExecContext(ctx,
		fmt.Sprintf("BEGIN; DROP VIEW IF EXISTS %s.%s; CREATE VIEW %s.%s %s AS SELECT %s FROM %s; COMMIT",
			pq.QuoteIdentifier(VersionedSchemaName(m.schema, version)),
			pq.QuoteIdentifier(name),
			pq.QuoteIdentifier(VersionedSchemaName(m.schema, version)),
			pq.QuoteIdentifier(name),
			withOptions,
			strings.Join(columns, ","),
			pq.QuoteIdentifier(table.Name)))
	if err != nil {
		return err
	}
	return nil
}

func VersionedSchemaName(schema string, version string) string {
	return schema + "_" + version
}
