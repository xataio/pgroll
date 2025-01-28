// SPDX-License-Identifier: Apache-2.0

package roll

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/lib/pq"

	"github.com/xataio/pgroll/pkg/backfill"
	"github.com/xataio/pgroll/pkg/migrations"
	"github.com/xataio/pgroll/pkg/schema"
)

// Start will apply the required changes to enable supporting the new schema version
func (m *Roll) Start(ctx context.Context, migration *migrations.Migration, cbs ...backfill.CallbackFn) error {
	tablesToBackfill, err := m.StartDDLOperations(ctx, migration, cbs...)
	if err != nil {
		return err
	}

	// perform backfills for the tables that require it
	return m.performBackfills(ctx, tablesToBackfill, cbs...)
}

// StartDDLOperations performs the DDL operations for the migration. This does
// not include running backfills for any modified tables.
func (m *Roll) StartDDLOperations(ctx context.Context, migration *migrations.Migration, cbs ...backfill.CallbackFn) ([]*schema.Table, error) {
	// check if there is an active migration, create one otherwise
	active, err := m.state.IsActiveMigrationPeriod(ctx, m.schema)
	if err != nil {
		return nil, err
	}
	if active {
		return nil, fmt.Errorf("a migration for schema %q is already in progress", m.schema)
	}

	// create a new active migration (guaranteed to be unique by constraints)
	newSchema, err := m.state.Start(ctx, m.schema, migration)
	if err != nil {
		return nil, fmt.Errorf("unable to start migration: %w", err)
	}

	// validate migration
	if !m.skipValidation {
		err = migration.Validate(ctx, newSchema)
		if err != nil {
			if err := m.state.Rollback(ctx, m.schema, migration.Name); err != nil {
				fmt.Printf("failed to rollback migration: %s\n", err)
			}
			return nil, fmt.Errorf("migration is invalid: %w", err)
		}
	}

	// run any BeforeStartDDL hooks
	if m.migrationHooks.BeforeStartDDL != nil {
		if err := m.migrationHooks.BeforeStartDDL(m); err != nil {
			return nil, fmt.Errorf("failed to execute BeforeStartDDL hook: %w", err)
		}
	}

	// defer execution of any AfterStartDDL hooks
	if m.migrationHooks.AfterStartDDL != nil {
		defer m.migrationHooks.AfterStartDDL(m)
	}

	// Get the name of the latest version schema
	// This is created after ops have started but ops need to know what it will
	// be called in order to set up any up/down triggers
	latestVersion, err := m.state.LatestVersion(ctx, m.schema)
	if err != nil {
		return nil, fmt.Errorf("unable to get name of latest version: %w", err)
	}
	latestSchema := VersionedSchemaName(m.schema, *latestVersion)

	// Reread the latest schema as validation may have updated the schema object
	// in memory.
	newSchema, err = m.state.ReadSchema(ctx, m.schema)
	if err != nil {
		return nil, fmt.Errorf("unable to read schema: %w", err)
	}

	// execute operations
	var tablesToBackfill []*schema.Table
	for _, op := range migration.Operations {
		table, err := op.Start(ctx, m.pgConn, latestSchema, m.sqlTransformer, newSchema, cbs...)
		if err != nil {
			errRollback := m.Rollback(ctx)

			return nil, errors.Join(
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
					return nil, fmt.Errorf("unable to refresh schema: %w", err)
				}
			}
		}
		if table != nil {
			tablesToBackfill = append(tablesToBackfill, table)
		}
	}

	if m.disableVersionSchemas || migration.ContainsRawSQLOperation() && m.noVersionSchemaForRawSQL {
		// skip creating version schemas
		return tablesToBackfill, nil
	}

	// create views for the new version
	if err := m.ensureViews(ctx, newSchema, migration.Name); err != nil {
		return nil, err
	}

	return tablesToBackfill, nil
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
		if table.Deleted {
			continue
		}
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
	if !m.disableVersionSchemas && (!migration.ContainsRawSQLOperation() || !m.noVersionSchemaForRawSQL) {
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
	currentSchema, err := m.state.ReadSchema(ctx, m.schema)
	if err != nil {
		return fmt.Errorf("unable to read schema: %w", err)
	}

	// run any BeforeCompleteDDL hooks
	if m.migrationHooks.BeforeCompleteDDL != nil {
		if err := m.migrationHooks.BeforeCompleteDDL(m); err != nil {
			return fmt.Errorf("failed to execute BeforeCompleteDDL hook: %w", err)
		}
	}

	// defer execution of any AfterCompleteDDL hooks
	if m.migrationHooks.AfterCompleteDDL != nil {
		defer m.migrationHooks.AfterCompleteDDL(m)
	}

	// execute operations
	refreshViews := false
	for _, op := range migration.Operations {
		err := op.Complete(ctx, m.pgConn, m.sqlTransformer, currentSchema)
		if err != nil {
			return fmt.Errorf("unable to execute complete operation: %w", err)
		}

		if _, ok := op.(migrations.RequiresSchemaRefreshOperation); ok {
			if _, ok := op.(*migrations.OpRawSQL); !ok || !m.noVersionSchemaForRawSQL {
				refreshViews = true
			}
		}
	}

	// recreate views for the new version (if some operations require it, ie SQL)
	if refreshViews && !m.disableVersionSchemas {
		currentSchema, err = m.state.ReadSchema(ctx, m.schema)
		if err != nil {
			return fmt.Errorf("unable to read schema: %w", err)
		}

		err = m.ensureViews(ctx, currentSchema, migration.Name)
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

// Rollback will revert the changes made by the migration
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

	// get the name of the previous version of the schema
	previousVersion, err := m.state.PreviousVersion(ctx, m.schema)
	if err != nil {
		return fmt.Errorf("unable to get name of previous version: %w", err)
	}

	// get the schema after the previous migration was applied
	schema := schema.New()
	if previousVersion != nil {
		schema, err = m.state.SchemaAfterMigration(ctx, m.schema, *previousVersion)
		if err != nil {
			return fmt.Errorf("unable to read schema: %w", err)
		}
	}

	// update the in-memory schema with the results of applying the migration
	migration.UpdateVirtualSchema(ctx, schema)

	// roll back operations in reverse order
	for i := len(migration.Operations) - 1; i >= 0; i-- {
		err := migration.Operations[i].Rollback(ctx, m.pgConn, m.sqlTransformer, schema)
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
func (m *Roll) ensureView(ctx context.Context, version, name string, table *schema.Table) error {
	columns := make([]string, 0, len(table.Columns))
	for k, v := range table.Columns {
		if !v.Deleted {
			columns = append(columns, fmt.Sprintf("%s AS %s", pq.QuoteIdentifier(v.Name), pq.QuoteIdentifier(k)))
		}
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

func (m *Roll) performBackfills(ctx context.Context, tables []*schema.Table, cbs ...backfill.CallbackFn) error {
	bf := backfill.New(m.pgConn,
		backfill.WithBatchSize(m.backfillBatchSize),
		backfill.WithBatchDelay(m.backfillBatchDelay),
		backfill.WithCallbacks(cbs...),
		backfill.WithStateSchema(m.state.Schema()),
	)

	for _, table := range tables {
		if err := bf.Start(ctx, table); err != nil {
			errRollback := m.Rollback(ctx)

			return errors.Join(
				fmt.Errorf("unable to backfill table %q: %w", table.Name, err),
				errRollback)
		}
	}

	return nil
}

func VersionedSchemaName(schema string, version string) string {
	return schema + "_" + version
}
