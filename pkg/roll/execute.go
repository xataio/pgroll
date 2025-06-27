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

func (m *Roll) Validate(ctx context.Context, migration *migrations.Migration) error {
	if m.skipValidation {
		return nil
	}
	lastSchema, err := m.state.ReadSchema(ctx, m.schema)
	if err != nil {
		return err
	}
	err = migration.Validate(ctx, lastSchema)
	if err != nil {
		return fmt.Errorf("migration '%s' is invalid: %w", migration.Name, err)
	}
	return nil
}

// Start will apply the required changes to enable supporting the new schema version
func (m *Roll) Start(ctx context.Context, migration *migrations.Migration, cfg *backfill.Config) error {
	// Fail early if we have existing schema without migration history
	hasExistingSchema, err := m.state.HasExistingSchemaWithoutHistory(ctx, m.schema)
	if err != nil {
		return fmt.Errorf("failed to check for existing schema: %w", err)
	}
	if hasExistingSchema {
		return ErrExistingSchemaWithoutHistory
	}

	m.logger.LogMigrationStart(migration)

	if err := m.Validate(ctx, migration); err != nil {
		return err
	}

	job, err := m.StartDDLOperations(ctx, migration)
	if err != nil {
		return err
	}

	// perform backfills for the tables that require it
	return m.performBackfills(ctx, job, cfg)
}

// StartDDLOperations performs the DDL operations for the migration. This does
// not include running backfills for any modified tables.
func (m *Roll) StartDDLOperations(ctx context.Context, migration *migrations.Migration) (*backfill.Job, error) {
	// check if there is an active migration, create one otherwise
	active, err := m.state.IsActiveMigrationPeriod(ctx, m.schema)
	if err != nil {
		return nil, err
	}
	if active {
		return nil, fmt.Errorf("a migration for schema %q is already in progress", m.schema)
	}

	// create a new active migration (guaranteed to be unique by constraints)
	if err = m.state.Start(ctx, m.schema, migration); err != nil {
		return nil, fmt.Errorf("unable to start migration: %w", err)
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

	// Construct the full name of the version schema that will be created by this
	// migration. The version schema is created after operations have completed
	// but ops need to know the name in advance in order to construct backfill
	// triggers.
	versionSchemaName := VersionedSchemaName(m.schema, migration.VersionSchemaName())

	// Reread the latest schema as validation may have updated the schema object
	// in memory.
	newSchema, err := m.state.ReadSchema(ctx, m.schema)
	if err != nil {
		return nil, fmt.Errorf("unable to read schema: %w", err)
	}

	// execute operations
	job := backfill.NewJob(m.schema, versionSchemaName)
	for _, op := range migration.Operations {
		actions, task, err := op.Start(ctx, m.logger, m.pgConn, newSchema)
		if err != nil {
			return nil, fmt.Errorf("unable to collect actions for start %q migration: %w", migration.Name, err)
		}
		for _, action := range actions {
			if err := action.Execute(ctx); err != nil {
				errRollback := m.Rollback(ctx)
				if errRollback != nil {
					return nil, errors.Join(
						fmt.Errorf("unable to execute start operation of %q: %w", migration.Name, err),
						fmt.Errorf("unable to roll back failed operation: %w", errRollback))
				}
				return nil, fmt.Errorf("failed to start %q migration, changes rolled back: %w", migration.Name, err)
			}
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
		if task != nil {
			job.AddTask(task)
		}
	}

	// create views for the new version
	if !m.disableVersionSchemas {
		if err := m.ensureViews(ctx, newSchema, migration); err != nil {
			return nil, err
		}
	}

	return job, nil
}

func (m *Roll) ensureViews(ctx context.Context, schema *schema.Schema, mig *migrations.Migration) error {
	versionSchema := VersionedSchemaName(m.schema, mig.VersionSchemaName())
	_, err := m.pgConn.ExecContext(ctx, fmt.Sprintf("CREATE SCHEMA IF NOT EXISTS %s", pq.QuoteIdentifier(versionSchema)))
	if err != nil {
		return err
	}

	// create views in the new schema
	for name, table := range schema.Tables {
		if table.Deleted {
			continue
		}
		err = m.ensureView(ctx, mig.VersionSchemaName(), name, table)
		if err != nil {
			return fmt.Errorf("unable to create view: %w", err)
		}
	}

	m.logger.LogSchemaCreation(mig.VersionSchemaName(), versionSchema)

	return nil
}

// Complete will update the database schema to match the current version
func (m *Roll) Complete(ctx context.Context) error {
	// get current ongoing migration
	migration, err := m.state.GetActiveMigration(ctx, m.schema)
	if err != nil {
		return fmt.Errorf("unable to get active migration: %w", err)
	}

	m.logger.LogMigrationComplete(migration)

	// Drop the old version schema if there is one
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
		actions, err := op.Complete(m.logger, m.pgConn, currentSchema)
		if err != nil {
			return fmt.Errorf("unable to collect actions for complete operation: %w", err)
		}

		for _, action := range actions {
			if err := action.Execute(ctx); err != nil {
				return fmt.Errorf("unable to execute complete operation: %w", err)
			}
		}

		currentSchema, err = m.state.ReadSchema(ctx, m.schema)
		if err != nil {
			return fmt.Errorf("unable to read schema: %w", err)
		}

		if _, ok := op.(migrations.RequiresSchemaRefreshOperation); ok {
			refreshViews = true
		}
	}

	// recreate views for the new version (if some operations require it, ie SQL)
	if refreshViews && !m.disableVersionSchemas {
		currentSchema, err = m.state.ReadSchema(ctx, m.schema)
		if err != nil {
			return fmt.Errorf("unable to read schema: %w", err)
		}

		err = m.ensureViews(ctx, currentSchema, migration)
		if err != nil {
			return err
		}
	}

	// mark as completed
	err = m.state.Complete(ctx, m.schema, migration.Name)
	if err != nil {
		return fmt.Errorf("unable to complete migration: %w", err)
	}

	m.logger.LogMigrationComplete(migration)

	return nil
}

// Rollback will revert the changes made by the migration
func (m *Roll) Rollback(ctx context.Context) error {
	// get current ongoing migration
	migration, err := m.state.GetActiveMigration(ctx, m.schema)
	if err != nil {
		return fmt.Errorf("unable to get active migration: %w", err)
	}

	m.logger.LogMigrationRollback(migration)

	// delete the schema and views for the new version
	versionSchema := VersionedSchemaName(m.schema, migration.VersionSchemaName())
	_, err = m.pgConn.ExecContext(ctx, fmt.Sprintf("DROP SCHEMA IF EXISTS %s CASCADE", pq.QuoteIdentifier(versionSchema)))
	if err != nil {
		return err
	}

	m.logger.LogSchemaDeletion(migration.Name, versionSchema)

	// get the name of the previous migration
	previousMigration, err := m.state.PreviousMigration(ctx, m.schema)
	if err != nil {
		return fmt.Errorf("unable to get name of previous version: %w", err)
	}

	// get the schema after the previous migration was applied
	schema := schema.New()
	if previousMigration != nil {
		schema, err = m.state.SchemaAfterMigration(ctx, m.schema, *previousMigration)
		if err != nil {
			return fmt.Errorf("unable to read schema: %w", err)
		}
	}

	// update the in-memory schema with the results of applying the migration
	if err := migration.UpdateVirtualSchema(ctx, schema); err != nil {
		return fmt.Errorf("unable to replay changes to in-memory schema: %w", err)
	}

	// roll back operations in reverse order
	for i := len(migration.Operations) - 1; i >= 0; i-- {
		actions, err := migration.Operations[i].Rollback(m.logger, m.pgConn, schema)
		if err != nil {
			return fmt.Errorf("unable to collect actions for rollback operation: %w", err)
		}
		for _, a := range actions {
			if err := a.Execute(ctx); err != nil {
				return fmt.Errorf("unable to execute rollback operation: %w", err)
			}
		}
	}

	// roll back the migration
	err = m.state.Rollback(ctx, m.schema, migration.Name)
	if err != nil {
		return fmt.Errorf("unable to rollback migration: %w", err)
	}

	m.logger.LogMigrationRollbackComplete(migration)

	return nil
}

// create view creates a view for the new version of the schema
func (m *Roll) ensureView(ctx context.Context, version, name string, table *schema.Table) error {
	columns := make([]string, 0, len(table.Columns))
	defaults := make(map[string]string, len(table.Columns))
	for k, v := range table.Columns {
		if !v.Deleted {
			columns = append(columns, fmt.Sprintf("%s AS %s", pq.QuoteIdentifier(v.Name), pq.QuoteIdentifier(k)))
			if v.Default != nil {
				defaults[k] = *v.Default
			}
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

	// We must set column default values for the views directly, as the
	// values are not kept from the underlying tables.
	var addDefaultsToView string
	for column, defaultVal := range defaults {
		addDefaultsToView += fmt.Sprintf("ALTER VIEW %s.%s ALTER %s SET DEFAULT %s; ",
			pq.QuoteIdentifier(VersionedSchemaName(m.schema, version)),
			pq.QuoteIdentifier(name),
			pq.QuoteIdentifier(column),
			defaultVal)
	}
	_, err := m.pgConn.ExecContext(ctx,
		fmt.Sprintf("BEGIN; DROP VIEW IF EXISTS %s.%s; CREATE VIEW %s.%s %s AS SELECT %s FROM %s; %s COMMIT",
			pq.QuoteIdentifier(VersionedSchemaName(m.schema, version)),
			pq.QuoteIdentifier(name),
			pq.QuoteIdentifier(VersionedSchemaName(m.schema, version)),
			pq.QuoteIdentifier(name),
			withOptions,
			strings.Join(columns, ","),
			pq.QuoteIdentifier(table.Name),
			addDefaultsToView))
	if err != nil {
		return err
	}
	return nil
}

func (m *Roll) performBackfills(ctx context.Context, job *backfill.Job, cfg *backfill.Config) error {
	bf := backfill.New(m.pgConn, cfg)

	bf.CreateTriggers(ctx, job)

	for _, table := range job.Tables {
		m.logger.LogBackfillStart(table.Name)

		if err := bf.Start(ctx, table); err != nil {
			errRollback := m.Rollback(ctx)

			return errors.Join(
				fmt.Errorf("unable to backfill table %q: %w", table.Name, err),
				errRollback)
		}

		m.logger.LogBackfillComplete(table.Name)
	}

	return nil
}

func VersionedSchemaName(schema string, version string) string {
	return schema + "_" + version
}
