package migrations

import (
	"context"
	"encoding/json"
	"fmt"
	"pg-roll/pkg/schema"
	"strings"

	"github.com/lib/pq"
)

var errActiveMigration = fmt.Errorf("there is an active migration already")

// Start will apply the required changes to enable supporting the new schema version
func (m *Migrations) Start(ctx context.Context, migration *Migration) error {
	// check if there is an active migration, create one otherwise
	active, err := m.state.IsActiveMigrationPeriod(ctx)
	if err != nil {
		return err
	}
	if active {
		return errActiveMigration
	}

	// TODO: retrieve current schema + store it as state?
	newSchema := schema.New()

	// create a new active migration (guaranteed to be unique by constraints)
	rawMigration, err := json.Marshal(migration)
	if err != nil {
		return fmt.Errorf("unable to marshal migration: %w", err)
	}
	err = m.state.Start(ctx, migration.Name, string(rawMigration))
	if err != nil {
		return fmt.Errorf("unable to start migration: %w", err)
	}

	// execute operations
	for _, op := range migration.Operations {
		err := op.Start(ctx, m.pgConn, newSchema)
		if err != nil {
			return fmt.Errorf("unable to execute start operation: %w", err)
		}
	}

	// create schema for the new version
	_, err = m.pgConn.ExecContext(ctx, fmt.Sprintf("CREATE SCHEMA IF NOT EXISTS %s", pq.QuoteIdentifier(migration.Name)))
	if err != nil {
		return err
	}

	// create views in the new schema
	for name, table := range newSchema.Tables {
		err = m.createView(ctx, migration.Name, name, table)
		if err != nil {
			return fmt.Errorf("unable to create view: %w", err)
		}
	}

	return nil
}

// Complete will update the database schema to match the current version
func (m *Migrations) Complete(ctx context.Context) error {
	// get current ongoing migration
	name, rawMigration, err := m.state.GetActiveMigration(ctx)
	if err != nil {
		return fmt.Errorf("unable to get active migration: %w", err)
	}

	var migration Migration
	fmt.Println(rawMigration)
	err = json.Unmarshal([]byte(rawMigration), &migration)
	if err != nil {
		return fmt.Errorf("unable to unmarshal migration: %w", err)
	}

	// execute operations
	for _, op := range migration.Operations {
		err := op.Complete(ctx, m.pgConn)
		if err != nil {
			return fmt.Errorf("unable to execute complete operation: %w", err)
		}
	}

	// TODO: drop views from previous version

	// mark as completed
	err = m.state.Complete(ctx, name)
	if err != nil {
		return fmt.Errorf("unable to complete migration: %w", err)
	}

	return nil
}

func (m *Migrations) Rollback(ctx context.Context, version string, ops Operations) error {
	// delete the schema and view for the new version
	_, err := m.pgConn.ExecContext(ctx, fmt.Sprintf("DROP SCHEMA IF EXISTS %s CASCADE", pq.QuoteIdentifier(version)))
	if err != nil {
		return err
	}

	// reverse the order of the operations so that they are undone in the correct order
	for i, j := 0, len(ops)-1; i < j; i, j = i+1, j-1 {
		ops[i], ops[j] = ops[j], ops[i]
	}

	// execute operations
	for _, op := range ops {
		err := op.Rollback(ctx, m.pgConn)
		if err != nil {
			return fmt.Errorf("unable to execute rollback operation: %w", err)
		}
	}

	return nil
}

// create view creates a view for the new version of the schema
func (m *Migrations) createView(ctx context.Context, version string, name string, table schema.Table) error {
	columns := make([]string, 0, len(table.Columns))
	for k, v := range table.Columns {
		columns = append(columns, fmt.Sprintf("%s AS %s", pq.QuoteIdentifier(k), pq.QuoteIdentifier(v.Name)))
	}

	_, err := m.pgConn.ExecContext(ctx,
		fmt.Sprintf("CREATE OR REPLACE VIEW %s.%s AS SELECT %s FROM %s",
			pq.QuoteIdentifier(version),
			pq.QuoteIdentifier(name),
			strings.Join(columns, ","),
			pq.QuoteIdentifier(table.Name)))
	if err != nil {
		return err
	}
	return nil
}
