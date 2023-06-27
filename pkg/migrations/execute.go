package migrations

import (
	"context"
	"fmt"
	"pg-roll/pkg/schema"
	"strings"

	"github.com/lib/pq"
)

// Start will apply the required changes to enable supporting the new schema version
func (m *Migrations) Start(ctx context.Context, version string, ops Operations) error {
	newSchema := schema.New()

	// execute operations
	for _, op := range ops {
		err := op.Start(ctx, m.pgConn, newSchema)
		if err != nil {
			return fmt.Errorf("unable to execute start operation: %w", err)
		}
	}

	// create schema for the new version
	_, err := m.pgConn.ExecContext(ctx, fmt.Sprintf("CREATE SCHEMA IF NOT EXISTS %s", pq.QuoteIdentifier(version)))
	if err != nil {
		return err
	}

	// create views in the new schema
	for name, table := range newSchema.Tables {
		err = m.createView(ctx, version, name, table)
		if err != nil {
			return fmt.Errorf("unable to create view: %w", err)
		}
	}

	return nil
}

// Complete will update the database schema to match the current version
func (m *Migrations) Complete(ctx context.Context, version string, ops Operations) error {
	// execute operations
	for _, op := range ops {
		err := op.Complete(ctx, m.pgConn)
		if err != nil {
			return fmt.Errorf("unable to execute complete operation: %w", err)
		}
	}

	// TODO: once we have state, drop views for previous versions

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
