// SPDX-License-Identifier: Apache-2.0

package migrations

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"

	_ "github.com/lib/pq"
	"gopkg.in/yaml.v3"

	"github.com/xataio/pgroll/pkg/backfill"
	"github.com/xataio/pgroll/pkg/db"
	"github.com/xataio/pgroll/pkg/schema"
)

// Operation is an operation that can be applied to a schema
type Operation interface {
	// Start will return the list of required changes to enable supporting the new schema
	// version in the database (through a view)
	// update the given views to expose the new schema version
	// Returns the table that requires backfilling, if any.
	Start(ctx context.Context, l Logger, conn db.DB, s *schema.Schema) (*StartResult, error)

	// Complete will update the database schema to match the current version
	// after calling Start.
	// This method should be called once the previous version is no longer used.
	Complete(l Logger, conn db.DB, s *schema.Schema) ([]DBAction, error)

	// Rollback will revert the changes made by Start. It is not possible to
	// rollback a completed migration.
	Rollback(l Logger, conn db.DB, s *schema.Schema) ([]DBAction, error)

	// Validate returns a descriptive error if the operation cannot be applied to the given schema.
	Validate(ctx context.Context, s *schema.Schema) error
}

// Createable interface must be implemented for all operations
// that can be created using the CLI create command.
//
// The function must prompt users to configure all attributes of an operation.
//
// Example implementation for OpMyOperation that has 3 attributes: table, column and down:
//
//	func (o *OpMyOperation) Create() {
//		o.Table, _ = pterm.DefaultInteractiveTextInput.WithDefaultText("table").Show()
//		o.Column, _ = pterm.DefaultInteractiveTextInput.WithDefaultText("column").Show()
//		o.Down, _ = pterm.DefaultInteractiveTextInput.WithDefaultText("down").Show()
//	}
type Createable interface {
	Create()
}

// IsolatedOperation is an operation that cannot be executed with other operations
// in the same migration.
type IsolatedOperation interface {
	// IsIsolated defines where this operation is isolated when executed on start, cannot be executed
	// with other operations.
	IsIsolated() bool
}

// RequiresSchemaRefreshOperation is an operation that requires the resulting schema to be refreshed.
type RequiresSchemaRefreshOperation interface {
	// RequiresSchemaRefresh defines if this operation requires the resulting schema to be refreshed when
	// executed on start.
	RequiresSchemaRefresh()
}

type (
	Operations []Operation
	Migration  struct {
		Name          string     `json:"-"`
		VersionSchema string     `json:"version_schema,omitempty"`
		Operations    Operations `json:"operations"`
	}
	RawMigration struct {
		Name          string          `json:"-"`
		VersionSchema string          `json:"version_schema,omitempty"`
		Operations    json.RawMessage `json:"operations"`
	}

	StartResult struct {
		Actions      []DBAction
		BackfillTask *backfill.Task
	}
)

// VersionSchemaName returns the version schema name for the migration.
// It defaults to the migration name if no version schema is set.
func (m *Migration) VersionSchemaName() string {
	if m.VersionSchema != "" {
		return m.VersionSchema
	}
	return m.Name
}

// Validate will check that the migration can be applied to the given schema
// returns a descriptive error if the migration is invalid
func (m *Migration) Validate(ctx context.Context, s *schema.Schema) error {
	for _, op := range m.Operations {
		if isolatedOp, ok := op.(IsolatedOperation); ok {
			if isolatedOp.IsIsolated() && len(m.Operations) > 1 {
				return InvalidMigrationError{Reason: fmt.Sprintf("operation %q cannot be executed with other operations", OperationName(op))}
			}
		}
	}

	for _, op := range m.Operations {
		err := op.Validate(ctx, s)
		if err != nil {
			return err
		}
	}

	return nil
}

// UpdateVirtualSchema updates the in-memory schema representation with the changes
// made by the migration. No changes are made to the physical database.
func (m *Migration) UpdateVirtualSchema(ctx context.Context, s *schema.Schema) error {
	db := &db.FakeDB{}

	// Run `Start` on each operation using the fake DB. Updates will be made to
	// the in-memory schema `s` without touching the physical database.
	for _, op := range m.Operations {
		if _, err := op.Start(ctx, NewNoopLogger(), db, s); err != nil {
			return err
		}
	}
	return nil
}

// UnmarshalYAML implements custom YAML unmarshaling for RawMigration to preserve
// column order in operations. The default yaml→json conversion loses order because
// it goes through Go maps.
func (r *RawMigration) UnmarshalYAML(value *yaml.Node) error {
	// Create temporary struct to unmarshal non-operations fields
	type temp struct {
		VersionSchema string `yaml:"version_schema"`
	}
	var t temp
	if err := value.Decode(&t); err != nil {
		return err
	}
	r.VersionSchema = t.VersionSchema
	
	// Find the operations node and convert it to JSON preserving order
	for i := 0; i < len(value.Content); i += 2 {
		var key string
		if err := value.Content[i].Decode(&key); err != nil {
			return err
		}
		if key == "operations" {
			jsonBytes, err := yamlNodeToJSON(value.Content[i+1])
			if err != nil {
				return fmt.Errorf("converting operations to JSON: %w", err)
			}
			r.Operations = jsonBytes
			return nil
		}
	}
	
	return fmt.Errorf("operations field not found in migration")
}

// yamlNodeToJSON converts a yaml.Node to JSON bytes while preserving key order.
// This manually walks the yaml.Node tree to maintain insertion order for mappings.
func yamlNodeToJSON(node *yaml.Node) ([]byte, error) {
	var buf bytes.Buffer
	
	switch node.Kind {
	case yaml.DocumentNode:
		if len(node.Content) > 0 {
			return yamlNodeToJSON(node.Content[0])
		}
		return []byte("null"), nil
		
	case yaml.MappingNode:
		buf.WriteByte('{')
		for i := 0; i < len(node.Content); i += 2 {
			if i > 0 {
				buf.WriteByte(',')
			}
			
			// Write key
			keyBytes, err := yamlNodeToJSON(node.Content[i])
			if err != nil {
				return nil, err
			}
			buf.Write(keyBytes)
			buf.WriteByte(':')
			
			// Write value
			valueBytes, err := yamlNodeToJSON(node.Content[i+1])
			if err != nil {
				return nil, err
			}
			buf.Write(valueBytes)
		}
		buf.WriteByte('}')
		
	case yaml.SequenceNode:
		buf.WriteByte('[')
		for i, item := range node.Content {
			if i > 0 {
				buf.WriteByte(',')
			}
			itemBytes, err := yamlNodeToJSON(item)
			if err != nil {
				return nil, err
			}
			buf.Write(itemBytes)
		}
		buf.WriteByte(']')
		
	case yaml.ScalarNode:
		switch node.Tag {
		case "!!str":
			return json.Marshal(node.Value)
		case "!!int":
			return []byte(node.Value), nil
		case "!!float":
			return []byte(node.Value), nil
		case "!!bool":
			return []byte(node.Value), nil
		case "!!null":
			return []byte("null"), nil
		default:
			// Try to parse as string if unknown
			return json.Marshal(node.Value)
		}
		
	case yaml.AliasNode:
		return yamlNodeToJSON(node.Alias)
		
	default:
		return nil, fmt.Errorf("unsupported YAML node kind: %v", node.Kind)
	}
	
	return buf.Bytes(), nil
}
