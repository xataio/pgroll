// SPDX-License-Identifier: Apache-2.0

package migrations

import (
	"bytes"
	"encoding/json"
	"fmt"

	"gopkg.in/yaml.v3"
)

// This file provides custom unmarshaling for OpCreateIndex to support both
// array and map formats for the 'columns' field, enabling backward compatibility
// while fixing column ordering issues.
//
// Background:
// - v0.9.0: columns was an array of strings
// - v0.10.0: BREAKING CHANGE to map format to add per-column settings
// - v0.11.0+: This change supports both array (with settings) and map formats
//
// Format detection:
// - Array: [{"name": "col1"}, {"name": "col2"}] - preserves order
// - Map: {"col1": {}, "col2": {}} - order not guaranteed in JSON
//
// The unmarshalers convert both formats to []IndexColumn internally.

// UnmarshalJSON implements custom JSON unmarshaling to support both array and map formats for columns.
// This allows backward compatibility with the old map format while preferring the new array format.
func (o *OpCreateIndex) UnmarshalJSON(data []byte) error {
	// Unmarshal into temporary struct with columns as RawMessage
	var temp struct {
		Method            OpCreateIndexMethod `json:"method"`
		Name              string              `json:"name"`
		Predicate         string              `json:"predicate"`
		StorageParameters string              `json:"storage_parameters"`
		Table             string              `json:"table"`
		Unique            bool                `json:"unique"`
		ColumnsRaw        json.RawMessage     `json:"columns"`
	}
	
	if err := json.Unmarshal(data, &temp); err != nil {
		return err
	}
	
	// Copy all non-columns fields
	o.Method = temp.Method
	o.Name = temp.Name
	o.Predicate = temp.Predicate
	o.StorageParameters = temp.StorageParameters
	o.Table = temp.Table
	o.Unique = temp.Unique
	
	// Detect format and unmarshal columns appropriately
	if len(temp.ColumnsRaw) == 0 {
		return nil
	}
	
	// Check if it's an array (starts with '[') or map (starts with '{')
	trimmed := bytes.TrimSpace(temp.ColumnsRaw)
	if len(trimmed) == 0 {
		return nil
	}
	
	if trimmed[0] == '[' {
		// Array format - unmarshal directly
		if err := json.Unmarshal(temp.ColumnsRaw, &o.Columns); err != nil {
			return fmt.Errorf("unmarshal columns array: %w", err)
		}
	} else if trimmed[0] == '{' {
		// Map format - convert to array and mark for deprecation
		var colMap map[string]IndexField
		if err := json.Unmarshal(temp.ColumnsRaw, &colMap); err != nil {
			return fmt.Errorf("unmarshal columns map: %w", err)
		}
		
		// Convert map to array (order will be non-deterministic for multi-column)
		o.Columns = make([]IndexColumn, 0, len(colMap))
		for name, field := range colMap {
			o.Columns = append(o.Columns, IndexColumn{
				Name:    name,
				Collate: field.Collate,
				Nulls:   field.Nulls,
				Opclass: field.Opclass,
				Sort:    field.Sort,
			})
		}
		
		// Mark this operation as having used map format for deprecation tracking
		o.markAsMapFormat()
	} else {
		return fmt.Errorf("columns must be either an array or object")
	}
	
	return nil
}

// UnmarshalYAML implements custom YAML unmarshaling to support both array and map formats for columns.
// This allows backward compatibility with the old map format while preferring the new array format.
func (o *OpCreateIndex) UnmarshalYAML(node *yaml.Node) error {
	// Manually decode all fields
	for i := 0; i < len(node.Content); i += 2 {
		keyNode := node.Content[i]
		valueNode := node.Content[i+1]
		
		var key string
		if err := keyNode.Decode(&key); err != nil {
			return err
		}
		
		switch key {
		case "name":
			if err := valueNode.Decode(&o.Name); err != nil {
				return err
			}
		case "table":
			if err := valueNode.Decode(&o.Table); err != nil {
				return err
			}
		case "method":
			if err := valueNode.Decode(&o.Method); err != nil {
				return err
			}
		case "predicate":
			if err := valueNode.Decode(&o.Predicate); err != nil {
				return err
			}
		case "storage_parameters":
			if err := valueNode.Decode(&o.StorageParameters); err != nil {
				return err
			}
		case "unique":
			if err := valueNode.Decode(&o.Unique); err != nil {
				return err
			}
		case "columns":
			if valueNode.Kind == yaml.SequenceNode {
				// Array format - decode directly
				if err := valueNode.Decode(&o.Columns); err != nil {
					return fmt.Errorf("decode columns array: %w", err)
				}
			} else if valueNode.Kind == yaml.MappingNode {
				// Map format - convert to array, preserving YAML key order
				o.Columns = make([]IndexColumn, 0)
				
				// Iterate through mapping nodes (keys and values alternate)
				for j := 0; j < len(valueNode.Content); j += 2 {
					var colName string
					if err := valueNode.Content[j].Decode(&colName); err != nil {
						return fmt.Errorf("decode column name: %w", err)
					}
					
					var field IndexField
					if err := valueNode.Content[j+1].Decode(&field); err != nil {
						return fmt.Errorf("decode column field: %w", err)
					}
					
					o.Columns = append(o.Columns, IndexColumn{
						Name:    colName,
						Collate: field.Collate,
						Nulls:   field.Nulls,
						Opclass: field.Opclass,
						Sort:    field.Sort,
					})
				}
				
				// Mark this operation as having used map format for deprecation tracking
				o.markAsMapFormat()
			}
		}
	}
	
	return nil
}
