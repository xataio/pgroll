// SPDX-License-Identifier: Apache-2.0

package migrations

import (
	"encoding/json"
	"maps"
)

type UpdaterFn func(operation map[string]any) (map[string]any, error)

// FileUpdater updates raw migration files if they contain any breaking
// changes that have proper updater functions registered.
type FileUpdater struct {
	updaterFns map[string][]UpdaterFn
}

func NewFileUpdater(updaters map[string][]UpdaterFn) *FileUpdater {
	return &FileUpdater{
		updaterFns: updaters,
	}
}

func (u *FileUpdater) Update(rawMigration *RawMigration) (*Migration, error) {
	var ops []map[string]any
	if err := json.Unmarshal(rawMigration.Operations, &ops); err != nil {
		return nil, err
	}
	var err error
	for _, op := range ops {
		for opName, fns := range u.updaterFns {
			// if the operation does not have registered updater function, do nothing
			if _, ok := op[opName]; !ok {
				continue
			}
			// run all registered updater functions on operation
			for _, fn := range fns {
				op, err = fn(op)
				if err != nil {
					return nil, err
				}
			}
		}
	}
	rawMigration.Operations, err = json.Marshal(ops)
	if err != nil {
		return nil, err
	}
	return ParseMigration(rawMigration)
}

// UpdateCreateIndexColumnsList transforms create_index's columns attribute from a string list
// to the new array format, preserving order:
//
// Old format:
//
//	columns: ["name", "email"]
//
// New format:
//
//	columns: [{"column": "name"}, {"column": "email"}]
//
// breaking change was released in v0.10.0
// PR: https://github.com/xataio/pgroll/pull/697
func UpdateCreateIndexColumnsList(op map[string]any) (map[string]any, error) {
	body, err := json.Marshal(op)
	if err != nil {
		return nil, err
	}
	var createIndexOp struct {
		CreateIndex struct {
			Columns []string `json:"columns"`
		} `json:"create_index"`
	}

	// error is ignored here, because it can only happen if the create_index
	// operation does not contain the expected, outdated structure
	if err := json.Unmarshal(body, &createIndexOp); err == nil {
		if createIndexOper, ok := op["create_index"].(map[string]any); ok {
			// Convert directly to new array format, preserving order
			newColumns := make([]map[string]any, 0, len(createIndexOp.CreateIndex.Columns))
			for _, col := range createIndexOp.CreateIndex.Columns {
				newColumns = append(newColumns, map[string]any{"column": col})
			}
			createIndexOper["columns"] = newColumns
		}
	}

	return op, nil
}

// UpdateCreateIndexColumnsMapToArray transforms create_index's columns from
// map to array format:
//
// Old format:
//
//	columns: {"name": {}, "email": {"sort": "DESC"}}
//
// New format:
//
//	columns: [{"column": "name"}, {"column": "email", "sort": "DESC"}]
//
// This Updater should run after UpdateCreateIndexColumnsList in the chain.
func UpdateCreateIndexColumnsMapToArray(ops map[string]any) (map[string]any, error) {
	createIndexOp, ok := ops["create_index"].(map[string]any)
	if !ok {
		return ops, nil
	}

	columns, ok := createIndexOp["columns"]
	if !ok {
		return ops, nil
	}

	// Only convert if it's a map (old format)
	colsMap, ok := columns.(map[string]any)
	if !ok {
		return ops, nil
	}

	// Convert map to array
	newColumns := make([]map[string]any, 0, len(colsMap))
	for colName, settings := range colsMap {
		entry := map[string]any{"column": colName}
		if settingsMap, ok := settings.(map[string]any); ok {
			maps.Copy(entry, settingsMap)
		}
		newColumns = append(newColumns, entry)
	}

	createIndexOp["columns"] = newColumns

	return ops, nil
}
