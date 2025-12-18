// SPDX-License-Identifier: Apache-2.0

package migrations

import "fmt"

// This file handles deprecation warnings for the map format of create_index columns.
//
// The map format is deprecated for multi-column and partial indexes because:
// 1. Go map iteration order is randomized
// 2. JSON map key order is not guaranteed by spec
// 3. Query planner needs predictable column order for optimization
//
// Single-column indexes can continue using map format indefinitely (no ordering issues).

// wasMapFormat tracks whether the columns were unmarshaled from map format.
// This is used for deprecation warnings.
type wasMapFormat bool

// deprecationTracker holds metadata about the original format for deprecation handling.
// Maps operation pointer to whether it used map format (true) or array format (false).
var deprecationTracker = make(map[*OpCreateIndex]bool)

// markAsMapFormat marks an operation as having been parsed from map format.
func (o *OpCreateIndex) markAsMapFormat() {
	deprecationTracker[o] = true
}

// isMapFormat returns true if this operation was parsed from map format.
func (o *OpCreateIndex) isMapFormat() bool {
	return deprecationTracker[o]
}

// checkDeprecation logs warnings or returns errors for deprecated map format usage.
func (o *OpCreateIndex) checkDeprecation(l Logger) error {
	if !o.isMapFormat() {
		return nil
	}

	numColumns := len(o.Columns)
	isPartial := o.Predicate != ""

	// Future enforcement: could add deadline if needed
	// For now, just warn indefinitely
	shouldError := false

	if numColumns > 1 || isPartial {
		msg := "DEPRECATION WARNING: Map format for 'columns' is deprecated for multi-column and partial indexes. " +
			"Use array format instead: columns: [{name: col1}, {name: col2}]. " +
			"Map format does not preserve column order which is critical for index performance."

		if shouldError {
			return fmt.Errorf("%s", msg)
		}
		
		// Log deprecation warning
		l.Info(msg)
	}

	return nil
}
