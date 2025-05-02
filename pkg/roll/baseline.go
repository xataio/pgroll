// SPDX-License-Identifier: Apache-2.0

package roll

import (
	"context"
)

// CreateBaseline creates a baseline migration for an existing database schema.
// This is used when starting pgroll with an existing database - it captures
// the current schema state as a baseline version without applying any changes.
// Future migrations will build upon this baseline version.
func (m *Roll) CreateBaseline(ctx context.Context, baselineVersion string) error {
	// Log the operation
	m.logger.Info("Creating baseline version %q for schema %q", baselineVersion, m.schema)

	// Delegate to state to create the actual baseline migration record
	return m.state.CreateBaseline(ctx, m.schema, baselineVersion)
}
