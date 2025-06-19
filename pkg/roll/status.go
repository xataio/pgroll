// SPDX-License-Identifier: Apache-2.0

package roll

import "context"

type MigrationStatus string

const (
	NoneMigrationStatus       MigrationStatus = "No migrations"
	InProgressMigrationStatus MigrationStatus = "In progress"
	CompleteMigrationStatus   MigrationStatus = "Complete"
)

// Status describes the current migration status of a database schema.
type Status struct {
	// The schema name.
	Schema string `json:"schema"`

	// The name of the latest version schema.
	Version string `json:"version"`

	// The status of the most recent migration.
	Status MigrationStatus `json:"status"`
}

// Status returns the current migration status of the specified schema
func (m *Roll) Status(ctx context.Context, schema string) (*Status, error) {
	latestVersion, err := m.State().LatestVersion(ctx, schema)
	if err != nil {
		return nil, err
	}
	if latestVersion == nil {
		latestVersion = new(string)
	}

	isActive, err := m.State().IsActiveMigrationPeriod(ctx, schema)
	if err != nil {
		return nil, err
	}

	var status MigrationStatus
	if *latestVersion == "" {
		status = NoneMigrationStatus
	} else if isActive {
		status = InProgressMigrationStatus
	} else {
		status = CompleteMigrationStatus
	}

	return &Status{
		Schema:  schema,
		Version: *latestVersion,
		Status:  status,
	}, nil
}
