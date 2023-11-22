// SPDX-License-Identifier: Apache-2.0

package state

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
