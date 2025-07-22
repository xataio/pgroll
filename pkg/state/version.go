// SPDX-License-Identifier: Apache-2.0

package state

import (
	"context"
	"errors"
	"fmt"

	"github.com/lib/pq"
	"golang.org/x/mod/semver"
)

var ErrNewPgrollSchema = errors.New("pgroll binary version is older than pgroll schema version")

// VersionCompatibility represents the result of comparing pgroll binary and
// state schema versions
type VersionCompatibility int

const (
	VersionCompatCheckSkipped VersionCompatibility = iota
	VersionCompatNotInitialized
	VersionCompatVersionSchemaOlder
	VersionCompatVersionSchemaEqual
	VersionCompatVersionSchemaNewer
)

// VersionCompatibility compares the pgroll version that was used to initialize
// the `State` instance with the version of the pgroll state schema.
func (s *State) VersionCompatibility(ctx context.Context) (VersionCompatibility, error) {
	pgrollVersion := s.pgrollVersion

	// Development versions of pgroll are not checked for compatibility
	if pgrollVersion == "development" {
		return VersionCompatCheckSkipped, nil
	}

	// Only perform compatibility check if pgroll is initialized
	ok, err := s.IsInitialized(ctx)
	if err != nil {
		return 0, fmt.Errorf("failed to check initialization status: %w", err)
	}
	if !ok {
		return VersionCompatCheckSkipped, nil
	}

	// Check if this is a legacy schema (pgroll schema exists but there is no
	// `pgroll_version` table).
	versionTableExists, err := s.versionTableExists(ctx)
	if err != nil {
		return 0, fmt.Errorf("failed to check version table existence: %w", err)
	}
	if !versionTableExists {
		return VersionCompatVersionSchemaOlder, nil
	}

	// Get the pgroll version that was used to initialize the pgroll schema
	schemaVersion, err := s.SchemaVersion(ctx)
	if err != nil {
		return 0, fmt.Errorf("failed to get stored version: %w", err)
	}

	// pgroll schemas created by development versions of pgroll are not checked
	// for compatibility.
	if schemaVersion == "development" {
		return VersionCompatCheckSkipped, nil
	}

	// Ensure both versions have the 'v' prefix for compatibility with Go's
	// semver package
	schemaVersion = ensureVPrefix(schemaVersion)
	pgrollVersion = ensureVPrefix(pgrollVersion)

	// If either the schema version or the pgroll version is invalid, do not make
	// any assumptions about compatibility
	if !semver.IsValid(schemaVersion) || !semver.IsValid(pgrollVersion) {
		return VersionCompatCheckSkipped, nil
	}

	// Canonicalize both versions to ensure they are in the correct format
	schemaVersion = semver.Canonical(schemaVersion)
	pgrollVersion = semver.Canonical(pgrollVersion)

	// Compare versions
	cmp := semver.Compare(schemaVersion, pgrollVersion)
	if cmp < 0 {
		return VersionCompatVersionSchemaOlder, nil
	}
	if cmp > 0 {
		return VersionCompatVersionSchemaNewer, nil
	}

	return VersionCompatVersionSchemaEqual, nil
}

// SchemaVersion retrieves the version stored in the pgroll_version table.
func (s *State) SchemaVersion(ctx context.Context) (string, error) {
	query := fmt.Sprintf("SELECT version FROM %s.pgroll_version ORDER BY initialized_at DESC LIMIT 1",
		pq.QuoteIdentifier(s.schema))

	var version string
	err := s.pgConn.QueryRowContext(ctx, query).Scan(&version)
	if err != nil {
		return "", err
	}

	return version, nil
}

// versionTableExists checks if the pgroll_version table exists in the state
// schema.
func (s *State) versionTableExists(ctx context.Context) (bool, error) {
	query := `SELECT EXISTS (
		SELECT 1 FROM information_schema.tables 
		WHERE table_schema = $1 AND table_name = 'pgroll_version'
	)`

	var exists bool
	err := s.pgConn.QueryRowContext(ctx, query, s.schema).Scan(&exists)
	return exists, err
}

// Ensure that the given version string starts with 'v' to ensure compatibility
// with the`golang.org/x/mod/semver` package
func ensureVPrefix(version string) string {
	if len(version) > 0 && version[0] != 'v' {
		return "v" + version
	}
	return version
}
