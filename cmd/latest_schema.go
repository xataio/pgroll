// SPDX-License-Identifier: Apache-2.0

package cmd

import (
	"context"
	"fmt"
	"os"

	"github.com/spf13/cobra"
	"github.com/xataio/pgroll/cmd/flags"
	"github.com/xataio/pgroll/pkg/roll"
)

func latestSchemaCmd() *cobra.Command {
	var migrationsDir string

	schemaCmd := &cobra.Command{
		Use:     "schema",
		Short:   "Print the latest version schema name",
		Example: "latest schema --local ./migrations",
		Args:    cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			ctx := cmd.Context()

			latestVersion, err := latestVersion(ctx, migrationsDir)
			if err != nil {
				return fmt.Errorf("failed to get latest version: %w", err)
			}

			fmt.Println(latestVersion)

			return nil
		},
	}

	schemaCmd.Flags().StringVarP(&migrationsDir, "local", "l", "", "retrieve the latest version from a local migration directory")

	return schemaCmd
}

// latestVersion returns the latest version schema name from either the
// remote database or a local directory.
func latestVersion(ctx context.Context, migrationsDir string) (string, error) {
	if migrationsDir != "" {
		return latestVersionLocal(ctx, migrationsDir)
	}
	return latestVersionRemote(ctx)
}

// latestVersionLocal returns the latest version schema name from a local
// migration directory on disk, assuming the migration files are
// lexicographically ordered by filename
func latestVersionLocal(ctx context.Context, migrationsDir string) (string, error) {
	// Ensure that the directory exists
	info, err := os.Stat(migrationsDir)
	if err != nil {
		return "", err
	}
	if !info.IsDir() {
		return "", fmt.Errorf("not a directory: %q", migrationsDir)
	}

	// Get the latest version schema name from the migrations in the local
	// directory
	latestVersion, err := roll.LatestVersionLocal(ctx, os.DirFS(migrationsDir))
	if err != nil {
		return "", err
	}

	return flags.Schema() + "_" + latestVersion, nil
}

// latestVersionRemote returns the latest version schema name from the target
// database
func latestVersionRemote(ctx context.Context) (string, error) {
	// Create a roll instance and check if pgroll is initialized
	m, err := NewRollWithInitCheck(ctx)
	if err != nil {
		return "", err
	}
	defer m.Close()

	// Get the latest version in the target schema
	latestVersion, err := m.LatestVersionRemote(ctx)
	if err != nil {
		return "", err
	}

	return m.Schema() + "_" + latestVersion, nil
}
