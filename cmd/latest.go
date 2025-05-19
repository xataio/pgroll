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

func latestCmd() *cobra.Command {
	var withSchema bool
	var migrationsDir string

	latestCmd := &cobra.Command{
		Use:     "latest",
		Short:   "Print the name of the latest schema version, either in the target database or a local directory",
		Example: "latest --local ./migrations",
		Args:    cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			ctx := cmd.Context()

			var latestVersion string
			var err error
			if migrationsDir != "" {
				latestVersion, err = latestVersionLocal(ctx, migrationsDir, withSchema)
				if err != nil {
					return fmt.Errorf("failed to get latest version from directory %q: %w", migrationsDir, err)
				}
			} else {
				latestVersion, err = latestVersionRemote(ctx, withSchema)
				if err != nil {
					return fmt.Errorf("failed to get latest version from database: %w", err)
				}
			}

			fmt.Println(latestVersion)

			return nil
		},
	}

	latestCmd.Flags().BoolVarP(&withSchema, "with-schema", "s", true, "prefix the version with the schema name")
	latestCmd.Flags().StringVarP(&migrationsDir, "local", "l", "", "retrieve the latest version from a local migration directory")

	return latestCmd
}

// latestVersionLocal returns the latest migration version from a local
// migration directory on disk, assuming the migration files are
// lexicographically ordered by filename
func latestVersionLocal(ctx context.Context, migrationsDir string, withSchema bool) (string, error) {
	// Ensure that the directory exists
	info, err := os.Stat(migrationsDir)
	if err != nil {
		return "", err
	}
	if !info.IsDir() {
		return "", fmt.Errorf("not a directory: %q", migrationsDir)
	}

	// Get the latest migration in the directory
	latestVersion, err := roll.LatestVersionLocal(ctx, os.DirFS(migrationsDir))
	if err != nil {
		return "", err
	}

	// Prepend the schema name to the latest version if requested
	if withSchema {
		latestVersion = flags.Schema() + "_" + latestVersion
	}

	return latestVersion, nil
}

// latestVersionRemote returns the latest applied migration version on the target database
func latestVersionRemote(ctx context.Context, withSchema bool) (string, error) {
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

	// Prepend the schema name to the latest version if requested
	if withSchema {
		latestVersion = m.Schema() + "_" + latestVersion
	}

	return latestVersion, nil
}
