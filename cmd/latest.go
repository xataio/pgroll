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
	latestCmd := &cobra.Command{
		Use:   "latest",
		Short: "Print the name of the latest schema version or migration",
		Long:  "Print the name of the latest schema version or migration, either in the target database or a local directory",
	}

	// Add subcommands
	latestCmd.AddCommand(latestSchemaCmd())
	latestCmd.AddCommand(latestMigrationCmd())

	return latestCmd
}

func latestSchemaCmd() *cobra.Command {
	var migrationsDir string

	schemaCmd := &cobra.Command{
		Use:     "schema",
		Short:   "Print the latest schema version (migration name prefixed with schema)",
		Example: "latest schema --local ./migrations",
		Args:    cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			ctx := cmd.Context()

			var latestVersion string
			var err error
			if migrationsDir != "" {
				latestVersion, err = latestVersionLocal(ctx, migrationsDir, true)
				if err != nil {
					return fmt.Errorf("failed to get latest version from directory %q: %w", migrationsDir, err)
				}
			} else {
				latestVersion, err = latestVersionRemote(ctx, true)
				if err != nil {
					return fmt.Errorf("failed to get latest version from database: %w", err)
				}
			}

			fmt.Println(latestVersion)

			return nil
		},
	}

	schemaCmd.Flags().StringVarP(&migrationsDir, "local", "l", "", "retrieve the latest version from a local migration directory")

	return schemaCmd
}

func latestMigrationCmd() *cobra.Command {
	var migrationsDir string

	migrationCmd := &cobra.Command{
		Use:     "migration",
		Short:   "Print the latest migration name (without schema prefix)",
		Example: "latest migration --local ./migrations",
		Args:    cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			ctx := cmd.Context()

			var latestVersion string
			var err error
			if migrationsDir != "" {
				latestVersion, err = latestVersionLocal(ctx, migrationsDir, false)
				if err != nil {
					return fmt.Errorf("failed to get latest version from directory %q: %w", migrationsDir, err)
				}
			} else {
				latestVersion, err = latestVersionRemote(ctx, false)
				if err != nil {
					return fmt.Errorf("failed to get latest version from database: %w", err)
				}
			}

			fmt.Println(latestVersion)

			return nil
		},
	}

	migrationCmd.Flags().StringVarP(&migrationsDir, "local", "l", "", "retrieve the latest version from a local migration directory")

	return migrationCmd
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
