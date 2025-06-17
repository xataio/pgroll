// SPDX-License-Identifier: Apache-2.0

package cmd

import (
	"context"
	"fmt"
	"os"

	"github.com/spf13/cobra"
	"github.com/xataio/pgroll/pkg/roll"
)

func latestMigrationCmd() *cobra.Command {
	var migrationsDir string

	migrationCmd := &cobra.Command{
		Use:     "migration",
		Short:   "Print the latest migration name",
		Example: "latest migration --local ./migrations",
		Args:    cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			ctx := cmd.Context()

			latestVersion, err := latestMigrationName(ctx, migrationsDir)
			if err != nil {
				return fmt.Errorf("failed to get latest migration: %w", err)
			}

			fmt.Println(latestVersion)

			return nil
		},
	}

	migrationCmd.Flags().StringVarP(&migrationsDir, "local", "l", "", "retrieve the latest migration from a local migration directory")

	return migrationCmd
}

// latestMigrationName returns the latest migration name from either the
// remote database or a local directory.
func latestMigrationName(ctx context.Context, migrationsDir string) (string, error) {
	if migrationsDir != "" {
		return latestMigrationNameLocal(ctx, migrationsDir)
	}
	return latestMigrationNameRemote(ctx)
}

// latestMigrationNameLocal returns the latest migration name from a local
// migration directory on disk, assuming the migration files are
// lexicographically ordered by filename.
func latestMigrationNameLocal(ctx context.Context, migrationsDir string) (string, error) {
	// Ensure that the directory exists
	info, err := os.Stat(migrationsDir)
	if err != nil {
		return "", err
	}
	if !info.IsDir() {
		return "", fmt.Errorf("not a directory: %q", migrationsDir)
	}

	// Get the latest migration name from the migrations in the local directory
	latestName, err := roll.LatestMigrationNameLocal(ctx, os.DirFS(migrationsDir))
	if err != nil {
		return "", err
	}

	return latestName, nil
}

// latestMigrationNameRemote returns the latest migration name from the target
// database.
func latestMigrationNameRemote(ctx context.Context) (string, error) {
	// Create a roll instance and check if pgroll is initialized
	m, err := NewRollWithInitCheck(ctx)
	if err != nil {
		return "", err
	}
	defer m.Close()

	// Get the name of the latest migration in the target schema
	latestName, err := m.LatestMigrationNameRemote(ctx)
	if err != nil {
		return "", err
	}

	return latestName, nil
}
