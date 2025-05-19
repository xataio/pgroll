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
		Use:  "latest",
		Args: cobra.NoArgs,
	}

	latestCmd.PersistentFlags().StringP("local", "l", "", "read migrations from a local migrations directory instead of the target database")

	latestCmd.AddCommand(latestSchemaCmd())
	latestCmd.AddCommand(latestVersionCmd())

	return latestCmd
}

func latestSchemaCmd() *cobra.Command {
	latestSchemaCmd := &cobra.Command{
		Use:   "schema",
		Short: "Print the name of the latest schema version in the target database or a local directory",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			migrationsDir, err := cmd.Parent().PersistentFlags().GetString("local")
			if err != nil {
				return err
			}

			latestVersion, err := getLatestVersion(cmd.Context(), migrationsDir, true)
			if err != nil {
				return err
			}

			fmt.Println(latestVersion)
			return nil
		},
	}

	return latestSchemaCmd
}

func latestVersionCmd() *cobra.Command {
	latestVersionCmd := &cobra.Command{
		Use:     "version",
		Short:   "Print the name of the latest migration in the target database or a local directory",
		Example: "latest --local ./migrations",
		Args:    cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			migrationsDir, err := cmd.Parent().PersistentFlags().GetString("local")
			if err != nil {
				return err
			}

			latestVersion, err := getLatestVersion(cmd.Context(), migrationsDir, false)
			if err != nil {
				return err
			}

			fmt.Println(latestVersion)
			return nil
		},
	}

	return latestVersionCmd
}

// getLatestVersion returns the latest migration version from a local
// migrations directory or remote database, optionally prepending the schema
// name to which the migration is applied.
func getLatestVersion(ctx context.Context, migrationsDir string, withSchema bool) (string, error) {
	if migrationsDir != "" {
		return latestVersionLocal(ctx, migrationsDir, withSchema)
	}
	return latestVersionRemote(ctx, withSchema)
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
