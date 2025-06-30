// SPDX-License-Identifier: Apache-2.0

package cmd

import (
	"fmt"

	"github.com/spf13/cobra"
	"github.com/xataio/pgroll/cmd/flags"
	"github.com/xataio/pgroll/internal/connstr"
)

func latestURLCmd() *cobra.Command {
	var migrationsDir string

	urlCmd := &cobra.Command{
		Use:       "url",
		Short:     "Print a database connection URL for the latest schema version",
		Long:      "Print a database connection URL for the latest schema version, either from the target database or a local directory",
		Example:   "pgroll latest url <connection-string> --local ./migrations",
		Args:      cobra.MaximumNArgs(1),
		ValidArgs: []string{"connection-string"},
		RunE: func(cmd *cobra.Command, args []string) error {
			ctx := cmd.Context()

			// Default to the Postgres URL from flags, or use the first argument if provided
			pgURL := flags.PostgresURL()
			if len(args) > 0 {
				pgURL = args[0]
			}

			// Get the latest version schema name, either from the remote database or a local directory
			latestVersion, err := latestVersion(ctx, migrationsDir)
			if err != nil {
				return fmt.Errorf("failed to get latest version: %w", err)
			}

			// Append the search_path option to the connection string
			str, err := connstr.AppendSearchPathOption(pgURL, latestVersion)
			if err != nil {
				return fmt.Errorf("failed to add search_path option: %w", err)
			}

			fmt.Println(str)

			return nil
		},
	}

	urlCmd.Flags().StringVarP(&migrationsDir, "local", "l", "", "retrieve the latest schema version from a local migration directory")

	return urlCmd
}
