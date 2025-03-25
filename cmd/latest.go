// SPDX-License-Identifier: Apache-2.0

package cmd

import (
	"fmt"
	"os"

	"github.com/spf13/cobra"
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

			m, err := NewRoll(ctx)
			if err != nil {
				return err
			}
			defer m.Close()

			var latestVersion string
			if migrationsDir != "" {
				info, err := os.Stat(migrationsDir)
				if err != nil {
					return fmt.Errorf("failed to stat directory: %w", err)
				}
				if !info.IsDir() {
					return fmt.Errorf("migrations directory %q is not a directory", migrationsDir)
				}

				latestVersion, err = roll.LatestVersionLocal(ctx, os.DirFS(migrationsDir))
				if err != nil {
					return fmt.Errorf("failed to get latest version from directory %q: %w", migrationsDir, err)
				}
			} else {
				// Ensure that pgroll is initialized
				ok, err := m.State().IsInitialized(cmd.Context())
				if err != nil {
					return err
				}
				if !ok {
					return errPGRollNotInitialized
				}

				latestVersion, err = m.LatestVersionRemote(ctx)
				if err != nil {
					return fmt.Errorf("failed to get latest version from database: %w", err)
				}
			}

			var prefix string
			if withSchema {
				prefix = m.Schema() + "_"
			}

			fmt.Printf("%s%s\n", prefix, latestVersion)

			return nil
		},
	}

	latestCmd.Flags().BoolVarP(&withSchema, "with-schema", "s", false, "prefix the version with the schema name")
	latestCmd.Flags().StringVarP(&migrationsDir, "local", "l", "", "retrieve the latest version from a local migration directory")

	return latestCmd
}
