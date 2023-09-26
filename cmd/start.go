// SPDX-License-Identifier: Apache-2.0

package cmd

import (
	"fmt"
	"path/filepath"
	"strings"

	"github.com/pterm/pterm"
	"github.com/xataio/pgroll/pkg/migrations"
	"github.com/xataio/pgroll/pkg/roll"

	"github.com/spf13/cobra"
)

func startCmd() *cobra.Command {
	var complete bool

	startCmd := &cobra.Command{
		Use:   "start <file>",
		Short: "Start a migration for the operations present in the given file",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			fileName := args[0]

			m, err := NewRoll(cmd.Context())
			if err != nil {
				return err
			}
			defer m.Close()

			migration, err := migrations.ReadMigrationFile(args[0])
			if err != nil {
				return fmt.Errorf("reading migration file: %w", err)
			}

			sp, _ := pterm.DefaultSpinner.WithText("Starting migration...").Start()
			cb := func(n int64) {
				sp.UpdateText(fmt.Sprintf("%d records complete...", n))
			}

			err = m.Start(cmd.Context(), migration, cb)
			if err != nil {
				sp.Fail(fmt.Sprintf("Failed to start migration: %s", err))
				return err
			}

			if complete {
				if err = m.Complete(cmd.Context()); err != nil {
					sp.Fail(fmt.Sprintf("Failed to complete migration: %s", err))
					return err
				}
			}

			version := strings.TrimSuffix(filepath.Base(fileName), filepath.Ext(fileName))
			viewName := roll.VersionedSchemaName(Schema, version)
			msg := fmt.Sprintf("New version of the schema available under the postgres %q schema", viewName)
			sp.Success(msg)

			return nil
		},
	}

	startCmd.Flags().BoolVarP(&complete, "complete", "c", false, "Mark the migration as complete")

	return startCmd
}
