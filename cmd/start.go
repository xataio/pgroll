// SPDX-License-Identifier: Apache-2.0

package cmd

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/pterm/pterm"
	"github.com/xataio/pgroll/cmd/flags"
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

			return runMigrationFromFile(cmd.Context(), m, fileName, complete)
		},
	}

	startCmd.Flags().BoolVarP(&complete, "complete", "c", false, "Mark the migration as complete")

	return startCmd
}

func runMigrationFromFile(ctx context.Context, m *roll.Roll, fileName string, complete bool) error {
	file, err := os.Open(fileName)
	if err != nil {
		return fmt.Errorf("opening migration file: %w", err)
	}

	migration, err := migrations.ReadMigration(file)
	if err != nil {
		file.Close()
		return fmt.Errorf("reading migration file: %w", err)
	}
	file.Close()

	sp, _ := pterm.DefaultSpinner.WithText("Starting migration...").Start()
	cb := func(n int64) {
		sp.UpdateText(fmt.Sprintf("%d records complete...", n))
	}

	err = m.Start(ctx, migration, cb)
	if err != nil {
		sp.Fail(fmt.Sprintf("Failed to start migration: %s", err))
		return err
	}

	if complete {
		if err = m.Complete(ctx); err != nil {
			sp.Fail(fmt.Sprintf("Failed to complete migration: %s", err))
			return err
		}
	}

	version := migration.Name
	if version == "" {
		version = strings.TrimSuffix(filepath.Base(fileName), filepath.Ext(fileName))
	}
	viewName := roll.VersionedSchemaName(flags.Schema(), version)
	msg := fmt.Sprintf("New version of the schema available under the postgres %q schema", viewName)
	sp.Success(msg)

	return nil
}
