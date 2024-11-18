// SPDX-License-Identifier: Apache-2.0

package cmd

import (
	"context"
	"fmt"
	"os"

	"github.com/pterm/pterm"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"

	"github.com/xataio/pgroll/cmd/flags"
	"github.com/xataio/pgroll/pkg/migrations"
	"github.com/xataio/pgroll/pkg/roll"
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

	startCmd.Flags().BoolP("skip-validation", "s", false, "skip migration validation")
	viper.BindPFlag("SKIP_VALIDATION", startCmd.Flags().Lookup("skip-validation"))

	return startCmd
}

func runMigrationFromFile(ctx context.Context, m *roll.Roll, fileName string, complete bool) error {
	migration, err := readMigration(fileName)
	if err != nil {
		return err
	}

	return runMigration(ctx, m, migration, complete)
}

func runMigration(ctx context.Context, m *roll.Roll, migration *migrations.Migration, complete bool) error {
	sp, _ := pterm.DefaultSpinner.WithText("Starting migration...").Start()
	cb := func(n int64) {
		sp.UpdateText(fmt.Sprintf("%d records complete...", n))
	}

	err := m.Start(ctx, migration, cb)
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
	viewName := roll.VersionedSchemaName(flags.Schema(), version)
	msg := fmt.Sprintf("New version of the schema available under the postgres %q schema", viewName)
	sp.Success(msg)

	return nil
}

func readMigration(fileName string) (*migrations.Migration, error) {
	file, err := os.Open(fileName)
	if err != nil {
		return nil, fmt.Errorf("opening migration file: %w", err)
	}
	defer file.Close()

	migration, err := migrations.ReadMigration(file)
	if err != nil {
		return nil, fmt.Errorf("reading migration file: %w", err)
	}

	return migration, nil
}
