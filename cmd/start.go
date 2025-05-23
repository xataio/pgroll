// SPDX-License-Identifier: Apache-2.0

package cmd

import (
	"context"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"time"

	"github.com/pterm/pterm"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"

	"github.com/xataio/pgroll/cmd/flags"
	"github.com/xataio/pgroll/pkg/backfill"
	"github.com/xataio/pgroll/pkg/migrations"
	"github.com/xataio/pgroll/pkg/roll"
)

func startCmd() *cobra.Command {
	var complete bool
	var batchSize int
	var batchDelay time.Duration

	startCmd := &cobra.Command{
		Use:       "start <file>",
		Short:     "Start a migration for the operations present in the given file",
		Args:      cobra.ExactArgs(1),
		ValidArgs: []string{"file"},
		RunE: func(cmd *cobra.Command, args []string) error {
			ctx := cmd.Context()
			fileName := args[0]

			// Create a roll instance and check if pgroll is initialized
			m, err := NewRollWithInitCheck(ctx)
			if err != nil {
				return err
			}
			defer m.Close()

			// Check whether the schema needs an initial baseline migration
			needsBaseline, err := m.State().HasExistingSchemaWithoutHistory(ctx, m.Schema())
			if err != nil {
				return fmt.Errorf("failed to check for existing schema: %w", err)
			}
			if needsBaseline {
				fmt.Printf("Schema %q is non-empty but has no migration history. Run `pgroll baseline` first\n", m.Schema())
				return nil
			}

			c := backfill.NewConfig(
				backfill.WithBatchSize(batchSize),
				backfill.WithBatchDelay(batchDelay),
			)

			return runMigrationFromFile(ctx, m, fileName, complete, c)
		},
	}

	startCmd.Flags().IntVar(&batchSize, "backfill-batch-size", backfill.DefaultBatchSize, "Number of rows backfilled in each batch")
	startCmd.Flags().DurationVar(&batchDelay, "backfill-batch-delay", backfill.DefaultDelay, "Duration of delay between batch backfills (eg. 1s, 1000ms)")
	startCmd.Flags().BoolVarP(&complete, "complete", "c", false, "Mark the migration as complete")
	startCmd.Flags().BoolP("skip-validation", "s", false, "skip migration validation")

	viper.BindPFlag("SKIP_VALIDATION", startCmd.Flags().Lookup("skip-validation"))

	return startCmd
}

func runMigrationFromFile(ctx context.Context, m *roll.Roll, fileName string, complete bool, c *backfill.Config) error {
	migration, err := migrations.ReadMigration(os.DirFS(filepath.Dir(fileName)), filepath.Base(fileName))
	if err != nil {
		return err
	}

	return runMigration(ctx, m, migration, complete, c)
}

func runMigration(ctx context.Context, m *roll.Roll, migration *migrations.Migration, complete bool, c *backfill.Config) error {
	sp, _ := pterm.DefaultSpinner.WithText("Starting migration...").Start()
	c.AddCallback(func(n int64, total int64) {
		if total > 0 {
			percent := float64(n) / float64(total) * 100
			// Percent can be > 100 if we're on the last batch in which case we still want to display 100.
			percent = math.Min(percent, 100)
			sp.UpdateText(fmt.Sprintf("%d records complete... (%.2f%%)", n, percent))
		} else {
			sp.UpdateText(fmt.Sprintf("%d records complete...", n))
		}
	})

	err := m.Validate(ctx, migration)
	if err != nil {
		sp.Fail(fmt.Sprintf("Failed to start migration: %s", err))
		return err
	}

	err = m.Start(ctx, migration, c)
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
