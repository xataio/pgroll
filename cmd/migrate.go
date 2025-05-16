// SPDX-License-Identifier: Apache-2.0

package cmd

import (
	"context"
	"fmt"
	"os"
	"time"

	"github.com/spf13/cobra"

	"github.com/xataio/pgroll/pkg/backfill"
	"github.com/xataio/pgroll/pkg/migrations"
	"github.com/xataio/pgroll/pkg/roll"
)

func migrateCmd() *cobra.Command {
	var complete bool
	var batchSize int
	var batchDelay time.Duration

	migrateCmd := &cobra.Command{
		Use:       "migrate <directory>",
		Short:     "Apply outstanding migrations from a directory to a database",
		Example:   "migrate ./migrations",
		Args:      cobra.ExactArgs(1),
		ValidArgs: []string{"directory"},
		RunE: func(cmd *cobra.Command, args []string) error {
			ctx := cmd.Context()
			migrationsDir := args[0]

			// Create a roll instance and check if pgroll is initialized
			m, err := NewRollWithInitCheck(ctx)
			if err != nil {
				return err
			}
			defer m.Close()

			latestVersion, err := m.State().LatestVersion(ctx, m.Schema())
			if err != nil {
				return fmt.Errorf("unable to determine latest version: %w", err)
			}

			active, err := m.State().IsActiveMigrationPeriod(ctx, m.Schema())
			if err != nil {
				return fmt.Errorf("unable to determine active migration period: %w", err)
			}
			if active {
				fmt.Printf("migration %q is active\n", *latestVersion)
				return nil
			}

			info, err := os.Stat(migrationsDir)
			if err != nil {
				return fmt.Errorf("failed to stat directory: %w", err)
			}
			if !info.IsDir() {
				return fmt.Errorf("migrations directory %q is not a directory", migrationsDir)
			}

			// Check whether the schema needs an initial baseline migration
			needsBaseline, err := m.State().HasExistingSchemaWithoutHistory(ctx, m.Schema())
			if err != nil {
				return fmt.Errorf("failed to check for existing schema: %w", err)
			}
			if needsBaseline {
				fmt.Printf("Schema %q is non-empty but has no migration history. Run `pgroll baseline` first\n", m.Schema())
				return nil
			}

			migs, err := m.UnappliedMigrations(ctx, os.DirFS(migrationsDir))
			if err != nil {
				return fmt.Errorf("failed to get migrations to apply: %w", err)
			}

			if len(migs) == 0 {
				fmt.Println("Database is up to date; no migrations to apply")
				return nil
			}

			backfillConfig := backfill.NewConfig(
				backfill.WithBatchSize(batchSize),
				backfill.WithBatchDelay(batchDelay),
			)

			// Run all migrations after the latest version up to the final migration,
			// completing each one.
			for _, mig := range migs[:len(migs)-1] {
				if err := runRawMigration(ctx, m, mig, true, backfillConfig); err != nil {
					return fmt.Errorf("failed to run migration file %q: %w", mig.Name, err)
				}
			}

			// Run the final migration, completing it only if requested.
			return runRawMigration(ctx, m, migs[len(migs)-1], complete, backfillConfig)
		},
	}

	migrateCmd.Flags().IntVar(&batchSize, "backfill-batch-size", backfill.DefaultBatchSize, "Number of rows backfilled in each batch")
	migrateCmd.Flags().DurationVar(&batchDelay, "backfill-batch-delay", backfill.DefaultDelay, "Duration of delay between batch backfills (eg. 1s, 1000ms)")
	migrateCmd.Flags().BoolVarP(&complete, "complete", "c", false, "complete the final migration rather than leaving it active")

	return migrateCmd
}

func runRawMigration(ctx context.Context, m *roll.Roll, migration *migrations.RawMigration, complete bool, c *backfill.Config) error {
	parsedMigration, err := migrations.ParseMigration(migration)
	if err != nil {
		return fmt.Errorf("failed to parse migration %q: %w", migration.Name, err)
	}

	return runMigration(ctx, m, parsedMigration, complete, c)
}
