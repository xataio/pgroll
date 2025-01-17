// SPDX-License-Identifier: Apache-2.0

package cmd

import (
	"context"

	"github.com/spf13/cobra"
	"github.com/spf13/viper"

	"github.com/xataio/pgroll/cmd/flags"
	"github.com/xataio/pgroll/pkg/roll"
	"github.com/xataio/pgroll/pkg/state"
)

// Version is the pgroll version
var Version = "development"

func init() {
	viper.SetEnvPrefix("PGROLL")
	viper.AutomaticEnv()

	rootCmd.PersistentFlags().String("postgres-url", "postgres://postgres:postgres@localhost?sslmode=disable", "Postgres URL")
	rootCmd.PersistentFlags().String("schema", "public", "Postgres schema to use for the migration")
	rootCmd.PersistentFlags().String("pgroll-schema", "pgroll", "Postgres schema to use for pgroll internal state")
	rootCmd.PersistentFlags().Int("lock-timeout", 500, "Postgres lock timeout in milliseconds for pgroll DDL operations")
	rootCmd.PersistentFlags().Int("backfill-batch-size", roll.DefaultBackfillBatchSize, "Number of rows backfilled in each batch")
	rootCmd.PersistentFlags().Duration("backfill-batch-delay", roll.DefaultBackfillDelay, "Duration of delay between batch backfills (eg. 1s, 1000ms)")
	rootCmd.PersistentFlags().String("role", "", "Optional postgres role to set when executing migrations")

	viper.BindPFlag("PG_URL", rootCmd.PersistentFlags().Lookup("postgres-url"))
	viper.BindPFlag("SCHEMA", rootCmd.PersistentFlags().Lookup("schema"))
	viper.BindPFlag("STATE_SCHEMA", rootCmd.PersistentFlags().Lookup("pgroll-schema"))
	viper.BindPFlag("LOCK_TIMEOUT", rootCmd.PersistentFlags().Lookup("lock-timeout"))
	viper.BindPFlag("BACKFILL_BATCH_SIZE", rootCmd.PersistentFlags().Lookup("backfill-batch-size"))
	viper.BindPFlag("BACKFILL_BATCH_DELAY", rootCmd.PersistentFlags().Lookup("backfill-batch-delay"))
	viper.BindPFlag("ROLE", rootCmd.PersistentFlags().Lookup("role"))
}

var rootCmd = &cobra.Command{
	Use:          "pgroll",
	SilenceUsage: true,
	Version:      Version,
}

func NewRoll(ctx context.Context) (*roll.Roll, error) {
	pgURL := flags.PostgresURL()
	schema := flags.Schema()
	stateSchema := flags.StateSchema()
	lockTimeout := flags.LockTimeout()
	role := flags.Role()
	backfillBatchSize := flags.BackfillBatchSize()
	backfillBatchDelay := flags.BackfillBatchDelay()
	skipValidation := flags.SkipValidation()

	state, err := state.New(ctx, pgURL, stateSchema)
	if err != nil {
		return nil, err
	}

	return roll.New(ctx, pgURL, schema, state,
		roll.WithLockTimeoutMs(lockTimeout),
		roll.WithRole(role),
		roll.WithBackfillBatchSize(backfillBatchSize),
		roll.WithBackfillBatchDelay(backfillBatchDelay),
		roll.WithSkipValidation(skipValidation),
	)
}

// Execute executes the root command.
func Execute() error {
	// register subcommands
	rootCmd.AddCommand(startCmd())
	rootCmd.AddCommand(completeCmd)
	rootCmd.AddCommand(rollbackCmd)
	rootCmd.AddCommand(analyzeCmd)
	rootCmd.AddCommand(initCmd)
	rootCmd.AddCommand(statusCmd)
	rootCmd.AddCommand(migrateCmd())
	rootCmd.AddCommand(pullCmd())
	rootCmd.AddCommand(latestCmd())
	rootCmd.AddCommand(sqlCmd())
	rootCmd.AddCommand(sqlFolderCmd())

	return rootCmd.Execute()
}
