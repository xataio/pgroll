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
	rootCmd.PersistentFlags().String("role", "", "Optional postgres role to set when executing migrations")

	viper.BindPFlag("PG_URL", rootCmd.PersistentFlags().Lookup("postgres-url"))
	viper.BindPFlag("SCHEMA", rootCmd.PersistentFlags().Lookup("schema"))
	viper.BindPFlag("STATE_SCHEMA", rootCmd.PersistentFlags().Lookup("pgroll-schema"))
	viper.BindPFlag("LOCK_TIMEOUT", rootCmd.PersistentFlags().Lookup("lock-timeout"))
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

	state, err := state.New(ctx, pgURL, stateSchema)
	if err != nil {
		return nil, err
	}

	return roll.New(ctx, pgURL, schema, state,
		roll.WithLockTimeoutMs(lockTimeout),
		roll.WithRole(role),
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

	return rootCmd.Execute()
}
