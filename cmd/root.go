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
	rootCmd.PersistentFlags().String("pgroll-schema", "pgroll", "Postgres schema in which the migration should be applied")

	viper.BindPFlag("PG_URL", rootCmd.PersistentFlags().Lookup("postgres-url"))
	viper.BindPFlag("SCHEMA", rootCmd.PersistentFlags().Lookup("schema"))
	viper.BindPFlag("STATE_SCHEMA", rootCmd.PersistentFlags().Lookup("pgroll-schema"))
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

	state, err := state.New(ctx, pgURL, stateSchema)
	if err != nil {
		return nil, err
	}

	return roll.New(ctx, pgURL, schema, state)
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
