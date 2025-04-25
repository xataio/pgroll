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

func NewRoll(ctx context.Context) (*roll.Roll, error) {
	pgURL := flags.PostgresURL()
	schema := flags.Schema()
	stateSchema := flags.StateSchema()
	lockTimeout := flags.LockTimeout()
	role := flags.Role()
	skipValidation := flags.SkipValidation()

	state, err := state.New(ctx, pgURL, stateSchema)
	if err != nil {
		return nil, err
	}

	return roll.New(ctx, pgURL, schema, state,
		roll.WithLockTimeoutMs(lockTimeout),
		roll.WithRole(role),
		roll.WithSkipValidation(skipValidation),
	)
}

func Prepare() *cobra.Command {
	rootCmd := &cobra.Command{
		Use:          "pgroll",
		SilenceUsage: true,
		Version:      Version,
		Long:         "For more information, visit http://pgroll.com/docs",
	}

	viper.SetEnvPrefix("PGROLL")
	viper.AutomaticEnv()

	// register subcommands
	rootCmd.AddCommand(startCmd())
	rootCmd.AddCommand(completeCmd())
	rootCmd.AddCommand(rollbackCmd())
	rootCmd.AddCommand(analyzeCmd())
	rootCmd.AddCommand(initCmd())
	rootCmd.AddCommand(statusCmd())
	rootCmd.AddCommand(migrateCmd())
	rootCmd.AddCommand(pullCmd())
	rootCmd.AddCommand(latestCmd())
	rootCmd.AddCommand(convertCmd())

	return rootCmd
}

// Execute executes the root command.
func Execute() error {
	cmd := Prepare()
	return cmd.Execute()
}
