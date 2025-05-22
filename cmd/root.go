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
	verbose := flags.Verbose()

	state, err := state.New(ctx, pgURL, stateSchema)
	if err != nil {
		return nil, err
	}

	return roll.New(ctx, pgURL, schema, state,
		roll.WithLockTimeoutMs(lockTimeout),
		roll.WithRole(role),
		roll.WithSkipValidation(skipValidation),
		roll.WithLogging(verbose),
	)
}

// EnsureInitialized checks if the pgroll state schema is initialized.
// Returns an error if the check fails or if pgroll is not initialized.
func EnsureInitialized(ctx context.Context, state *state.State) error {
	ok, err := state.IsInitialized(ctx)
	if err != nil {
		return err
	}
	if !ok {
		return errPGRollNotInitialized
	}
	return nil
}

// NewRollWithInitCheck creates a roll instance and checks if pgroll is initialized.
// Returns the roll instance and an error if creation fails or if pgroll is not initialized.
func NewRollWithInitCheck(ctx context.Context) (*roll.Roll, error) {
	// Create a roll instance
	m, err := NewRoll(ctx)
	if err != nil {
		return nil, err
	}

	// Ensure that pgroll is initialized
	if err := EnsureInitialized(ctx, m.State()); err != nil {
		m.Close()
		return nil, err
	}

	return m, nil
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

	rootCmd.PersistentFlags().String("postgres-url", "postgres://postgres:postgres@localhost?sslmode=disable", "Postgres URL")
	rootCmd.PersistentFlags().String("schema", "public", "Postgres schema to use for the migration")
	rootCmd.PersistentFlags().String("pgroll-schema", "pgroll", "Postgres schema to use for pgroll internal state")
	rootCmd.PersistentFlags().Int("lock-timeout", 500, "Postgres lock timeout in milliseconds for pgroll DDL operations")
	rootCmd.PersistentFlags().String("role", "", "Optional postgres role to set when executing migrations")
	rootCmd.PersistentFlags().Bool("verbose", false, "Enable verbose logging")

	viper.BindPFlag("PG_URL", rootCmd.PersistentFlags().Lookup("postgres-url"))
	viper.BindPFlag("SCHEMA", rootCmd.PersistentFlags().Lookup("schema"))
	viper.BindPFlag("STATE_SCHEMA", rootCmd.PersistentFlags().Lookup("pgroll-schema"))
	viper.BindPFlag("LOCK_TIMEOUT", rootCmd.PersistentFlags().Lookup("lock-timeout"))
	viper.BindPFlag("ROLE", rootCmd.PersistentFlags().Lookup("role"))
	viper.BindPFlag("VERBOSE", rootCmd.PersistentFlags().Lookup("verbose"))

	// register subcommands
	rootCmd.AddCommand(startCmd())
	rootCmd.AddCommand(completeCmd)
	rootCmd.AddCommand(rollbackCmd)
	rootCmd.AddCommand(analyzeCmd)
	rootCmd.AddCommand(initCmd)
	rootCmd.AddCommand(statusCmd)
	rootCmd.AddCommand(updateCmd())
	rootCmd.AddCommand(createCmd())
	rootCmd.AddCommand(migrateCmd())
	rootCmd.AddCommand(pullCmd())
	rootCmd.AddCommand(latestCmd())
	rootCmd.AddCommand(convertCmd())
	rootCmd.AddCommand(baselineCmd())

	return rootCmd
}

// Execute executes the root command.
func Execute() error {
	cmd := Prepare()
	return cmd.Execute()
}
