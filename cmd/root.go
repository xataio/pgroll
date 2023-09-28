// SPDX-License-Identifier: Apache-2.0

package cmd

import (
	"context"

	"github.com/spf13/cobra"
	"github.com/xataio/pgroll/pkg/roll"
	"github.com/xataio/pgroll/pkg/state"
)

var (
	// PGURL is the Postgres URL to connect to
	PGURL string

	// Schema is the schema to use for the migration
	Schema string

	// StateSchema is the Postgres schema where pgroll will store its state
	StateSchema string

	// Version is the pgroll version
	Version = "development"
)

func init() {
	rootCmd.PersistentFlags().StringVar(&PGURL, "postgres-url", "postgres://postgres:postgres@localhost?sslmode=disable", "Postgres URL")
	rootCmd.PersistentFlags().StringVar(&Schema, "schema", "public", "Postgres schema to use for the migration")
	rootCmd.PersistentFlags().StringVar(&StateSchema, "pgroll-schema", "pgroll", "Postgres schema in which the migration should be applied")
}

var rootCmd = &cobra.Command{
	Use:          "pgroll",
	SilenceUsage: true,
	Version:      Version,
}

func NewRoll(ctx context.Context) (*roll.Roll, error) {
	state, err := state.New(ctx, PGURL, StateSchema)
	if err != nil {
		return nil, err
	}

	return roll.New(ctx, PGURL, Schema, state)
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
