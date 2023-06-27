package cmd

import (
	"context"
	"pg-roll/pkg/migrations"
	"pg-roll/pkg/state"

	"github.com/spf13/cobra"
)

var (
	PGURL       string
	StateSchema string
)

func init() {
	rootCmd.PersistentFlags().StringVar(&PGURL, "postgres_url", "postgres://postgres:postgres@localhost?sslmode=disable", "Postgres URL")
	rootCmd.PersistentFlags().StringVar(&StateSchema, "pgroll_schema", "pgroll", "Postgres schema where pg-roll will store its state")
}

var (
	rootCmd = &cobra.Command{
		Use:          "pg-roll",
		SilenceUsage: true,
	}
)

func NewMigrations(ctx context.Context) (*migrations.Migrations, error) {
	state, err := state.New(ctx, PGURL, StateSchema)
	if err != nil {
		return nil, err
	}

	return migrations.New(ctx, PGURL, state)
}

// Execute executes the root command.
func Execute() error {
	// register subcommands
	rootCmd.AddCommand(startCmd)
	rootCmd.AddCommand(completeCmd)
	rootCmd.AddCommand(analyzeCmd)
	rootCmd.AddCommand(initCmd)

	return rootCmd.Execute()
}
