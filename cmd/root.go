package cmd

import (
	"github.com/spf13/cobra"
)

var (
	PGURL string
)

func init() {
	rootCmd.PersistentFlags().StringVar(&PGURL, "postgres_url", "postgres://postgres:postgres@localhost?sslmode=disable", "Postgres URL")
}

var (
	rootCmd = &cobra.Command{
		Use:          "pg-roll",
		Short:        "TODO: Add short description",
		Long:         `TODO: Add long description`,
		SilenceUsage: true,
	}
)

// Execute executes the root command.
func Execute() error {
	// register subcommands
	rootCmd.AddCommand(startCmd)
	rootCmd.AddCommand(completeCmd)

	return rootCmd.Execute()
}
