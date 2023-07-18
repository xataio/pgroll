package cmd

import (
	"fmt"
	"path/filepath"
	"strings"

	"pg-roll/pkg/migrations"

	"github.com/spf13/cobra"
)

func startCmd() *cobra.Command {
	var complete bool

	startCmd := &cobra.Command{
		Use:   "start <file>",
		Short: "Start a migration for the operations present in the given file",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			fileName := args[0]

			m, err := NewRoll(cmd.Context())
			if err != nil {
				return err
			}
			defer m.Close()

			migration, err := migrations.ReadMigrationFile(args[0])
			if err != nil {
				return fmt.Errorf("reading migration file: %w", err)
			}

			version := strings.TrimSuffix(filepath.Base(fileName), filepath.Ext(fileName))

			err = m.Start(cmd.Context(), migration)
			if err != nil {
				return err
			}

			if complete {
				if err = m.Complete(cmd.Context()); err != nil {
					return err
				}
			}

			fmt.Printf("Migration successful!, new version of the schema available under postgres '%s' schema\n", version)
			return nil
		},
	}

	startCmd.Flags().BoolVarP(&complete, "complete", "c", false, "Mark the migration as complete")

	return startCmd
}
