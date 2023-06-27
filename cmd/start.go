package cmd

import (
	"fmt"
	"path/filepath"
	"pg-roll/pkg/migrations"
	"strings"

	"github.com/spf13/cobra"
)

var startCmd = &cobra.Command{
	Use:   "start <file>",
	Short: "Start a migration for the operations present in the given file",
	Args:  cobra.ExactArgs(1),
	RunE: func(cmd *cobra.Command, args []string) error {
		fileName := args[0]

		m, err := NewMigrations(cmd.Context())
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

		fmt.Printf("Migration successful!, new version of the schema available under postgres '%s' schema\n", version)
		return nil
	},
}
