package cmd

import (
	"fmt"
	"path/filepath"
	"strings"

	"pg-roll/pkg/migrations"

	"github.com/spf13/cobra"
)

var rollbackCmd = &cobra.Command{
	Use:   "rollback <file>",
	Short: "Roll back an ongoing migration",
	Long:  "Roll back an ongoing migration. This will revert the changes made by the migration.",
	Args:  cobra.ExactArgs(1),
	RunE: func(cmd *cobra.Command, args []string) error {
		fileName := args[0]

		m, err := migrations.New(cmd.Context(), PGURL)
		if err != nil {
			return err
		}
		defer m.Close()

		ops, err := migrations.ReadMigrationFile(args[0])
		if err != nil {
			return fmt.Errorf("reading migration file: %w", err)
		}

		version := strings.TrimSuffix(filepath.Base(fileName), filepath.Ext(fileName))

		err = m.Rollback(cmd.Context(), version, ops)
		if err != nil {
			return err
		}

		fmt.Printf("Migration rolled back. Changes made by %q have been reverted.\n", version)
		return nil
	},
}
