package cmd

import (
	"fmt"
	"path/filepath"
	"strings"

	"pg-roll/pkg/migrations"

	"github.com/spf13/cobra"
)

var completeCmd = &cobra.Command{
	Use:   "complete <file>",
	Short: "Complete an ongoing migration with the operations present in the given file",
	Long:  `TODO: Add long description`,
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

		err = m.Complete(cmd.Context(), version, ops)
		if err != nil {
			return err
		}

		fmt.Println("Migration successful!")
		return nil
	},
}
