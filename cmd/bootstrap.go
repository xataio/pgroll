// SPDX-License-Identifier: Apache-2.0

package cmd

import (
	"fmt"
	"os"
	"path/filepath"
	"slices"

	"github.com/spf13/cobra"
)

var bootstrapCmd = &cobra.Command{
	Use:   "bootstrap <folder>",
	Short: "Bootstrap a new database from a directory of migration files",
	Long: `Bootstrap a new database from a directory of migration files. All files in the directory will be executed
in lexicographical order. All migrations are completed.`,
	Args: cobra.ExactArgs(1),
	RunE: func(cmd *cobra.Command, args []string) error {
		migrationsDir := args[0]

		m, err := NewRoll(cmd.Context())
		if err != nil {
			return err
		}
		defer m.Close()

		// open folder and read all json files
		files, err := os.ReadDir(migrationsDir)
		if err != nil {
			return fmt.Errorf("reading migration directory: %w", err)
		}
		migrationFiles := []string{}
		for _, file := range files {
			if file.IsDir() || filepath.Ext(file.Name()) != ".json" {
				continue
			}
			migrationFiles = append(migrationFiles, filepath.Join(migrationsDir, file.Name()))
		}
		slices.Sort(migrationFiles)

		for _, fileName := range migrationFiles {
			if err := runMigrationFromFile(cmd.Context(), m, fileName, true); err != nil {
				return fmt.Errorf("running migration file '%s': %w", fileName, err)
			}
		}

		return nil
	},
}
