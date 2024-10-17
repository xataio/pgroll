// SPDX-License-Identifier: Apache-2.0

package cmd

import (
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strings"

	"github.com/pterm/pterm"
	"github.com/spf13/cobra"

	"github.com/xataio/pgroll/cmd/flags"
	"github.com/xataio/pgroll/pkg/migrations"
	"github.com/xataio/pgroll/pkg/roll"
)

var bootstrapCmd = &cobra.Command{
	Use:   "bootstrap <folder>",
	Short: "Bootstrap a new database from a directory of migration files",
	Long: `Bootstrap a new database from a directory of migration files. All files in the directory will be executed
in alphabetical order. All migrations are completed.`,
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
			file, err := os.Open(fileName)
			if err != nil {
				return fmt.Errorf("opening migration file: %w", err)
			}
			migration, err := migrations.ReadMigration(file)
			if err != nil {
				file.Close()
				return fmt.Errorf("reading migration file: %w", err)
			}

			sp, _ := pterm.DefaultSpinner.WithText("Starting migration...").Start()
			cb := func(n int64) {
				sp.UpdateText(fmt.Sprintf("%d records complete...", n))
			}

			err = m.Start(cmd.Context(), migration, cb)
			if err != nil {
				sp.Fail(fmt.Sprintf("Failed to start migration: %s", err))
				file.Close()
				return err
			}
			file.Close()

			if err = m.Complete(cmd.Context()); err != nil {
				sp.Fail(fmt.Sprintf("Failed to complete migration: %s", err))
				return err
			}

			version := migration.Name
			if version == "" {
				version = strings.TrimSuffix(filepath.Base(fileName), filepath.Ext(fileName))
			}
			viewName := roll.VersionedSchemaName(flags.Schema(), version)
			msg := fmt.Sprintf("New version of the schema available under the postgres %q schema", viewName)
			sp.Success(msg)
		}

		return nil
	},
}
