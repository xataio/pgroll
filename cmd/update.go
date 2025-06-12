// SPDX-License-Identifier: Apache-2.0

package cmd

import (
	"fmt"
	"os"
	"path/filepath"

	"github.com/spf13/cobra"
	"github.com/xataio/pgroll/pkg/migrations"
)

func updateCmd() *cobra.Command {
	var useJSON bool

	updateCmd := &cobra.Command{
		Use:       "update <directory>",
		Short:     "Update outdated migrations in a directory",
		Example:   "update ./migrations",
		Args:      cobra.ExactArgs(1),
		ValidArgs: []string{"directory"},
		RunE: func(cmd *cobra.Command, args []string) error {
			ctx := cmd.Context()
			migrationsDir := args[0]

			// Create a roll instance and check if pgroll is initialized
			m, err := NewRollWithInitCheck(ctx)
			if err != nil {
				return err
			}
			defer m.Close()

			info, err := os.Stat(migrationsDir)
			if err != nil {
				return fmt.Errorf("failed to stat directory: %w", err)
			}
			if !info.IsDir() {
				return fmt.Errorf("migrations directory %q is not a directory", migrationsDir)
			}

			files, err := migrations.CollectFilesFromDir(os.DirFS(migrationsDir))
			if err != nil {
				return fmt.Errorf("failed to reading migration files from directory: %w", err)
			}

			for _, f := range files {
				mig, err := migrations.ReadRawMigration(os.DirFS(migrationsDir), f)
				if err != nil {
					return fmt.Errorf("failed to read migration file: %w", err)
				}
				if _, err := migrations.ParseMigration(mig); err == nil {
					continue
				}

				updater := newFileUpdater()
				updatedMigration, err := updater.Update(mig)
				if err != nil {
					return fmt.Errorf("failed to update migration file: %w", err)
				}

				format := migrations.NewMigrationFormat(useJSON)
				migrationFileName := fmt.Sprintf("%s.%s", mig.Name, format.Extension())
				file, err := os.Create(filepath.Join(migrationsDir, migrationFileName))
				if err != nil {
					return fmt.Errorf("failed to update migration file: %w", err)
				}
				err = migrations.NewWriter(file, format).Write(updatedMigration)
				if err != nil {
					file.Close()
					return fmt.Errorf("failed to write migration file: %w", err)
				}
				file.Close()
			}

			return nil
		},
	}

	updateCmd.Flags().BoolVarP(&useJSON, "json", "j", false, "Output migration file in JSON format instead of YAML")

	return updateCmd
}

func newFileUpdater() *migrations.FileUpdater {
	return migrations.NewFileUpdater(map[string][]migrations.UpdaterFn{
		string(migrations.OpNameCreateIndex): {
			migrations.UpdateCreateIndexColumnsList,
		},
	})
}
