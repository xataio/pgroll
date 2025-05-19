// SPDX-License-Identifier: Apache-2.0

package cmd

import (
	"context"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"

	"github.com/spf13/cobra"
	"github.com/xataio/pgroll/pkg/migrations"
	"github.com/xataio/pgroll/pkg/roll"
)

func updateCmd() *cobra.Command {
	var useJSON bool
	var local bool

	updateCmd := &cobra.Command{
		Use:       "update <directory>",
		Short:     "update outdated migrations in a directory",
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

			migs, err := getMigrationsToUpdate(ctx, m, os.DirFS(migrationsDir), local)
			if err != nil {
				return err
			}

			if len(migs) == 0 {
				fmt.Println("database is up to date; no migrations to apply")
				return nil
			}

			for _, mig := range migs {
				if _, err := migrations.ParseMigration(mig); err == nil {
					continue
				}

				updater := migrations.NewFileUpdater()
				updatedMigration, err := updater.Update(mig)
				if err != nil {
					return err
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
	updateCmd.Flags().BoolVarP(&local, "local", "l", false, "Update all local migration files")

	return updateCmd
}

func getMigrationsToUpdate(ctx context.Context, m *roll.Roll, migrationsDir fs.FS, local bool) ([]*migrations.RawMigration, error) {
	if !local {
		migs, err := m.UnappliedMigrations(ctx, migrationsDir)
		if err != nil {
			return nil, fmt.Errorf("failed to get migrations to apply: %w", err)
		}
		return migs, nil
	}
	files, err := migrations.CollectFilesFromDir(migrationsDir)
	if err != nil {
		return nil, fmt.Errorf("reading migration files: %w", err)
	}
	migs := make([]*migrations.RawMigration, 0, len(files))
	for _, f := range files {
		m, err := migrations.ReadRawMigration(migrationsDir, f)
		if err != nil {
			return nil, err
		}
		migs = append(migs, m)
	}
	return migs, nil
}
