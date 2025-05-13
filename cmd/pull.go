// SPDX-License-Identifier: Apache-2.0

package cmd

import (
	"fmt"
	"os"
	"path/filepath"

	"github.com/spf13/cobra"
	"github.com/xataio/pgroll/pkg/migrations"
)

func pullCmd() *cobra.Command {
	opts := map[string]string{
		"p": "prefix each migration filename with its position in the schema history",
		"j": "output each migration in JSON format instead of YAML",
	}
	var withPrefixes, useJSON bool

	pullCmd := &cobra.Command{
		Use:       "pull <target directory>",
		Short:     "Pull migration history from the target database and write it to disk",
		Args:      cobra.ExactArgs(1),
		ValidArgs: []string{"directory"},
		RunE: func(cmd *cobra.Command, args []string) error {
			ctx := cmd.Context()
			targetDir := args[0]

			// Create a roll instance and check if pgroll is initialized
			m, err := NewRollWithInitCheck(ctx)
			if err != nil {
				return err
			}
			defer m.Close()

			// Ensure that the target directory exists
			if err := ensureDirectoryExists(targetDir); err != nil {
				return err
			}

			// Get the list of missing migrations (those that have been applied to
			// the target database but are missing in the local directory).
			migs, err := m.MissingMigrations(ctx, os.DirFS(targetDir))
			if err != nil {
				return fmt.Errorf("failed to get missing migrations: %w", err)
			}

			// Write the missing migrations to the target directory
			for i, mig := range migs {
				prefix := ""
				if withPrefixes {
					prefix = fmt.Sprintf("%04d", i+1) + "_"
				}
				filePath, err := writeMigrationToFile(mig, targetDir, prefix, useJSON)
				if err != nil {
					return fmt.Errorf("failed to write migration %q: %w", filePath, err)
				}
			}
			return nil
		},
	}

	pullCmd.Flags().BoolVarP(&withPrefixes, "with-prefixes", "p", false, opts["p"])
	pullCmd.Flags().BoolVarP(&useJSON, "json", "j", false, opts["j"])

	return pullCmd
}

// ensureDirectoryExists ensures that the target directory exists, creating it if it doesn't.
// Returns an error if the directory cannot be created or if there's an issue checking its existence.
func ensureDirectoryExists(targetDir string) error {
	_, err := os.Stat(targetDir)
	if err != nil {
		if os.IsNotExist(err) {
			err := os.MkdirAll(targetDir, 0o755)
			if err != nil {
				return fmt.Errorf("failed to create target directory: %w", err)
			}
		} else {
			return fmt.Errorf("failed to stat directory: %w", err)
		}
	}
	return nil
}

// WriteToFile writes the migration to a file in `targetDir`, prefixing the
// filename with `prefix`. The output format defaults to YAML, but can
// be changed to JSON by setting `useJSON` to true. The function returns
// the full path of the created file or an error if the operation fails.
func writeMigrationToFile(m *migrations.RawMigration, targetDir, prefix string, useJSON bool) (string, error) {
	if err := ensureDirectoryExists(targetDir); err != nil {
		return "", err
	}

	format := migrations.NewMigrationFormat(useJSON)
	fileName := fmt.Sprintf("%s%s.%s", prefix, m.Name, format.Extension())
	filePath := filepath.Join(targetDir, fileName)

	file, err := os.Create(filePath)
	if err != nil {
		return "", err
	}
	defer file.Close()

	err = migrations.NewWriter(file, format).WriteRaw(m)
	if err != nil {
		return "", err
	}
	return filePath, nil
}
