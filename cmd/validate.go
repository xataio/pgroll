// SPDX-License-Identifier: Apache-2.0

package cmd

import (
	"os"
	"path/filepath"

	"github.com/spf13/cobra"
	"github.com/xataio/pgroll/pkg/migrations"
)

var validateCmd = &cobra.Command{
	Use:       "validate <file>",
	Short:     "Validate a migration file",
	Example:   "validate migrations/03_my_migration.yaml",
	Args:      cobra.ExactArgs(1),
	ValidArgs: []string{"file"},
	RunE: func(cmd *cobra.Command, args []string) error {
		ctx := cmd.Context()
		fileName := args[0]

		m, err := NewRollWithInitCheck(ctx)
		if err != nil {
			return err
		}
		defer m.Close()

		migration, err := migrations.ReadMigration(os.DirFS(filepath.Dir(fileName)), filepath.Base(fileName))
		if err != nil {
			return err
		}
		err = m.Validate(ctx, migration)
		if err != nil {
			return err
		}
		return nil
	},
}
