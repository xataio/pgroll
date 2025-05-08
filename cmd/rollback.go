// SPDX-License-Identifier: Apache-2.0

package cmd

import (
	"fmt"

	"github.com/pterm/pterm"
	"github.com/spf13/cobra"
)

var rollbackCmd = &cobra.Command{
	Use:   "rollback",
	Short: "Roll back an ongoing migration",
	RunE: func(cmd *cobra.Command, args []string) error {
		// Create a roll instance and check if pgroll is initialized
		m, err := NewRollWithInitCheck(cmd.Context())
		if err != nil {
			return err
		}
		defer m.Close()

		sp, _ := pterm.DefaultSpinner.WithText("Rolling back migration...").Start()
		err = m.Rollback(cmd.Context())
		if err != nil {
			sp.Fail(fmt.Sprintf("Failed to roll back migration: %s", err))
			return err
		}

		sp.Success("Migration rolled back. Changes made since the last version have been reverted")
		return nil
	},
}
