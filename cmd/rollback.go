// SPDX-License-Identifier: Apache-2.0

package cmd

import (
	"fmt"

	"github.com/pterm/pterm"
	"github.com/spf13/cobra"
)

var rollbackCmd = &cobra.Command{
	Use:   "rollback <file>",
	Short: "Roll back an ongoing migration",
	RunE: func(cmd *cobra.Command, args []string) error {
		m, err := NewRoll(cmd.Context())
		if err != nil {
			return err
		}
		defer m.Close()

		// Ensure that pgroll is initialized
		ok, err := m.State().IsInitialized(cmd.Context())
		if err != nil {
			return err
		}
		if !ok {
			return errPGRollNotInitialized
		}

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
