// SPDX-License-Identifier: Apache-2.0

package cmd

import (
	"fmt"

	"github.com/pterm/pterm"
	"github.com/spf13/cobra"
)

var completeCmd = &cobra.Command{
	Use:   "complete <file>",
	Short: "Complete an ongoing migration with the operations present in the given file",
	RunE: func(cmd *cobra.Command, args []string) error {
		m, err := NewRoll(cmd.Context())
		if err != nil {
			return err
		}
		defer m.Close()

		sp, _ := pterm.DefaultSpinner.WithText("Rolling back migration...").Start()
		err = m.Complete(cmd.Context())
		if err != nil {
			sp.Fail(fmt.Sprintf("Failed to complete migration: %s", err))
			return err
		}

		sp.Success("Migration successful!")
		return nil
	},
}
