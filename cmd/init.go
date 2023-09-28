// SPDX-License-Identifier: Apache-2.0

package cmd

import (
	"fmt"

	"github.com/pterm/pterm"
	"github.com/spf13/cobra"
)

var initCmd = &cobra.Command{
	Use:   "init <file>",
	Short: "Initializes pgroll, creating the required pg_roll schema to store state",
	RunE: func(cmd *cobra.Command, args []string) error {
		m, err := NewRoll(cmd.Context())
		if err != nil {
			return err
		}
		defer m.Close()

		sp, _ := pterm.DefaultSpinner.WithText("Initializing pgroll...").Start()
		err = m.Init(cmd.Context())
		if err != nil {
			sp.Fail(fmt.Sprintf("Failed to initialize pgroll: %s", err))
			return err
		}

		sp.Success("Initialization complete")
		return nil
	},
}
