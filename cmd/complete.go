// SPDX-License-Identifier: Apache-2.0

package cmd

import (
	"fmt"

	"github.com/pterm/pterm"
	"github.com/spf13/cobra"
	"github.com/xataio/pgroll/cmd/flags"
)

func completeCmd() *cobra.Command {
	completeCmd := &cobra.Command{
		Use:   "complete <file>",
		Short: "Complete an ongoing migration with the operations present in the given file",
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

			sp, _ := pterm.DefaultSpinner.WithText("Completing migration...").Start()
			err = m.Complete(cmd.Context())
			if err != nil {
				sp.Fail(fmt.Sprintf("Failed to complete migration: %s", err))
				return err
			}

			sp.Success("Migration successful!")
			return nil
		},
	}

	flags.PgConnectionFlags(completeCmd)

	return completeCmd
}
