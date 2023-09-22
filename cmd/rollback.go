// SPDX-License-Identifier: Apache-2.0

package cmd

import (
	"fmt"

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

		err = m.Rollback(cmd.Context())
		if err != nil {
			return err
		}

		fmt.Printf("Migration rolled back. Changes made since the last version have been reverted.\n")
		return nil
	},
}
