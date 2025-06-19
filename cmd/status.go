// SPDX-License-Identifier: Apache-2.0

package cmd

import (
	"encoding/json"
	"fmt"

	"github.com/xataio/pgroll/cmd/flags"

	"github.com/spf13/cobra"
)

var statusCmd = &cobra.Command{
	Use:   "status",
	Short: "Show pgroll status",
	RunE: func(cmd *cobra.Command, _ []string) error {
		ctx := cmd.Context()

		m, err := NewRollWithInitCheck(ctx)
		if err != nil {
			return err
		}
		defer m.Close()

		status, err := m.Status(ctx, flags.Schema())
		if err != nil {
			return err
		}

		statusJSON, err := json.MarshalIndent(status, "", "  ")
		if err != nil {
			return err
		}

		fmt.Println(string(statusJSON))
		return nil
	},
}
