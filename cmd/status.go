// SPDX-License-Identifier: Apache-2.0

package cmd

import (
	"encoding/json"
	"fmt"

	"github.com/xataio/pgroll/cmd/flags"
	"github.com/xataio/pgroll/pkg/state"

	"github.com/spf13/cobra"
)

var statusCmd = &cobra.Command{
	Use:   "status",
	Short: "Show pgroll status",
	RunE: func(cmd *cobra.Command, _ []string) error {
		ctx := cmd.Context()

		state, err := state.New(ctx, flags.PostgresURL(), flags.StateSchema())
		if err != nil {
			return err
		}
		defer state.Close()

		// Ensure that pgroll is initialized
		if err := EnsureInitialized(ctx, state); err != nil {
			return err
		}

		status, err := state.Status(ctx, flags.Schema())
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
