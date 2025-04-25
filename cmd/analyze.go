// SPDX-License-Identifier: Apache-2.0

package cmd

import (
	"encoding/json"
	"fmt"

	"github.com/spf13/cobra"

	"github.com/xataio/pgroll/cmd/flags"
	"github.com/xataio/pgroll/pkg/state"
)

func analyzeCmd() *cobra.Command {
	analyzeCmd := &cobra.Command{
		Use:    "analyze",
		Short:  "Analyze the SQL schema of the target database",
		Hidden: true,
		Args:   cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			ctx := cmd.Context()
			state, err := state.New(ctx, flags.PostgresURL(), flags.StateSchema())
			if err != nil {
				return err
			}
			defer state.Close()

			// Ensure that pgroll is initialized
			ok, err := state.IsInitialized(cmd.Context())
			if err != nil {
				return err
			}
			if !ok {
				return errPGRollNotInitialized
			}

			schema, err := state.ReadSchema(ctx, flags.Schema())
			if err != nil {
				return err
			}

			schemaJSON, err := json.MarshalIndent(schema, "", "  ")
			if err != nil {
				return err
			}

			fmt.Println(string(schemaJSON))
			return nil
		},
	}

	flags.PgConnectionFlags(analyzeCmd)

	return analyzeCmd
}
