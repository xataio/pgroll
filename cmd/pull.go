// SPDX-License-Identifier: Apache-2.0

package cmd

import (
	"fmt"

	"github.com/xataio/pgroll/cmd/flags"
	"github.com/xataio/pgroll/pkg/state"

	"github.com/spf13/cobra"
)

func pullCmd() *cobra.Command {
	opts := map[string]string{
		"p": "prefix each migration filename with its position in the schema history",
	}
	var withPrefixes bool

	pullCmd := &cobra.Command{
		Use:       "pull <target directory>",
		Short:     "Pull migration history from the target database and write it to disk",
		Args:      cobra.ExactArgs(1),
		ValidArgs: []string{"directory"},
		RunE: func(cmd *cobra.Command, args []string) error {
			ctx := cmd.Context()
			targetDir := args[0]

			state, err := state.New(ctx, flags.PostgresURL(), flags.StateSchema())
			if err != nil {
				return err
			}
			defer state.Close()

			migs, err := state.SchemaHistory(ctx, flags.Schema())
			if err != nil {
				return fmt.Errorf("failed to read schema history: %w", err)
			}

			for i, mig := range migs {
				prefix := ""
				if withPrefixes {
					prefix = fmt.Sprintf("%04d", i+1) + "_"
				}
				err := mig.WriteToFile(targetDir, prefix)
				if err != nil {
					return fmt.Errorf("failed to write migration %q: %w", mig.Migration.Name, err)
				}
			}
			return nil
		},
	}

	pullCmd.Flags().BoolVarP(&withPrefixes, "with-prefixes", "p", false, opts["p"])

	return pullCmd
}
