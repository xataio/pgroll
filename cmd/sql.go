// SPDX-License-Identifier: Apache-2.0

package cmd

import (
	"encoding/json"
	"fmt"
	"os"

	"github.com/spf13/cobra"
	"github.com/xataio/pgroll/pkg/sql2pgroll"
)

func convertCmd() *cobra.Command {
	convertCmd := &cobra.Command{
		Use:       "convert <sql statement>",
		Short:     "Convert SQL statements to pgroll operations",
		Args:      cobra.ExactArgs(1),
		ValidArgs: []string{"statement"},
		Hidden:    true,
		RunE: func(cmd *cobra.Command, args []string) error {
			sql := args[0]

			ops, err := sql2pgroll.Convert(sql)
			if err != nil {
				return fmt.Errorf("failed to convert SQL statement: %w", err)
			}

			enc := json.NewEncoder(os.Stdout)
			enc.SetIndent("", "  ")
			if err := enc.Encode(ops); err != nil {
				return fmt.Errorf("failed to encode operations: %w", err)
			}

			return nil
		},
	}

	return convertCmd
}
