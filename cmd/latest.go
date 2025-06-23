// SPDX-License-Identifier: Apache-2.0

package cmd

import (
	"github.com/spf13/cobra"
)

func latestCmd() *cobra.Command {
	latestCmd := &cobra.Command{
		Use:   "latest",
		Short: "Print the name of the latest schema version or migration",
		Long:  "Print the name of the latest schema version or migration, either in the target database or a local directory",
	}

	latestCmd.AddCommand(latestSchemaCmd())
	latestCmd.AddCommand(latestMigrationCmd())
	latestCmd.AddCommand(latestURLCmd())

	return latestCmd
}
