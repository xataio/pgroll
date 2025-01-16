// SPDX-License-Identifier: Apache-2.0

package cmd

import (
	"fmt"

	"github.com/spf13/cobra"
)

func convertCmd() *cobra.Command {
	convertCmd := &cobra.Command{
		Use:    "convert <path to file with migrations>",
		Short:  "Convert SQL statements to pgroll operations from SQL",
		Args:   cobra.ExactArgs(1),
		Hidden: true,
		RunE: func(cmd *cobra.Command, args []string) error {
			file := args[0]

			_, err := readSQLFromFile(file)
			return err
		},
	}

	return convertCmd
}

func readSQLFromFile(file string) ([]string, error) {
	fmt.Println("Reading file: ", file)

	panic("not implemented")
}
