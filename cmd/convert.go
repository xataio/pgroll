// SPDX-License-Identifier: Apache-2.0

package cmd

import (
	"fmt"
	"io"
	"os"

	"github.com/spf13/cobra"
)

func convertCmd() *cobra.Command {
	convertCmd := &cobra.Command{
		Use:       "convert <path to file with migrations>",
		Short:     "Convert SQL statements to pgroll operations from SQL",
		Args:      cobra.MaximumNArgs(1),
		ValidArgs: []string{"migration-file"},
		Hidden:    true,
		RunE: func(cmd *cobra.Command, args []string) error {
			reader, err := openSQLReader(args)
			if err != nil {
				return fmt.Errorf("open SQL migration: %w", err)
			}
			defer reader.Close()

			_, err = scanSQLStatements(reader)
			return err
		},
	}

	return convertCmd
}

func openSQLReader(args []string) (io.ReadCloser, error) {
	if len(args) == 0 {
		return os.Stdin, nil
	}
	return os.Open(args[0])
}

func scanSQLStatements(reader io.Reader) ([]string, error) {
	panic("not implemented")
}
