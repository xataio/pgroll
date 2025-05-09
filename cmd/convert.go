// SPDX-License-Identifier: Apache-2.0

package cmd

import (
	"bytes"
	"fmt"
	"io"
	"os"

	"github.com/spf13/cobra"
	"github.com/xataio/pgroll/pkg/migrations"
	"github.com/xataio/pgroll/pkg/sql2pgroll"
)

func convertCmd() *cobra.Command {
	var migrationName string
	var useJSON bool

	convertCmd := &cobra.Command{
		Use:       "convert <path to file with migrations>",
		Short:     "Convert SQL statements to a pgroll migration",
		Long:      "Convert SQL statements to a pgroll migration. The command can read SQL statements from stdin or a file",
		Args:      cobra.MaximumNArgs(1),
		ValidArgs: []string{"migration-file"},
		RunE: func(cmd *cobra.Command, args []string) error {
			reader, err := openSQLReader(args)
			if err != nil {
				return fmt.Errorf("open SQL migration: %w", err)
			}
			defer reader.Close()

			migration, err := sqlStatementsToMigration(reader, migrationName)
			if err != nil {
				return err
			}
			err = migrations.NewWriter(os.Stdout, migrations.NewMigrationFormat(useJSON)).Write(&migration)
			if err != nil {
				return fmt.Errorf("failed to write migration to stdout: %w", err)
			}
			return nil
		},
	}

	convertCmd.Flags().StringVarP(&migrationName, "name", "n", "", "Name of the migration")
	convertCmd.Flags().BoolVarP(&useJSON, "json", "j", false, "Output migration file in JSON format instead of YAML")

	return convertCmd
}

func openSQLReader(args []string) (io.ReadCloser, error) {
	if len(args) == 0 {
		return os.Stdin, nil
	}
	return os.Open(args[0])
}

func sqlStatementsToMigration(reader io.Reader, name string) (migrations.Migration, error) {
	var buf bytes.Buffer
	_, err := io.Copy(&buf, reader)
	if err != nil {
		return migrations.Migration{}, err
	}
	ops, err := sql2pgroll.Convert(buf.String())
	if err != nil {
		return migrations.Migration{}, err
	}
	return migrations.Migration{
		Name:       name,
		Operations: ops,
	}, nil
}
