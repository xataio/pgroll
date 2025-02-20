// SPDX-License-Identifier: Apache-2.0

package cmd

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"time"

	"github.com/spf13/cobra"
	"github.com/xataio/pgroll/pkg/migrations"
	"github.com/xataio/pgroll/pkg/sql2pgroll"
)

func convertCmd() *cobra.Command {
	var migrationName string

	convertCmd := &cobra.Command{
		Use:       "convert <path to file with migrations>",
		Short:     "Convert SQL statements to a pgroll migration from SQL",
		Long:      "Convert SQL statements to pgroll migrations from SQL. The command can read SQL statements from stdin or a file",
		Args:      cobra.MaximumNArgs(1),
		ValidArgs: []string{"migration-file"},
		Hidden:    true,
		RunE: func(cmd *cobra.Command, args []string) error {
			reader, err := openSQLReader(args)
			if err != nil {
				return fmt.Errorf("open SQL migration: %w", err)
			}
			defer reader.Close()

			if migrationName == "" {
				migrationName = time.Now().Format("20060102150405")
			}
			migration, err := sqlStatementsToMigration(reader, migrationName)
			if err != nil {
				return err
			}
			enc := json.NewEncoder(os.Stdout)
			enc.SetIndent("", "  ")
			if err := enc.Encode(migration); err != nil {
				return fmt.Errorf("encode migration: %w", err)
			}
			return nil
		},
	}

	convertCmd.Flags().StringVar(&migrationName, "migration-name", "{current_timestamp}", "Name of the migration")

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
