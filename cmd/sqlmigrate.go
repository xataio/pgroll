// SPDX-License-Identifier: Apache-2.0

package cmd

import (
	"bytes"
	"fmt"
	"io"
	"time"

	"github.com/spf13/cobra"
	"github.com/xataio/pgroll/pkg/migrations"
)

func sqlMigrateCmd() *cobra.Command {
	var migrationName string

	convertCmd := &cobra.Command{
		Use:       "sqlmigrate <path to file with migrations>",
		Short:     "Run SQL migration scripts",
		Long:      "Run SQL migration scripts without turning them into pgroll migrations. The command can read SQL statements from stdin or a file",
		Args:      cobra.MaximumNArgs(1),
		ValidArgs: []string{"migration-file"},
		RunE: func(cmd *cobra.Command, args []string) error {
			reader, err := openSQLReader(args)
			if err != nil {
				return fmt.Errorf("open SQL migration: %w", err)
			}
			defer reader.Close()

			if migrationName == "{current_timestamp}" {
				migrationName = time.Now().Format("20060102150405")
			}
			migration, err := sqlStatementsToSQLMigration(reader, migrationName)
			if err != nil {
				return err
			}

			m, err := NewRoll(cmd.Context())
			if err != nil {
				return err
			}
			defer m.Close()

			return runMigration(cmd.Context(), m, migration, true, nil)
		},
	}

	convertCmd.Flags().StringVarP(&migrationName, "name", "n", "{current_timestamp}", "Name of the migration")

	return convertCmd
}

func sqlStatementsToSQLMigration(reader io.Reader, name string) (*migrations.Migration, error) {
	var buf bytes.Buffer
	_, err := io.Copy(&buf, reader)
	if err != nil {
		return nil, err
	}

	return &migrations.Migration{
		Name: name,
		Operations: migrations.Operations{
			&migrations.OpRawSQL{Up: buf.String()},
		},
	}, nil
}
