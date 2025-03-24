package cmd

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/spf13/cobra"
	"gopkg.in/yaml.v3"
	"github.com/xataio/pgroll/pkg/migrations"
	"github.com/xataio/pgroll/pkg/sql2pgroll"
)

func convertCmd() *cobra.Command {
	var migrationName string

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

			if migrationName == "{current_timestamp}" {
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

	convertCmd.Flags().StringVarP(&migrationName, "name", "n", "{current_timestamp}", "Name of the migration")

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

func detectFileFormat(filename string) (string, error) {
	ext := strings.ToLower(filepath.Ext(filename))
	switch ext {
	case ".json":
		return "json", nil
	case ".yaml", ".yml":
		return "yaml", nil
	default:
		return "", fmt.Errorf("unsupported file format: %s", ext)
	}
}

func readMigrationFile(filename string) (*migrations.Migration, error) {
	format, err := detectFileFormat(filename)
	if err != nil {
		return nil, err
	}

	file, err := os.Open(filename)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	switch format {
	case "json":
		return migrations.ReadMigration(file)
	case "yaml":
		return migrations.ReadMigrationYAML(file)
	default:
		return nil, fmt.Errorf("unsupported file format: %s", format)
	}
}
