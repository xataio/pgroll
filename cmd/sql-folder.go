// SPDX-License-Identifier: Apache-2.0

package cmd

import (
	"bufio"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/spf13/cobra"
	"github.com/xataio/pgroll/pkg/sql2pgroll"
)

func sqlFolderCmd() *cobra.Command {
	sqlFolderCmd := &cobra.Command{
		Use:    "sql-folder <path to file with migrations>",
		Short:  "Convert SQL statements to pgroll operations from SQL files in a folder",
		Args:   cobra.ExactArgs(1),
		Hidden: true,
		RunE: func(cmd *cobra.Command, args []string) error {
			folder := args[0]

			sqls, err := readSQLFromFolder(folder)
			if err != nil {
				return err
			}

			for _, sql := range sqls {
				ops, err := sql2pgroll.Convert(sql)
				if err != nil {
					return fmt.Errorf("failed to convert SQL statement: %w", err)
				}

				enc := json.NewEncoder(os.Stdout)
				enc.SetIndent("", "  ")
				if err := enc.Encode(ops); err != nil {
					return fmt.Errorf("failed to encode operations: %w", err)
				}
			}

			return nil
		},
	}

	return sqlFolderCmd
}

func readSQLFromFolder(folder string) ([]string, error) {
	sqlStatements := make([]string, 0)
	files, err := os.ReadDir(folder)
	if err != nil {
		return nil, err
	}

	for _, file := range files {
		if file.IsDir() {
			continue
		}
		// open file reader
		reader, err := os.Open(filepath.Join(folder, file.Name()))
		if err != nil {
			return nil, err
		}

		// read file and remove sql comments from lines
		contentsWithoutComments := ""
		scanner := bufio.NewScanner(reader)
		for scanner.Scan() {
			if strings.HasPrefix(scanner.Text(), "--") {
				continue
			}
			contentsWithoutComments += scanner.Text()
		}

		for _, sqlStatement := range strings.Split(contentsWithoutComments, ";") {
			if sqlStatement == "" {
				continue
			}
			sqlStatements = append(sqlStatements, sqlStatement)
		}
	}
	return sqlStatements, nil

}
