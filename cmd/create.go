// SPDX-License-Identifier: Apache-2.0

//nolint:goconst
package cmd

import (
	"encoding/json"
	"fmt"
	"os"
	"strings"

	"github.com/pterm/pterm"
	"github.com/spf13/cobra"
	"github.com/xataio/pgroll/pkg/migrations"
	"sigs.k8s.io/yaml"
)

func createCmd() *cobra.Command {
	var isEmpty bool
	var name string
	var outputFormat string

	createCmd := &cobra.Command{
		Use:   "create",
		Short: "Create a new migration interactively",
		RunE: func(cmd *cobra.Command, args []string) error {
			outputFormat := strings.ToLower(outputFormat)
			if outputFormat != "yaml" && outputFormat != "json" {
				return fmt.Errorf("invalid migration format: %q", outputFormat)
			}

			if name == "" {
				name, _ = pterm.DefaultInteractiveTextInput.
					WithDefaultText("Set the name of your migration").
					Show()
			}

			mig := &migrations.Migration{}
			addMoreOperations := !isEmpty

			for addMoreOperations {
				selectedOption, _ := pterm.DefaultInteractiveSelect.
					WithDefaultText("Select operation").
					WithOptions(migrations.AllNonDeprecatedOperations).
					Show()

				op, _ := migrations.OperationFromName(migrations.OpName(selectedOption))
				mig.Operations = append(mig.Operations, op)
				if operation, ok := op.(migrations.Createable); ok {
					operation.Create()
				}
				addMoreOperations, _ = pterm.DefaultInteractiveConfirm.
					WithDefaultText("Add more operations").
					Show()
			}

			migrationFileName := fmt.Sprintf("%s.%s", name, outputFormat)
			file, err := os.Create(migrationFileName)
			if err != nil {
				return fmt.Errorf("failed to create migration file: %w", err)
			}
			defer file.Close()

			switch outputFormat {
			case "json":
				enc := json.NewEncoder(file)
				enc.SetIndent("", "  ")
				if err := enc.Encode(mig); err != nil {
					return fmt.Errorf("failed to encode migration: %w", err)
				}
			case "yaml":
				out, err := yaml.Marshal(mig)
				if err != nil {
					return fmt.Errorf("failed to encode migration: %w", err)
				}
				_, err = file.Write(out)
				if err != nil {
					return fmt.Errorf("failed to write migration: %w", err)
				}
			default:
				return fmt.Errorf("invalid output format: %q", outputFormat)
			}

			pterm.Success.Println("Migration written to " + migrationFileName)

			return nil
		},
	}
	createCmd.Flags().BoolVarP(&isEmpty, "empty", "e", false, "Create empty migration file")
	createCmd.Flags().StringVarP(&name, "name", "n", "", "Migration name")
	createCmd.Flags().StringVarP(&outputFormat, "output", "o", "yaml", "Output format: yaml or json")

	return createCmd
}
