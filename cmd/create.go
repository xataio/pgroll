// SPDX-License-Identifier: Apache-2.0

package cmd

import (
	"encoding/json"
	"fmt"
	"os"
	"time"

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
			if name == "" {
				name, _ = pterm.DefaultInteractiveTextInput.
					WithDefaultText("Set the name of your migration").
					WithDefaultValue(time.Now().Format("20060102150405")).
					Show()
			}

			if outputFormat == "" {
				outputFormat, _ = pterm.DefaultInteractiveSelect.
					WithDefaultText("File format").
					WithOptions([]string{"yaml", "json"}).
					WithDefaultOption("yaml").
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

			file, _ := os.Create(fmt.Sprintf("%s.%s", name, outputFormat))
			defer file.Close()

			switch outputFormat {
			case "json":
				enc := json.NewEncoder(file)
				enc.SetIndent("", "  ")
				if err := enc.Encode(mig); err != nil {
					return fmt.Errorf("encode migration: %w", err)
				}
			case "yaml":
				out, err := yaml.Marshal(mig)
				if err != nil {
					return fmt.Errorf("encode migration: %w", err)
				}
				_, err = file.Write(out)
				if err != nil {
					return fmt.Errorf("write migration: %w", err)
				}
			}

			return nil
		},
	}
	createCmd.Flags().BoolVarP(&isEmpty, "empty", "e", false, "Create empty migration file")
	createCmd.Flags().StringVarP(&name, "name", "n", "", "Migration name")
	createCmd.Flags().StringVarP(&outputFormat, "output", "0", "", "Output format: yaml or json")

	return createCmd
}
