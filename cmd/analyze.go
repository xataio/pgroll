package cmd

import (
	"encoding/json"
	"fmt"

	"github.com/spf13/cobra"

	"pg-roll/pkg/state"
)

var analyzeCmd = &cobra.Command{
	Use:    "analyze <schemaName>",
	Short:  "Analyze the SQL schema of the target database",
	Hidden: true,
	Args:   cobra.MaximumNArgs(1),
	RunE: func(cmd *cobra.Command, args []string) error {
		ctx := cmd.Context()
		state, err := state.New(ctx, PGURL, StateSchema)
		if err != nil {
			return err
		}
		defer state.Close()

		if len(args) == 0 {
			args = []string{"public"}
		}
		schemaName := args[0]

		schema, err := state.ReadSchema(ctx, schemaName)
		if err != nil {
			return err
		}

		schemaJSON, err := json.MarshalIndent(schema, "", "  ")
		if err != nil {
			return err
		}

		fmt.Println(string(schemaJSON))
		return nil
	},
}
