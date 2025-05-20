// SPDX-License-Identifier: Apache-2.0

package main

import (
	"encoding/json"
	"fmt"
	"log"
	"os"

	"github.com/spf13/cobra"
	"github.com/spf13/pflag"
	"github.com/xataio/pgroll/cmd"
)

type Result struct {
	Name     string    `json:"name"`
	Commands []Command `json:"commands"`
	Flags    []Flag    `json:"flags"`
}

type Command struct {
	Name        string    `json:"name"`
	Short       string    `json:"short"`
	Use         string    `json:"use"`
	Hidden      bool      `json:"hidden,omitempty"`
	Example     string    `json:"example"`
	Flags       []Flag    `json:"flags"`
	Subcommands []Command `json:"subcommands"`
	Args        []string  `json:"args"`
}

type Flag struct {
	Name        string `json:"name"`
	Shorthand   string `json:"shorthand,omitempty"`
	Description string `json:"description"`
	Default     string `json:"default"`
}

func main() {
	fmt.Println("Generating CLI JSON schema...")

	rootCmd := cmd.Prepare()

	result := Result{
		Name:     rootCmd.Name(),
		Commands: extractCommands(rootCmd.Commands()),
		Flags:    extractFlags(rootCmd.PersistentFlags()),
	}

	if err := writeJSONToFile("cli-definition.json", result); err != nil {
		log.Fatalf("failed to write JSON to file: %v", err)
	}

	fmt.Println("CLI JSON schema generated successfully")
}

func extractCommands(cmds []*cobra.Command) []Command {
	if cmds == nil {
		return []Command{}
	}

	commands := make([]Command, 0, len(cmds))
	for _, cmd := range cmds {
		commands = append(commands, processCommand(cmd))
	}
	return commands
}

func processCommand(cmd *cobra.Command) Command {
	return Command{
		Name:        cmd.Name(),
		Short:       cmd.Short,
		Use:         cmd.Use,
		Hidden:      cmd.Hidden,
		Example:     cmd.Example,
		Args:        validateArgs(cmd),
		Flags:       extractFlags(cmd.Flags()),
		Subcommands: extractCommands(cmd.Commands()),
	}
}

func extractFlags(flagSet *pflag.FlagSet) []Flag {
	if flagSet == nil {
		return []Flag{}
	}

	flags := make([]Flag, 0, flagSet.NFlag())
	flagSet.VisitAll(func(flag *pflag.Flag) {
		flags = append(flags, Flag{
			Name:        flag.Name,
			Shorthand:   flag.Shorthand,
			Description: flag.Usage,
			Default:     flag.DefValue,
		})
	})
	return flags
}

func validateArgs(cmd *cobra.Command) []string {
	if cmd.Args == nil && cmd.ValidArgs == nil {
		return []string{}
	}

	maxArgs := 0
	for i := 0; i < 10; i++ {
		args := make([]string, i)
		for j := range args {
			args[j] = fmt.Sprintf("arg%d", j)
		}
		if err := cmd.Args(cmd, args); err == nil {
			maxArgs = i
		}
	}

	if maxArgs != len(cmd.ValidArgs) {
		log.Fatalf("Mismatch between maxArgs and ValidArgs for command: %s", cmd.Name())
	}

	if cmd.ValidArgs == nil {
		return []string{}
	}

	return cmd.ValidArgs
}

func writeJSONToFile(filename string, data any) error {
	file, err := os.Create(filename)
	if err != nil {
		return fmt.Errorf("failed to create file: %w", err)
	}
	defer file.Close()

	encoder := json.NewEncoder(file)
	encoder.SetIndent("", "  ")
	encoder.SetEscapeHTML(false)

	if err := encoder.Encode(data); err != nil {
		return fmt.Errorf("failed to encode JSON: %w", err)
	}
	return nil
}
