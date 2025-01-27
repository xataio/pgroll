package main

import (
	"encoding/json"
	"fmt"
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

// Command represents the structure for a command with its flags and arguments
type Command struct {
	Name        string    `json:"name"`
	Short       string    `json:"short"`
	Use         string    `json:"use"`
	Example     string    `json:"example"`
	Flags       []Flag    `json:"flags"`
	Subcommands []Command `json:"subcommands"`
	Args        []string  `json:"args"`
}

// Flag represents the structure for a flag
type Flag struct {
	Name        string `json:"name"`
	Shorthand   string `json:"shorthand,omitempty"`
	Description string `json:"description"`
	Default     string `json:"default"`
}

func main() {
	fmt.Println("Generating CLI JSON schema...")

	root := cmd.Prepare()

	result := Result{
		Name:     root.Name(),
		Commands: make([]Command, 0),
		Flags:    make([]Flag, 0),
	}

	for _, cmd := range root.Commands() {
		processCommand(&result.Commands, cmd)
	}

	flags := make([]Flag, 0)
	root.PersistentFlags().VisitAll(func(flag *pflag.Flag) {
		flags = append(flags, Flag{
			Name:        flag.Name,
			Shorthand:   flag.Shorthand,
			Description: flag.Usage,
			Default:     flag.DefValue,
		})
	})
	result.Flags = flags

	// Write the JSON schema to a file
	file, err := os.Create("cli-definition.json")
	if err != nil {
		fmt.Println("Error creating file:", err)
		return
	}
	defer file.Close()

	encoder := json.NewEncoder(file)
	encoder.SetIndent("", "  ")
	encoder.SetEscapeHTML(false)
	err = encoder.Encode(result)
	if err != nil {
		fmt.Println("Error encoding JSON:", err)
		return
	}

	fmt.Println("CLI JSON schema generated successfully")
}

func processCommand(commands *[]Command, cmd *cobra.Command) {
	command := Command{
		Name:    cmd.Name(),
		Short:   cmd.Short,
		Use:     cmd.Use,
		Example: cmd.Example,
	}

	if cmd.ValidArgs != nil {
		command.Args = cmd.ValidArgs
	} else {
		command.Args = []string{}
	}

	flags := make([]Flag, 0)
	cmd.Flags().VisitAll(func(flag *pflag.Flag) {
		flags = append(flags, Flag{
			Name:        flag.Name,
			Shorthand:   flag.Shorthand,
			Description: flag.Usage,
			Default:     flag.DefValue,
		})
	})
	command.Flags = flags

	subcommands := make([]Command, 0)
	for _, subcmd := range cmd.Commands() {
		processCommand(&subcommands, subcmd)
	}
	command.Subcommands = subcommands

	*commands = append(*commands, command)
}
