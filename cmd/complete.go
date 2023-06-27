package cmd

import (
	"fmt"

	"github.com/spf13/cobra"
)

var completeCmd = &cobra.Command{
	Use:   "complete <file>",
	Short: "Complete an ongoing migration with the operations present in the given file",
	RunE: func(cmd *cobra.Command, args []string) error {
		m, err := NewRoll(cmd.Context())
		if err != nil {
			return err
		}
		defer m.Close()

		err = m.Complete(cmd.Context())
		if err != nil {
			return err
		}

		fmt.Println("Migration successful!")
		return nil
	},
}
