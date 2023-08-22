package cmd

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/xataio/pg-roll/pkg/state"

	"github.com/spf13/cobra"
)

type statusLine struct {
	Schema  string
	Version string
	Status  string
}

var statusCmd = &cobra.Command{
	Use:   "status",
	Short: "Show pgroll status",
	RunE: func(cmd *cobra.Command, _ []string) error {
		ctx := cmd.Context()
		state, err := state.New(ctx, PGURL, StateSchema)
		if err != nil {
			return err
		}
		defer state.Close()

		statusLine, err := statusForSchema(ctx, state, Schema)
		if err != nil {
			return err
		}

		statusJSON, err := json.MarshalIndent(statusLine, "", "  ")
		if err != nil {
			return err
		}

		fmt.Println(string(statusJSON))
		return nil
	},
}

func statusForSchema(ctx context.Context, state *state.State, schema string) (*statusLine, error) {
	latestVersion, err := state.LatestVersion(ctx, schema)
	if err != nil {
		return nil, err
	}
	if latestVersion == nil {
		latestVersion = new(string)
	}

	isActive, err := state.IsActiveMigrationPeriod(ctx, schema)
	if err != nil {
		return nil, err
	}

	var status string
	if *latestVersion == "" {
		status = "No migrations"
	} else if isActive {
		status = "In Progress"
	} else {
		status = "Complete"
	}

	return &statusLine{
		Schema:  schema,
		Version: *latestVersion,
		Status:  status,
	}, nil
}
