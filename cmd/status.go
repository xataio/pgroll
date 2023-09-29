// SPDX-License-Identifier: Apache-2.0

package cmd

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/xataio/pgroll/cmd/flags"
	"github.com/xataio/pgroll/pkg/state"

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
		state, err := state.New(ctx, flags.PostgresURL(), flags.StateSchema())
		if err != nil {
			return err
		}
		defer state.Close()

		statusLine, err := statusForSchema(ctx, state, flags.StateSchema())
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

func statusForSchema(ctx context.Context, st *state.State, schema string) (*statusLine, error) {
	latestVersion, err := st.LatestVersion(ctx, schema)
	if err != nil {
		return nil, err
	}
	if latestVersion == nil {
		latestVersion = new(string)
	}

	isActive, err := st.IsActiveMigrationPeriod(ctx, schema)
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
