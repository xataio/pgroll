package cmd

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"

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

		statusJSON, err := getStatus(ctx)
		if err != nil {
			return err
		}

		fmt.Println(string(statusJSON))
		return nil
	},
}

func handleStatus(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")

	ctx := r.Context()
	status, err := getStatus(ctx)
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		json.NewEncoder(w).Encode(map[string]string{"error": err.Error()})
		return
	}

	w.WriteHeader(http.StatusOK)
	w.Write(status)
}

func getStatus(ctx context.Context) ([]byte, error) {
	state, err := state.New(ctx, PGURL, StateSchema)
	if err != nil {
		return nil, err
	}
	defer state.Close()

	statusLine, err := statusForSchema(ctx, state, Schema)
	if err != nil {
		return nil, err
	}

	statusJSON, err := json.MarshalIndent(statusLine, "", "  ")
	if err != nil {
		return nil, err
	}

	return statusJSON, nil
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
