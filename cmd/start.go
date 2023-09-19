package cmd

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"path/filepath"
	"strings"

	"github.com/xataio/pg-roll/pkg/migrations"
	"github.com/xataio/pg-roll/pkg/roll"

	"github.com/spf13/cobra"
)

func startCmd() *cobra.Command {
	var complete bool

	startCmd := &cobra.Command{
		Use:   "start <file>",
		Short: "Start a migration for the operations present in the given file",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			fileName := args[0]

			migration, err := migrations.ReadMigrationFile(args[0])
			if err != nil {
				return fmt.Errorf("reading migration file: %w", err)
			}

			version := strings.TrimSuffix(filepath.Base(fileName), filepath.Ext(fileName))

			viewName, err := startMigration(cmd.Context(), version, migration, complete)
			if err != nil {
				return fmt.Errorf("starting migration: %w", err)
			}

			fmt.Printf("Migration successful! New version of the schema available under postgres '%s' schema\n", viewName)
			return nil
		},
	}

	startCmd.Flags().BoolVarP(&complete, "complete", "c", false, "Mark the migration as complete")

	return startCmd
}

func startHttp(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")

	if r.Method != http.MethodPost {
		w.WriteHeader(http.StatusMethodNotAllowed)
		json.NewEncoder(w).Encode(map[string]string{"error": "method not allowed"})
		return
	}

	var body struct {
		Version   string                `json:"version"`
		Migration *migrations.Migration `json:"migration"`
	}
	err := json.NewDecoder(r.Body).Decode(&body)
	if err != nil {
		w.WriteHeader(http.StatusBadRequest)
		json.NewEncoder(w).Encode(map[string]string{"error": err.Error()})
		return
	}

	ctx := r.Context()
	viewName, err := startMigration(ctx, body.Version, body.Migration, false)
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		json.NewEncoder(w).Encode(map[string]string{"error": err.Error()})
	} else {
		w.WriteHeader(http.StatusOK)
		json.NewEncoder(w).Encode(map[string]string{"view": viewName})
	}
}

func startMigration(ctx context.Context, version string, migration *migrations.Migration, complete bool) (string, error) {
	m, err := NewRoll(ctx)
	if err != nil {
		return "", err
	}
	defer m.Close()

	err = m.Start(ctx, migration)
	if err != nil {
		return "", err
	}

	if complete {
		if err = m.Complete(ctx); err != nil {
			return "", err
		}
	}

	viewName := roll.VersionedSchemaName(Schema, version)
	return viewName, nil
}
