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

			viewName, err := startMigration(cmd.Context(), fileName, migration, complete)
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
		Name      string                `json:"name"`
		Migration *migrations.Migration `json:"migration"`
	}
	err := json.NewDecoder(r.Body).Decode(&body)
	if err != nil {
		w.WriteHeader(http.StatusBadRequest)
		json.NewEncoder(w).Encode(map[string]string{"error": err.Error()})
		return
	}

	ctx := r.Context()
	viewName, err := startMigration(ctx, body.Name, body.Migration, false)
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		json.NewEncoder(w).Encode(map[string]any{"success": false, "error": err.Error()})
	} else {
		w.WriteHeader(http.StatusOK)
		json.NewEncoder(w).Encode(map[string]any{"success": true, "view": viewName})
	}
}

func startMigration(ctx context.Context, name string, migration *migrations.Migration, complete bool) (string, error) {
	version := strings.TrimSuffix(filepath.Base(name), filepath.Ext(name))

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
