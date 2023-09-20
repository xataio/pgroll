package cmd

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"

	"github.com/spf13/cobra"
)

var rollbackCmd = &cobra.Command{
	Use:   "rollback <file>",
	Short: "Roll back an ongoing migration",
	RunE: func(cmd *cobra.Command, args []string) error {
		if err := rollback(cmd.Context()); err != nil {
			return err
		}

		fmt.Printf("Migration rolled back. Changes made since the last version have been reverted.\n")
		return nil
	},
}

func handleRollback(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")

	ctx := r.Context()
	if err := rollback(ctx); err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		json.NewEncoder(w).Encode(map[string]string{"error": err.Error()})
		return
	}

	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(map[string]string{"status": "ok"})
}

func rollback(ctx context.Context) error {
	m, err := NewRoll(ctx)
	if err != nil {
		return err
	}
	defer m.Close()

	err = m.Rollback(ctx)
	if err != nil {
		return err
	}

	return nil
}
