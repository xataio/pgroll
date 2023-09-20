package cmd

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"

	"github.com/spf13/cobra"
)

var completeCmd = &cobra.Command{
	Use:   "complete <file>",
	Short: "Complete an ongoing migration with the operations present in the given file",
	RunE: func(cmd *cobra.Command, args []string) error {
		err := completeMigration(cmd.Context())
		if err != nil {
			return err
		}

		fmt.Println("Migration successful!")
		return nil
	},
}

func handleComplete(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")

	ctx := r.Context()
	if err := completeMigration(ctx); err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		json.NewEncoder(w).Encode(map[string]string{"error": err.Error()})
		return
	}

	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(map[string]string{"status": "ok"})
}

func completeMigration(ctx context.Context) error {
	m, err := NewRoll(ctx)
	if err != nil {
		return err
	}
	defer m.Close()

	err = m.Complete(ctx)
	if err != nil {
		return err
	}

	return nil
}
