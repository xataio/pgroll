package cmd

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"

	"github.com/spf13/cobra"
	"github.com/xataio/pg-roll/pkg/migrations"
)

func statusHandler(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")

	ctx := r.Context()
	status, err := getStatus(ctx)
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		json.NewEncoder(w).Encode(map[string]string{"error": err.Error()})
	} else {
		w.WriteHeader(http.StatusOK)
		w.Write(status)
	}
}

type StartBody struct {
	Version   string                `json:"version"`
	Migration *migrations.Migration `json:"migration"`
}

func startHandler(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")

	if r.Method != http.MethodPost {
		w.WriteHeader(http.StatusMethodNotAllowed)
		json.NewEncoder(w).Encode(map[string]string{"error": "method not allowed"})
		return
	}

	var body StartBody
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

var serveCmd = &cobra.Command{
	Use:   "serve [port]",
	Short: "Start a server to handle pgroll commands",
	RunE: func(cmd *cobra.Command, args []string) error {
		port := ":8080"
		if len(args) > 0 {
			port = fmt.Sprintf(":%s", args[0])
		}

		http.HandleFunc("/status", statusHandler)
		http.HandleFunc("/start", startHandler)

		srv := &http.Server{
			Addr:    port,
			Handler: nil,
		}

		log.Printf("Starting server on %s\n", port)
		err := srv.ListenAndServe()
		if err != nil {
			log.Fatal("Error starting server: ", err)
		}

		return nil
	},
}
