package cmd

import (
	"bytes"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"sync"

	"github.com/spf13/cobra"
)

func statusHandler(w http.ResponseWriter, r *http.Request) {
	var wg sync.WaitGroup
	wg.Add(1)

	buf := new(bytes.Buffer)

	go func() {
		ctx := r.Context()
		status, err := getStatus(ctx)
		if err != nil {
			json.NewEncoder(buf).Encode(map[string]string{"error": err.Error()})
		} else {
			buf.Write(status)
		}

		wg.Done()
	}()

	wg.Wait()

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	w.Write(buf.Bytes())
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
