package cmd

import (
	"fmt"
	"log"
	"net/http"

	"github.com/spf13/cobra"
)

var serveCmd = &cobra.Command{
	Use:   "serve [port]",
	Short: "Start a server to handle pgroll commands",
	RunE: func(cmd *cobra.Command, args []string) error {
		port := ":8080"
		if len(args) > 0 {
			port = fmt.Sprintf(":%s", args[0])
		}

		http.HandleFunc("/status", statusHttp)
		http.HandleFunc("/start", startHttp)

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
