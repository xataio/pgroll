package cmd

import (
	"database/sql"
	"encoding/json"
	"fmt"

	"github.com/covrom/goerd/drivers/postgres"
	"github.com/covrom/goerd/schema"
	"github.com/spf13/cobra"
)

var analyzeCmd = &cobra.Command{
	Use:    "analyze",
	Short:  "Analyze the SQL schema of the target database",
	Hidden: true,
	RunE: func(_ *cobra.Command, _ []string) error {
		db, err := sql.Open("postgres", PGURL)
		if err != nil {
			return err
		}
		defer db.Close()

		driver := postgres.New(db)
		s := &schema.Schema{}
		err = driver.Analyze(s)
		if err != nil {
			return err
		}

		schemaJSON, err := json.MarshalIndent(s, "", "  ")
		if err != nil {
			return err
		}

		fmt.Println(string(schemaJSON))
		return nil
	},
}
