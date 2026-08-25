// SPDX-License-Identifier: Apache-2.0

package connstr_test

import (
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/xataio/pgroll/internal/connstr"
)

func TestAppendSearchPathOption(t *testing.T) {
	tests := []struct {
		Name     string
		ConnStr  string
		Schema   string
		Expected string
	}{
		{
			Name:     "empty schema doesn't change connection string",
			ConnStr:  "postgres://postgres:postgres@localhost:5432?sslmode=disable",
			Schema:   "",
			Expected: "postgres://postgres:postgres@localhost:5432?sslmode=disable",
		},
		{
			Name:     "can set options as the only query parameter",
			ConnStr:  "postgres://postgres:postgres@localhost:5432",
			Schema:   "apples",
			Expected: "postgres://postgres:postgres@localhost:5432?options=-c%20search_path%3Dapples",
		},
		{
			Name:     "can set options as an additional query parameter",
			ConnStr:  "postgres://postgres:postgres@localhost:5432?sslmode=disable",
			Schema:   "bananas",
			Expected: "postgres://postgres:postgres@localhost:5432?options=-c%20search_path%3Dbananas&sslmode=disable",
		},
	}

	for _, tt := range tests {
		t.Run(tt.Name, func(t *testing.T) {
			result, err := connstr.AppendSearchPathOption(tt.ConnStr, tt.Schema)
			assert.NoError(t, err)

			assert.Equal(t, tt.Expected, result)
		})
	}
}

func TestDefaultURL(t *testing.T) {
	// The libpq environment variables that DefaultURL consults. Clearing them
	// before each case keeps the test independent of the host environment.
	envVars := []string{"PGHOST", "PGPORT", "PGUSER", "PGPASSWORD", "PGDATABASE", "PGSSLMODE"}

	tests := []struct {
		Name     string
		Env      map[string]string
		Expected string
	}{
		{
			Name:     "no environment variables gives the local default",
			Env:      nil,
			Expected: "postgres://postgres:postgres@localhost?sslmode=disable",
		},
		{
			Name:     "PGHOST overrides the host",
			Env:      map[string]string{"PGHOST": "db.example.com"},
			Expected: "postgres://postgres:postgres@db.example.com?sslmode=disable",
		},
		{
			Name:     "PGPORT is appended to the host",
			Env:      map[string]string{"PGPORT": "5433"},
			Expected: "postgres://postgres:postgres@localhost:5433?sslmode=disable",
		},
		{
			Name:     "PGUSER and PGPASSWORD override the credentials",
			Env:      map[string]string{"PGUSER": "alice", "PGPASSWORD": "s3cret"},
			Expected: "postgres://alice:s3cret@localhost?sslmode=disable",
		},
		{
			Name:     "PGDATABASE sets the database name",
			Env:      map[string]string{"PGDATABASE": "mydb"},
			Expected: "postgres://postgres:postgres@localhost/mydb?sslmode=disable",
		},
		{
			Name:     "PGSSLMODE overrides the sslmode",
			Env:      map[string]string{"PGSSLMODE": "require"},
			Expected: "postgres://postgres:postgres@localhost?sslmode=require",
		},
		{
			Name: "all variables set together",
			Env: map[string]string{
				"PGHOST":     "db.example.com",
				"PGPORT":     "6432",
				"PGUSER":     "alice",
				"PGPASSWORD": "s3cret",
				"PGDATABASE": "mydb",
				"PGSSLMODE":  "require",
			},
			Expected: "postgres://alice:s3cret@db.example.com:6432/mydb?sslmode=require",
		},
	}

	for _, tt := range tests {
		t.Run(tt.Name, func(t *testing.T) {
			for _, k := range envVars {
				t.Setenv(k, "")
				os.Unsetenv(k)
			}
			for k, v := range tt.Env {
				t.Setenv(k, v)
			}

			assert.Equal(t, tt.Expected, connstr.DefaultURL())
		})
	}
}
