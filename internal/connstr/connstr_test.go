// SPDX-License-Identifier: Apache-2.0

package connstr_test

import (
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
