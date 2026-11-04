// SPDX-License-Identifier: Apache-2.0

package migrations

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestRewriteCheckExpression(t *testing.T) {
	t.Parallel()

	testCases := []struct {
		name     string
		check    string
		columns  []string
		expected string
	}{
		{
			name:     "unquoted column",
			check:    "age > 18",
			columns:  []string{"age"},
			expected: "_pgroll_new_age > 18",
		},
		{
			name:     "quoted column",
			check:    `"age" > 18`,
			columns:  []string{"age"},
			expected: `"_pgroll_new_age" > 18`,
		},
		{
			name:     "quoted column with uppercase letters",
			check:    `"Age" > 18 AND age > 0`,
			columns:  []string{"Age"},
			expected: `"_pgroll_new_Age" > 18 AND age > 0`,
		},
		{
			name:     "unquoted column is case insensitive",
			check:    "AGE > 18",
			columns:  []string{"age"},
			expected: "_pgroll_new_age > 18",
		},
		{
			name:     "function name containing the column name",
			check:    "subpath(path, 2, 1) = 'foo'",
			columns:  []string{"path"},
			expected: "subpath(_pgroll_new_path, 2, 1) = 'foo'",
		},
		{
			name:     "other column containing the column name",
			check:    "id > 0 AND user_id > 0",
			columns:  []string{"id"},
			expected: "_pgroll_new_id > 0 AND user_id > 0",
		},
		{
			name:     "string literal containing the column name",
			check:    "status <> 'no status'",
			columns:  []string{"status"},
			expected: "_pgroll_new_status <> 'no status'",
		},
		{
			name:     "function with the same name as the column",
			check:    "length(length) > 0",
			columns:  []string{"length"},
			expected: "length(_pgroll_new_length) > 0",
		},
		{
			name:     "type with the same name as the column",
			check:    "path::text <> '' AND path::path IS NOT NULL",
			columns:  []string{"path"},
			expected: "_pgroll_new_path::text <> '' AND _pgroll_new_path::path IS NOT NULL",
		},
		{
			name:     "column named like a keyword",
			check:    `"name" <> '' AND value > 0`,
			columns:  []string{"name", "value"},
			expected: `"_pgroll_new_name" <> '' AND _pgroll_new_value > 0`,
		},
		{
			name:     "multiple columns",
			check:    `"name" != "nick"`,
			columns:  []string{"name", "nick"},
			expected: `"_pgroll_new_name" != "_pgroll_new_nick"`,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			assert.Equal(t, tc.expected, rewriteCheckExpression(tc.check, tc.columns...))
		})
	}
}
