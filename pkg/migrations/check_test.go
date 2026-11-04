// SPDX-License-Identifier: Apache-2.0

package migrations

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestCheckConstraintValidate(t *testing.T) {
	t.Parallel()

	t.Run("Name required", func(t *testing.T) {
		check := &CheckConstraint{
			Name: "",
		}
		err := check.Validate()
		assert.EqualError(t, err, `field "name" is required`)
	})
	t.Run("Name length", func(t *testing.T) {
		check := &CheckConstraint{
			Name: strings.Repeat("x", maxIdentifierLength+1),
		}
		err := check.Validate()
		assert.EqualError(t, err, `length of "xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx" (64) exceeds maximum length of 63`)
	})
}

func TestRewriteCheckExpression(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		check    string
		columns  []string
		expected string
	}{
		{
			name:     "simple check expression",
			check:    "age > 18",
			columns:  []string{"age"},
			expected: "_pgroll_new_age > 18",
		},
		{
			name:     "function containing column name as substring",
			check:    "subpath(path, 2, 1) = 'foo'",
			columns:  []string{"path"},
			expected: "subpath(_pgroll_new_path, 2, 1) = 'foo'",
		},
		{
			name:     "string literal matching column name",
			check:    "subpath(path, 2, 1) = 'path'",
			columns:  []string{"path"},
			expected: "subpath(_pgroll_new_path, 2, 1) = 'path'",
		},
		{
			name:     "quoted identifier",
			check:    `"path" > 0`,
			columns:  []string{"path"},
			expected: `"_pgroll_new_path" > 0`,
		},
		{
			name:     "multiple columns",
			check:    "path != other_path AND length(path) > 0 AND age > 21",
			columns:  []string{"path", "age"},
			expected: "_pgroll_new_path != other_path AND length(_pgroll_new_path) > 0 AND _pgroll_new_age > 21",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			res := rewriteCheckExpression(tc.check, tc.columns...)
			assert.Equal(t, tc.expected, res)
		})
	}
}

