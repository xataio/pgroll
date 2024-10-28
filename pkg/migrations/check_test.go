// SPDX-License-Identifier: Apache-2.0

package migrations

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestCheckConstraintValidate(t *testing.T) {
	t.Run("Name required", func(t *testing.T) {
		check := &CheckConstraint{
			Name: "",
		}
		// For now none of the tests use the schema
		err := check.Validate()
		assert.EqualError(t, err, `field "name" is required`)
	})
	t.Run("Name length", func(t *testing.T) {
		check := &CheckConstraint{
			Name: strings.Repeat("x", maxIdentifierLength+1),
		}
		// For now none of the tests use the schema
		err := check.Validate()
		assert.EqualError(t, err, `length of "xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx" (64) exceeds maximum length of 63`)
	})
}
