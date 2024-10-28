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
