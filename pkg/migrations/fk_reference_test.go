// SPDX-License-Identifier: Apache-2.0

package migrations

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestForeignKeyReferenceValidate(t *testing.T) {
	t.Parallel()

	t.Run("Name required", func(t *testing.T) {
		fk := &ForeignKeyReference{
			Name: "",
		}
		// For now none of the tests use the schema
		err := fk.Validate(nil)
		assert.EqualError(t, err, `field "name" is required`)
	})
	t.Run("Name length", func(t *testing.T) {
		fk := &ForeignKeyReference{
			Name: strings.Repeat("x", maxIdentifierLength+1),
		}
		// For now none of the tests use the schema
		err := fk.Validate(nil)
		assert.EqualError(t, err, `length of "xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx" (64) exceeds maximum length of 63`)
	})
}
