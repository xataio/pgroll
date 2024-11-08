// SPDX-License-Identifier: Apache-2.0

package migrations

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/xataio/pgroll/pkg/schema"
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
	t.Run("On delete casing", func(t *testing.T) {
		for _, onDelete := range []string{"cascade", "CASCADE", "RESTRICT", "resTRIct", "NO ACtiON", "no action", "SET NULL", "set null", "SET DEFAULT", "set default"} {
			t.Run(onDelete, func(t *testing.T) {
				fk := &ForeignKeyReference{
					Table:    "my_table",
					Name:     "fk",
					OnDelete: ptr(ForeignKeyReferenceOnDelete(onDelete)),
				}
				err := fk.Validate(&schema.Schema{Tables: map[string]schema.Table{"my_table": {}}})
				assert.NoError(t, err)
			})
		}
	})
}
