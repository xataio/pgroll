// SPDX-License-Identifier: Apache-2.0

package migrations

// Validate validates the UniqueConstraint
func (c *UniqueConstraint) Validate() error {
	if c.Name == "" {
		return FieldRequiredError{Name: "name"}
	}

	return nil
}
