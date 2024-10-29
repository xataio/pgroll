// SPDX-License-Identifier: Apache-2.0

package migrations

// Validate checks that the CheckConstraint is valid
func (c *CheckConstraint) Validate() error {
	if c.Name == "" {
		return FieldRequiredError{Name: "name"}
	}

	if err := ValidateIdentifierLength(c.Name); err != nil {
		return err
	}

	if c.Constraint == "" {
		return FieldRequiredError{Name: "constraint"}
	}

	return nil
}
