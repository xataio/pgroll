// SPDX-License-Identifier: Apache-2.0

package migrations

// IsNullable returns true if the column is nullable
func (c *Column) IsNullable() bool {
	return c.Nullable
}

// IsUnique returns true if the column values must be unique
func (c *Column) IsUnique() bool {
	return c.Unique
}

// IsPrimaryKey returns true if the column is part of the primary key
func (c *Column) IsPrimaryKey() bool {
	return c.Pk
}

// HasImplicitDefault returns true if the column has an implicit default value
func (c *Column) HasImplicitDefault() bool {
	switch c.Type {
	case "smallserial", "serial", "bigserial":
		return true
	default:
		return false
	}
}

// Validate returns true iff the column contains all fields required to create
// the column
func (c *Column) Validate() bool {
	if c.Name == "" {
		return false
	}
	if c.Type == "" {
		return false
	}
	return true
}
