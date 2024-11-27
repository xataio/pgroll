// SPDX-License-Identifier: Apache-2.0

package migrations

// IsNullable returns true if the column is nullable
func (c *Column) IsNullable() bool {
	if c.Nullable != nil {
		return *c.Nullable
	}
	return false
}

// IsUnique returns true if the column values must be unique
func (c *Column) IsUnique() bool {
	if c.Unique != nil {
		return *c.Unique
	}
	return false
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
