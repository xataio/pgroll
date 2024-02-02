// SPDX-License-Identifier: Apache-2.0

package migrations

func (c *Column) IsNullable() bool {
	if c.Nullable != nil {
		return *c.Nullable
	}
	return false
}

func (c *Column) IsUnique() bool {
	if c.Unique != nil {
		return *c.Unique
	}
	return false
}

func (c *Column) IsPrimaryKey() bool {
	if c.Pk != nil {
		return *c.Pk
	}
	return false
}
