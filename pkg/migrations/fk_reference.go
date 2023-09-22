// SPDX-License-Identifier: Apache-2.0

package migrations

import "github.com/xataio/pgroll/pkg/schema"

type ForeignKeyReference struct {
	Name   string `json:"name"`
	Table  string `json:"table"`
	Column string `json:"column"`
}

func (f *ForeignKeyReference) Validate(s *schema.Schema) error {
	if f.Name == "" {
		return FieldRequiredError{Name: "name"}
	}

	table := s.GetTable(f.Table)
	if table == nil {
		return TableDoesNotExistError{Name: f.Table}
	}

	column := table.GetColumn(f.Column)
	if column == nil {
		return ColumnDoesNotExistError{Table: f.Table, Name: f.Column}
	}

	return nil
}
