// SPDX-License-Identifier: Apache-2.0

package migrations

import "github.com/xataio/pgroll/pkg/schema"

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

	if f.OnDelete != nil {
		if *f.OnDelete != ForeignKeyReferenceOnDeleteNOACTION && *f.OnDelete != ForeignKeyReferenceOnDeleteRESTRICT && *f.OnDelete != ForeignKeyReferenceOnDeleteCASCADE && *f.OnDelete != ForeignKeyReferenceOnDeleteSETNULL && *f.OnDelete != ForeignKeyReferenceOnDeleteSETDEFAULT {
			return InvalidEnumValueError{Property: "onDelete", Value: string(*f.OnDelete)}
		}
	}

	return nil
}
