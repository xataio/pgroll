// SPDX-License-Identifier: Apache-2.0

package migrations

import (
	"strings"

	"github.com/xataio/pgroll/pkg/schema"
)

// Validate checks that the ForeignKeyReference is valid
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

	switch strings.ToUpper(string(f.OnDelete)) {
	case string(ForeignKeyReferenceOnDeleteNOACTION):
	case string(ForeignKeyReferenceOnDeleteRESTRICT):
	case string(ForeignKeyReferenceOnDeleteSETDEFAULT):
	case string(ForeignKeyReferenceOnDeleteSETNULL):
	case string(ForeignKeyReferenceOnDeleteCASCADE):
	case "":
		break
	default:
		return InvalidOnDeleteSettingError{Name: f.Name, Setting: string(f.OnDelete)}
	}

	return nil
}
