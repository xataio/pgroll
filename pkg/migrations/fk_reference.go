// SPDX-License-Identifier: Apache-2.0

package migrations

import (
	"fmt"
	"strings"

	"github.com/xataio/pgroll/pkg/schema"
)

// Validate checks that the ForeignKeyReference is valid
func (f *ForeignKeyReference) Validate(s *schema.Schema) error {
	if f.Name == "" {
		return FieldRequiredError{Name: "name"}
	}

	if err := ValidateIdentifierLength(f.Name); err != nil {
		return err
	}

	if f.Columns != nil && f.Column != nil {
		return fmt.Errorf("only one of column or columns is allowed")
	}

	table := s.GetTable(f.Table)
	if table == nil {
		return TableDoesNotExistError{Name: f.Table}
	}

	// check if table reference
	if f.Columns != nil && len(f.Columns) == 0 {
		// check if primary key exists in case of table reference
		if len(table.PrimaryKey) == 0 {
			return PrimaryKeyDoesNotExistError{Table: f.Table}
		}
	}
	// check if single column reference
	if f.Column != nil {
		// check if column exists in case of column reference
		column := table.GetColumn(*f.Column)
		if column == nil {
			return ColumnDoesNotExistError{Table: f.Table, Name: *f.Column}
		}
	}
	// check if multiple column reference
	if f.Columns != nil {
		for _, col := range f.Columns {
			// check if column exists in case of column reference
			column := table.GetColumn(col)
			if column == nil {
				return ColumnDoesNotExistError{Table: f.Table, Name: col}
			}
		}
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
