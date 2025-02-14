// SPDX-License-Identifier: Apache-2.0

package migrations

import (
	"fmt"
	"strings"

	"github.com/lib/pq"
)

// ConstraintSQLWriter is a helper struct to write constraint SQL
// statements.
// It can generate SQL for unique, check, primary key, foreign key and exclude constraints.
// The generated SQL can be used in CREATE TABLE or ALTER TABLE statements both
// as an inline constraint or as a table level constraint.
type ConstraintSQLWriter struct {
	Name              string
	Columns           []string
	InitiallyDeferred bool
	Deferrable        bool
	SkipValidation    bool

	// unique, exclude, primary key constraints support the following options
	IncludeColumns    []string
	StorageParameters string
	Tablespace        string
}

// WriterUnique generates a unique constraint.
// Supported options:
// - nullsNotDistinct: if true, NULL values are considered equal.
// - includeColumns: additional columns to include in the index.
// - storageParameters: storage parameters for the index.
// - tablespace: tablespace for the index.
// - deferrable: if true, the constraint is deferrable.
// - initiallyDeferred: if true, the constraint is initially deferred.
func (w *ConstraintSQLWriter) WriteUnique(nullsNotDistinct bool) string {
	var constraint string
	if w.Name != "" {
		constraint = fmt.Sprintf("CONSTRAINT %s ", pq.QuoteIdentifier(w.Name))
	}
	nullsDistinct := ""
	if nullsNotDistinct {
		nullsDistinct = "NULLS NOT DISTINCT"
	}
	constraint += fmt.Sprintf("UNIQUE %s (%s)", nullsDistinct, strings.Join(quoteColumnNames(w.Columns), ", "))
	constraint += w.addIndexParameters()
	constraint += w.addDeferrable()
	return constraint
}

// WriteCheck generates a check constraint.
// Supported options:
// - noInherit: if true, the constraint is not inherited by child tables.
// - skipValidation: if true, the constraint is not validated.
func (w *ConstraintSQLWriter) WriteCheck(check string, noInherit bool) string {
	constraint := ""
	if w.Name != "" {
		constraint = fmt.Sprintf("CONSTRAINT %s ", pq.QuoteIdentifier(w.Name))
	}
	if !strings.HasPrefix(check, "CHECK (") {
		constraint += fmt.Sprintf("CHECK (%s)", check)
	} else {
		constraint += check
	}
	if noInherit {
		constraint += " NO INHERIT"
	}
	constraint += w.addNotValid()
	return constraint
}

// WritePrimaryKey generates a primary key constraint.
// Supported options:
// - includeColumns: additional columns to include in the index.
// - storageParameters: storage parameters for the index.
// - tablespace: tablespace for the index.
// - deferrable: if true, the constraint is deferrable.
// - initiallyDeferred: if true, the constraint is initially deferred.
func (w *ConstraintSQLWriter) WritePrimaryKey() string {
	constraint := ""
	if w.Name != "" {
		constraint = fmt.Sprintf("CONSTRAINT %s ", pq.QuoteIdentifier(w.Name))
	}
	constraint += fmt.Sprintf("PRIMARY KEY (%s)", strings.Join(quoteColumnNames(w.Columns), ", "))
	constraint += w.addIndexParameters()
	constraint += w.addDeferrable()
	return constraint
}

// WriteForeignKey generates a foreign key constraint on the table level and inline.
// Supported options:
// - includeColumns: additional columns to include in the index.
// - storageParameters: storage parameters for the index.
// - tablespace: tablespace for the index.
// - deferrable: if true, the constraint is deferrable.
// - initiallyDeferred: if true, the constraint is initially deferred.
// - skipValidation: if true, the constraint is not validated.
func (w *ConstraintSQLWriter) WriteForeignKey(referencedTable string, referencedColumns []string, onDelete, onUpdate ForeignKeyAction, setColumns []string, matchType ForeignKeyMatchType) string {
	onDeleteAction := string(ForeignKeyActionNOACTION)
	if onDelete != "" {
		onDeleteAction = strings.ToUpper(string(onDelete))
		if len(setColumns) != 0 {
			onDeleteAction += " (" + strings.Join(quoteColumnNames(setColumns), ", ") + ")"
		}
	}
	onUpdateAction := string(ForeignKeyActionNOACTION)
	if onUpdate != "" {
		onUpdateAction = strings.ToUpper(string(onUpdate))
	}
	matchTypeStr := string(ForeignKeyMatchTypeSIMPLE)
	if matchType != "" {
		matchTypeStr = strings.ToUpper(string(matchType))
	}

	constraint := ""
	if w.Name != "" {
		constraint = fmt.Sprintf("CONSTRAINT %s ", pq.QuoteIdentifier(w.Name))
	}
	// in case of in line foreign key constraint, columns are already included in the column definition
	if len(w.Columns) != 0 {
		constraint += fmt.Sprintf("FOREIGN KEY (%s) ", strings.Join(quoteColumnNames(w.Columns), ", "))
	}
	constraint += fmt.Sprintf("REFERENCES %s (%s) MATCH %s ON DELETE %s ON UPDATE %s",
		pq.QuoteIdentifier(referencedTable),
		strings.Join(quoteColumnNames(referencedColumns), ", "),
		matchTypeStr,
		onDeleteAction,
		onUpdateAction,
	)
	constraint += w.addDeferrable()
	constraint += w.addNotValid()
	return constraint
}

// WriteExclude generates an exclude constraint.
// Supported options:
// - includeColumns: additional columns to include in the index.
// - storageParameters: storage parameters for the index.
// - tablespace: tablespace for the index.
// - deferrable: if true, the constraint is deferrable.
// - initiallyDeferred: if true, the constraint is initially deferred.
func (w *ConstraintSQLWriter) WriteExclude(indexMethod, elements, predicate string) string {
	constraint := ""
	if w.Name != "" {
		constraint = fmt.Sprintf("CONSTRAINT %s ", pq.QuoteIdentifier(w.Name))
	}
	constraint += fmt.Sprintf("EXCLUDE USING %s (%s)", indexMethod, elements)
	constraint += w.addIndexParameters()
	if predicate != "" {
		constraint += fmt.Sprintf(" WHERE (%s)", predicate)
	}
	constraint += w.addDeferrable()
	return constraint
}

func (w *ConstraintSQLWriter) addIndexParameters() string {
	constraint := ""
	if len(w.IncludeColumns) != 0 {
		constraint += fmt.Sprintf(" INCLUDE (%s)", strings.Join(quoteColumnNames(w.IncludeColumns), ", "))
	}
	if w.StorageParameters != "" {
		constraint += fmt.Sprintf(" WITH (%s)", w.StorageParameters)
	}
	if w.Tablespace != "" {
		constraint += fmt.Sprintf(" USING INDEX TABLESPACE %s", w.Tablespace)
	}
	return constraint
}

func (w *ConstraintSQLWriter) addDeferrable() string {
	if !w.Deferrable {
		return ""
	}
	deferrable := " DEFERRABLE"
	if w.InitiallyDeferred {
		deferrable += " INITIALLY DEFERRED"
	} else {
		deferrable += " INITIALLY IMMEDIATE"
	}
	return deferrable
}

func (w *ConstraintSQLWriter) addNotValid() string {
	if w.SkipValidation {
		return " NOT VALID"
	}
	return ""
}
