// SPDX-License-Identifier: Apache-2.0

package migrations_test

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/xataio/pgroll/pkg/migrations"
)

func TestConstraintUnique(t *testing.T) {
	tests := map[string]struct {
		name              string
		columns           []string
		includeColumns    []string
		deferrable        bool
		initiallyDeferred bool
		storageParameters string
		tablespace        string
		nullsNotDistinct  bool
		expected          string
	}{
		"single column nulls distinct": {
			name:     "test",
			columns:  []string{"column"},
			expected: `CONSTRAINT "test" UNIQUE  ("column")`,
		},
		"single column nulls not distinct": {
			name:             "test",
			columns:          []string{"column"},
			nullsNotDistinct: true,
			expected:         `CONSTRAINT "test" UNIQUE NULLS NOT DISTINCT ("column")`,
		},
		"single column with storage options": {
			name:              "test",
			columns:           []string{"column"},
			storageParameters: "fillfactor=70",
			expected:          `CONSTRAINT "test" UNIQUE  ("column") WITH (fillfactor=70)`,
		},
		"single column with tablespace": {
			name:       "test",
			columns:    []string{"column"},
			tablespace: "test_tablespace",
			expected:   `CONSTRAINT "test" UNIQUE  ("column") USING INDEX TABLESPACE test_tablespace`,
		},
		"single column with storage options and tablespace": {
			name:              "test",
			columns:           []string{"column"},
			storageParameters: "fillfactor=70",
			tablespace:        "test_tablespace",
			expected:          `CONSTRAINT "test" UNIQUE  ("column") WITH (fillfactor=70) USING INDEX TABLESPACE test_tablespace`,
		},
		"single column with include columns": {
			name:           "test",
			columns:        []string{"column"},
			includeColumns: []string{"include_column", "other_include_column"},
			expected:       `CONSTRAINT "test" UNIQUE  ("column") INCLUDE ("include_column", "other_include_column")`,
		},
		"single column with include columns deferred": {
			name:           "test",
			columns:        []string{"column"},
			deferrable:     true,
			includeColumns: []string{"include_column", "other_include_column"},
			expected:       `CONSTRAINT "test" UNIQUE  ("column") INCLUDE ("include_column", "other_include_column") DEFERRABLE INITIALLY IMMEDIATE`,
		},
		"single column with include columns deferred initially deferred": {
			name:              "test",
			columns:           []string{"column"},
			deferrable:        true,
			initiallyDeferred: true,
			includeColumns:    []string{"include_column", "other_include_column"},
			expected:          `CONSTRAINT "test" UNIQUE  ("column") INCLUDE ("include_column", "other_include_column") DEFERRABLE INITIALLY DEFERRED`,
		},
		"single column with include columns nulls not distinct": {
			name:             "test",
			columns:          []string{"column"},
			includeColumns:   []string{"include_column", "other_include_column"},
			nullsNotDistinct: true,
			expected:         `CONSTRAINT "test" UNIQUE NULLS NOT DISTINCT ("column") INCLUDE ("include_column", "other_include_column")`,
		},
		"multi column nulls distinct": {
			name:     "test",
			columns:  []string{"column1", "column2"},
			expected: `CONSTRAINT "test" UNIQUE  ("column1", "column2")`,
		},
		"multi column nulls not distinct": {
			name:             "test",
			columns:          []string{"column1", "column2"},
			nullsNotDistinct: true,
			expected:         `CONSTRAINT "test" UNIQUE NULLS NOT DISTINCT ("column1", "column2")`,
		},
		"multi column with storage options and include columns": {
			name:              "test",
			columns:           []string{"column1", "column2"},
			storageParameters: "fillfactor=70",
			includeColumns:    []string{"include_column", "other_include_column"},
			expected:          `CONSTRAINT "test" UNIQUE  ("column1", "column2") INCLUDE ("include_column", "other_include_column") WITH (fillfactor=70)`,
		},
		"multi column without name": {
			columns:  []string{"column1", "column2"},
			expected: `UNIQUE  ("column1", "column2")`,
		},
		"multi column without name deferrable": {
			columns:    []string{"column1", "column2"},
			deferrable: true,
			expected:   `UNIQUE  ("column1", "column2") DEFERRABLE INITIALLY IMMEDIATE`,
		},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			writer := &migrations.ConstraintSQLWriter{
				Name:              tc.name,
				Columns:           tc.columns,
				Deferrable:        tc.deferrable,
				InitiallyDeferred: tc.initiallyDeferred,
				IncludeColumns:    tc.includeColumns,
				StorageParameters: tc.storageParameters,
				Tablespace:        tc.tablespace,
			}

			constraint := writer.WriteUnique(tc.nullsNotDistinct)
			assert.Equal(t, tc.expected, constraint)
		})
	}
}

func TestConstraintCheck(t *testing.T) {
	tests := map[string]struct {
		name           string
		check          string
		noInherit      bool
		skipValidation bool
		expected       string
	}{
		"simple check": {
			name:     "test",
			check:    "length(column) > 0 AND column != column_other",
			expected: `CONSTRAINT "test" CHECK (length(column) > 0 AND column != column_other)`,
		},
		"other simple check no inherit": {
			name:      "test",
			check:     "name != 'test'",
			noInherit: true,
			expected:  `CONSTRAINT "test" CHECK (name != 'test') NO INHERIT`,
		},
		"unnamed check": {
			check:    "length(column) > 0 AND column != column_other",
			expected: `CHECK (length(column) > 0 AND column != column_other)`,
		},
		"simple check skip validation": {
			name:           "test",
			check:          "length(column) > 0 AND column != column_other",
			skipValidation: true,
			expected:       `CONSTRAINT "test" CHECK (length(column) > 0 AND column != column_other) NOT VALID`,
		},
		"simple check not inheritable skip validation": {
			name:           "test",
			check:          "length(column) > 0 AND column != column_other",
			skipValidation: true,
			noInherit:      true,
			expected:       `CONSTRAINT "test" CHECK (length(column) > 0 AND column != column_other) NO INHERIT NOT VALID`,
		},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			writer := &migrations.ConstraintSQLWriter{
				Name:           tc.name,
				SkipValidation: tc.skipValidation,
			}

			constraint := writer.WriteCheck(tc.check, tc.noInherit)
			assert.Equal(t, tc.expected, constraint)
		})
	}
}

func TestConstraintPrimary(t *testing.T) {
	tests := map[string]struct {
		name              string
		columns           []string
		deferrable        bool
		initiallyDeferred bool
		expected          string
	}{
		"single primary key constraint": {
			name:     "test",
			columns:  []string{"column"},
			expected: `CONSTRAINT "test" PRIMARY KEY ("column")`,
		},
		"composite primary key": {
			name:     "test",
			columns:  []string{"column1", "column2"},
			expected: `CONSTRAINT "test" PRIMARY KEY ("column1", "column2")`,
		},
		"unnamed composite primary key": {
			columns:  []string{"column1", "column2"},
			expected: `PRIMARY KEY ("column1", "column2")`,
		},
		"composite primary key deferred": {
			name:       "test",
			columns:    []string{"column1", "column2"},
			deferrable: true,
			expected:   `CONSTRAINT "test" PRIMARY KEY ("column1", "column2") DEFERRABLE INITIALLY IMMEDIATE`,
		},
		"composite primary key deferrable initially deferred": {
			name:              "test",
			columns:           []string{"column1", "column2"},
			deferrable:        true,
			initiallyDeferred: true,
			expected:          `CONSTRAINT "test" PRIMARY KEY ("column1", "column2") DEFERRABLE INITIALLY DEFERRED`,
		},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			writer := &migrations.ConstraintSQLWriter{
				Name:              tc.name,
				Columns:           tc.columns,
				Deferrable:        tc.deferrable,
				InitiallyDeferred: tc.initiallyDeferred,
			}

			constraint := writer.WritePrimaryKey()
			assert.Equal(t, tc.expected, constraint)
		})
	}
}

func TestConstraintForeign(t *testing.T) {
	tests := map[string]struct {
		name               string
		columns            []string
		referencedTable    string
		referencedColumns  []string
		matchType          migrations.ForeignKeyMatchType
		onDelete           migrations.ForeignKeyAction
		onDeleteSetColumns []string
		onUpdate           migrations.ForeignKeyAction
		deferrable         bool
		initiallyDeferred  bool
		skipValidation     bool
		expected           string
	}{
		"inline foreign key": {
			referencedTable:   "other_table",
			referencedColumns: []string{"id"},
			expected:          `REFERENCES "other_table" ("id") MATCH SIMPLE ON DELETE NO ACTION ON UPDATE NO ACTION`,
		},
		"inline foreign key with referential actions": {
			referencedTable:   "other_table",
			referencedColumns: []string{"id"},
			onDelete:          migrations.ForeignKeyActionCASCADE,
			onUpdate:          migrations.ForeignKeyActionCASCADE,
			expected:          `REFERENCES "other_table" ("id") MATCH SIMPLE ON DELETE CASCADE ON UPDATE CASCADE`,
		},
		"inline foreign key with referential actions and match type": {
			referencedTable:   "other_table",
			referencedColumns: []string{"id"},
			onDelete:          migrations.ForeignKeyActionCASCADE,
			onUpdate:          migrations.ForeignKeyActionSETNULL,
			matchType:         migrations.ForeignKeyMatchTypeFULL,
			expected:          `REFERENCES "other_table" ("id") MATCH FULL ON DELETE CASCADE ON UPDATE SET NULL`,
		},
		"single foreign key": {
			name:              "test",
			columns:           []string{"other_id"},
			referencedTable:   "other_table",
			referencedColumns: []string{"id"},
			expected:          `CONSTRAINT "test" FOREIGN KEY ("other_id") REFERENCES "other_table" ("id") MATCH SIMPLE ON DELETE NO ACTION ON UPDATE NO ACTION`,
		},
		"single foreign key deferred": {
			name:              "test",
			columns:           []string{"other_id"},
			referencedTable:   "other_table",
			referencedColumns: []string{"id"},
			deferrable:        true,
			expected:          `CONSTRAINT "test" FOREIGN KEY ("other_id") REFERENCES "other_table" ("id") MATCH SIMPLE ON DELETE NO ACTION ON UPDATE NO ACTION DEFERRABLE INITIALLY IMMEDIATE`,
		},
		"single foreign key deferred initially deferred": {
			name:              "test",
			columns:           []string{"other_id"},
			referencedTable:   "other_table",
			referencedColumns: []string{"id"},
			deferrable:        true,
			initiallyDeferred: true,
			expected:          `CONSTRAINT "test" FOREIGN KEY ("other_id") REFERENCES "other_table" ("id") MATCH SIMPLE ON DELETE NO ACTION ON UPDATE NO ACTION DEFERRABLE INITIALLY DEFERRED`,
		},
		"composite foreign key": {
			name:              "test",
			columns:           []string{"other_id_1", "other_id_2"},
			referencedTable:   "other_table",
			referencedColumns: []string{"id_1", "id_2"},
			expected:          `CONSTRAINT "test" FOREIGN KEY ("other_id_1", "other_id_2") REFERENCES "other_table" ("id_1", "id_2") MATCH SIMPLE ON DELETE NO ACTION ON UPDATE NO ACTION`,
		},
		"composite foreign key on delete set columns": {
			name:               "test",
			columns:            []string{"other_id_1", "other_id_2"},
			referencedTable:    "other_table",
			referencedColumns:  []string{"id_1", "id_2"},
			onDelete:           migrations.ForeignKeyActionSETNULL,
			onDeleteSetColumns: []string{"other_id_1"},
			expected:           `CONSTRAINT "test" FOREIGN KEY ("other_id_1", "other_id_2") REFERENCES "other_table" ("id_1", "id_2") MATCH SIMPLE ON DELETE SET NULL ("other_id_1") ON UPDATE NO ACTION`,
		},
		"composite foreign key not validated": {
			name:              "test",
			columns:           []string{"other_id_1", "other_id_2"},
			referencedTable:   "other_table",
			referencedColumns: []string{"id_1", "id_2"},
			skipValidation:    true,
			expected:          `CONSTRAINT "test" FOREIGN KEY ("other_id_1", "other_id_2") REFERENCES "other_table" ("id_1", "id_2") MATCH SIMPLE ON DELETE NO ACTION ON UPDATE NO ACTION NOT VALID`,
		},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			writer := &migrations.ConstraintSQLWriter{
				Name:              tc.name,
				Columns:           tc.columns,
				Deferrable:        tc.deferrable,
				InitiallyDeferred: tc.initiallyDeferred,
				SkipValidation:    tc.skipValidation,
			}

			constraint := writer.WriteForeignKey(
				tc.referencedTable,
				tc.referencedColumns,
				tc.onDelete,
				tc.onUpdate,
				tc.onDeleteSetColumns,
				tc.matchType,
			)
			assert.Equal(t, tc.expected, constraint)
		})
	}
}

func TestConstraintExclude(t *testing.T) {
	tests := map[string]struct {
		name        string
		indexMethod string
		elements    string
		predicate   string

		includeColumns    []string
		deferrable        bool
		initiallyDeferred bool
		storageParameters string
		tablespace        string
		expected          string
	}{
		"single column unique": {
			name:        "test",
			indexMethod: "btree",
			elements:    "column WITH =",
			expected:    `CONSTRAINT "test" EXCLUDE USING btree (column WITH =)`,
		},
		"single column unique with partial index": {
			name:        "test",
			indexMethod: "btree",
			elements:    "column WITH =",
			predicate:   "column > 10",
			expected:    `CONSTRAINT "test" EXCLUDE USING btree (column WITH =) WHERE (column > 10)`,
		},
		"single column with storage options": {
			name:              "test",
			indexMethod:       "btree",
			elements:          "column WITH =",
			storageParameters: "fillfactor=70",
			expected:          `CONSTRAINT "test" EXCLUDE USING btree (column WITH =) WITH (fillfactor=70)`,
		},
		"single column with tablespace": {
			name:        "test",
			indexMethod: "btree",
			elements:    "column WITH =",
			tablespace:  "test_tablespace",
			expected:    `CONSTRAINT "test" EXCLUDE USING btree (column WITH =) USING INDEX TABLESPACE test_tablespace`,
		},
		"single column with storage options and tablespace": {
			name:              "test",
			indexMethod:       "btree",
			elements:          "column WITH =",
			storageParameters: "fillfactor=70",
			tablespace:        "test_tablespace",
			expected:          `CONSTRAINT "test" EXCLUDE USING btree (column WITH =) WITH (fillfactor=70) USING INDEX TABLESPACE test_tablespace`,
		},
		"single column with partial index and storage options and tablespace": {
			name:              "test",
			indexMethod:       "btree",
			elements:          "column WITH =",
			predicate:         "column > 5",
			storageParameters: "fillfactor=70",
			tablespace:        "test_tablespace",
			expected:          `CONSTRAINT "test" EXCLUDE USING btree (column WITH =) WITH (fillfactor=70) USING INDEX TABLESPACE test_tablespace WHERE (column > 5)`,
		},
		"single column with include columns": {
			name:           "test",
			indexMethod:    "btree",
			elements:       "column WITH =",
			includeColumns: []string{"include_column", "other_include_column"},
			expected:       `CONSTRAINT "test" EXCLUDE USING btree (column WITH =) INCLUDE ("include_column", "other_include_column")`,
		},
		"single column with include columns deferred": {
			name:           "test",
			indexMethod:    "btree",
			elements:       "column WITH =",
			deferrable:     true,
			includeColumns: []string{"include_column", "other_include_column"},
			expected:       `CONSTRAINT "test" EXCLUDE USING btree (column WITH =) INCLUDE ("include_column", "other_include_column") DEFERRABLE INITIALLY IMMEDIATE`,
		},
		"single column with include columns deferred initially deferred": {
			name:              "test",
			indexMethod:       "btree",
			elements:          "column WITH =",
			deferrable:        true,
			initiallyDeferred: true,
			includeColumns:    []string{"include_column", "other_include_column"},
			expected:          `CONSTRAINT "test" EXCLUDE USING btree (column WITH =) INCLUDE ("include_column", "other_include_column") DEFERRABLE INITIALLY DEFERRED`,
		},
		"multi column with storage options and include columns": {
			name:              "test",
			indexMethod:       "gist",
			elements:          "col1 WITH &&, col2 WITH =",
			storageParameters: "fillfactor=70",
			includeColumns:    []string{"include_column", "other_include_column"},
			expected:          `CONSTRAINT "test" EXCLUDE USING gist (col1 WITH &&, col2 WITH =) INCLUDE ("include_column", "other_include_column") WITH (fillfactor=70)`,
		},
		"multi column without name": {
			indexMethod: "gist",
			elements:    "col1 WITH &&, col2 WITH =",
			expected:    `EXCLUDE USING gist (col1 WITH &&, col2 WITH =)`,
		},
		"multi column without name deferrable": {
			indexMethod: "gist",
			elements:    "col1 WITH &&, col2 WITH =",
			deferrable:  true,
			expected:    `EXCLUDE USING gist (col1 WITH &&, col2 WITH =) DEFERRABLE INITIALLY IMMEDIATE`,
		},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			writer := &migrations.ConstraintSQLWriter{
				Name:              tc.name,
				Deferrable:        tc.deferrable,
				InitiallyDeferred: tc.initiallyDeferred,
				IncludeColumns:    tc.includeColumns,
				StorageParameters: tc.storageParameters,
				Tablespace:        tc.tablespace,
			}

			constraint := writer.WriteExclude(tc.indexMethod, tc.elements, tc.predicate)
			assert.Equal(t, tc.expected, constraint)
		})
	}
}
