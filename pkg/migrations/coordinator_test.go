// SPDX-License-Identifier: Apache-2.0

package migrations

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestCoordinator(t *testing.T) {
	type testCase map[string]struct {
		actions       []DBAction
		expectedOrder []string
	}

	testCases := testCase{
		"empty": {
			actions:       []DBAction{},
			expectedOrder: []string{},
		},
		"single action": {
			actions: []DBAction{
				NewRenameDuplicatedColumnAction(nil, nil, "column1"),
			},
			expectedOrder: []string{"rename_duplicated_column1"},
		},
		"multiple actions with duplicates": {
			actions: []DBAction{
				NewRenameDuplicatedColumnAction(nil, nil, "column1"),
				NewRenameDuplicatedColumnAction(nil, nil, "column2"),
				NewRenameDuplicatedColumnAction(nil, nil, "column1"), // Duplicate
			},
			expectedOrder: []string{"rename_duplicated_column2", "rename_duplicated_column1"},
		},
		"multiple actions with mutiple duplicated for renaming": {
			actions: []DBAction{
				NewDropColumnAction(nil, "column1"),
				NewRenameDuplicatedColumnAction(nil, nil, "column1"),
				NewDropColumnAction(nil, "column2"),
				NewRenameDuplicatedColumnAction(nil, nil, "column2"),
				NewDropColumnAction(nil, "column3"),
				NewRenameDuplicatedColumnAction(nil, nil, "column3"),
				NewDropColumnAction(nil, "column1"),
				NewRenameDuplicatedColumnAction(nil, nil, "column1"),
				NewDropColumnAction(nil, "column2"),
				NewRenameDuplicatedColumnAction(nil, nil, "column2"),
			},
			expectedOrder: []string{
				"drop_column_column3_",
				"rename_duplicated_column3",
				"drop_column_column1_",
				"rename_duplicated_column1",
				"drop_column_column2_",
				"rename_duplicated_column2",
			},
		},
		"add same column multiple times to same column": {
			actions: []DBAction{
				NewCreateTableAction(nil, "test_table", "", ""),
				NewAddColumnAction(nil, "column1", Column{}, false),
				NewAddColumnAction(nil, "column1", Column{}, false), // Duplicate
			},
			expectedOrder: []string{"create_table_test_table", "add_column_column1_"},
		},
		"create table multiple time with different statement with the same name": {
			actions: []DBAction{
				NewCreateTableAction(nil, "test_table", "", ""),
				NewCreateTableAction(nil, "test_table", "CREATE TABLE test_table (id INT)", ""),
				NewCommentTableAction(nil, "test_table", ptr("This is a test table")),
				NewCommentColumnAction(nil, "test_table", "id", ptr("This is a test column")),
			},
			expectedOrder: []string{"create_table_test_table", "comment_table_test_table", "comment_column_test_table_id"},
		},
		"drop default value on column": {
			actions: []DBAction{
				NewCreateTableAction(nil, "test_table", "", ""),
				NewAddColumnAction(nil, "column1", Column{Default: ptr("default_value")}, false),
				NewDropDefaultValueAction(nil, "test_table", "column1"),
			},
			expectedOrder: []string{"create_table_test_table", "add_column_column1_", "drop_default_test_table_column1"},
		},
		"create table and add multiple constraints": {
			actions: []DBAction{
				NewCreateTableAction(nil, "test_table", "", ""),
				NewAddColumnAction(nil, "column1", Column{}, false),
				NewAddColumnAction(nil, "column2", Column{}, false),
				NewCreateUniqueIndexConcurrentlyAction(nil, "public", "test_table", "my_idx", "column1"),
				NewCreateFKConstraintAction(nil, "test_table", "column2", []string{"other_column"}, nil, false, false, false),
				NewCreateCheckConstraintAction(nil, "test_table", "my_check", "column1 > 0", []string{"column1"}, false, false),
			},
			expectedOrder: []string{
				"create_table_test_table",
				"add_column_column1_",
				"add_column_column2_",
				"create_unique_index_concurrently_test_table_my_idx",
				"create_fk_constraint_test_table_column2",
				"create_check_constraint_test_table_my_check",
			},
		},
	}

	for name, tc := range testCases {
		t.Run(name, func(t *testing.T) {
			coordinator := NewCoordinator(tc.actions)
			if len(coordinator.order) != len(tc.expectedOrder) {
				t.Fatalf("expected order length %d, got %d", len(tc.expectedOrder), len(coordinator.order))
			}

			require.Equal(t, tc.expectedOrder, coordinator.order, "order of actions does not match expected")
		})
	}
}
