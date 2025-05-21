// SPDX-License-Identifier: Apache-2.0

package migrations_test

import (
	"context"
	"encoding/json"
	"testing"
	"testing/fstest"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/xataio/pgroll/pkg/migrations"
	"github.com/xataio/pgroll/pkg/schema"
)

func TestMigrationsIsolated(t *testing.T) {
	t.Parallel()

	migration := migrations.Migration{
		Name: "sql",
		Operations: migrations.Operations{
			&migrations.OpRawSQL{
				Up: `foo`,
			},
			&migrations.OpCreateTable{Name: "foo"},
		},
	}

	err := migration.Validate(context.TODO(), schema.New())
	var wantErr migrations.InvalidMigrationError
	assert.ErrorAs(t, err, &wantErr)
}

func TestMigrationsIsolatedValid(t *testing.T) {
	t.Parallel()

	migration := migrations.Migration{
		Name: "sql",
		Operations: migrations.Operations{
			&migrations.OpRawSQL{
				Up: `foo`,
			},
		},
	}
	err := migration.Validate(context.TODO(), schema.New())
	assert.NoError(t, err)
}

func TestOnCompleteSQLMigrationsAreNotIsolated(t *testing.T) {
	t.Parallel()

	migration := migrations.Migration{
		Name: "sql",
		Operations: migrations.Operations{
			&migrations.OpRawSQL{
				Up:         `foo`,
				OnComplete: true,
			},
			&migrations.OpCreateTable{Name: "foo"},
		},
	}
	err := migration.Validate(context.TODO(), schema.New())
	assert.NoError(t, err)
}

func TestCollectFilesFromDir(t *testing.T) {
	t.Parallel()

	tests := map[string]struct {
		dir           fstest.MapFS
		expectedFiles []string
	}{
		"find all json files": {
			dir: fstest.MapFS{
				"01_migration_1.json": &fstest.MapFile{},
				"03_migration_3.json": &fstest.MapFile{},
				"02_migration_2.json": &fstest.MapFile{},
			},
			expectedFiles: []string{"01_migration_1.json", "02_migration_2.json", "03_migration_3.json"},
		},
		"find all yaml and yml files": {
			dir: fstest.MapFS{
				"01_migration_1.yaml": &fstest.MapFile{},
				"03_migration_3.yaml": &fstest.MapFile{},
				"02_migration_2.yml":  &fstest.MapFile{},
			},
			expectedFiles: []string{"01_migration_1.yaml", "02_migration_2.yml", "03_migration_3.yaml"},
		},
		"find all files": {
			dir: fstest.MapFS{
				"01_migration_1.json": &fstest.MapFile{},
				"03_migration_3.yaml": &fstest.MapFile{},
				"02_migration_2.yml":  &fstest.MapFile{},
			},
			expectedFiles: []string{"01_migration_1.json", "02_migration_2.yml", "03_migration_3.yaml"},
		},
	}

	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			files, err := migrations.CollectFilesFromDir(test.dir)
			assert.NoError(t, err)
			assert.Equal(t, test.expectedFiles, files)
		})
	}
}

func TestMigrationNamesAreSetCorrectlyWhenReadingFromFile(t *testing.T) {
	t.Parallel()

	// Set up a directory with multiple migration files
	dir := fstest.MapFS{
		"01_migration_1.json": &fstest.MapFile{Data: exampleMigration(t)},
		"02_migration_2.yml":  &fstest.MapFile{Data: exampleMigration(t)},
		"03.migration.3.yml":  &fstest.MapFile{Data: exampleMigration(t)},
	}

	testcases := []struct {
		FileName              string
		ExpectedMigrationName string
	}{
		{
			FileName:              "01_migration_1.json",
			ExpectedMigrationName: "01_migration_1",
		},
		{
			FileName:              "02_migration_2.yml",
			ExpectedMigrationName: "02_migration_2",
		},
		{
			FileName:              "03.migration.3.yml",
			ExpectedMigrationName: "03.migration.3",
		},
	}

	for _, tc := range testcases {
		t.Run(tc.FileName, func(t *testing.T) {
			mig, err := migrations.ReadMigration(dir, tc.FileName)
			require.NoError(t, err)

			// Ensure that the migration name is set correctly from the filename
			assert.Equal(t, tc.ExpectedMigrationName, mig.Name)
		})
	}
}

func TestAllNonDeprecatedOperationsAreCreateable(t *testing.T) {
	for _, opName := range migrations.AllNonDeprecatedOperations {
		t.Run(opName, func(t *testing.T) {
			op, err := migrations.OperationFromName(migrations.OpName(opName))
			assert.NoError(t, err)
			_, ok := op.(migrations.Createable)
			assert.True(t, ok, "operation %q must have a Create function", opName)
		})
	}
}

func exampleMigration(t *testing.T) []byte {
	t.Helper()

	mig := &migrations.Migration{
		Operations: migrations.Operations{
			&migrations.OpRawSQL{Up: "SELECT 1"},
		},
	}

	bytes, err := json.Marshal(mig)
	require.NoError(t, err)

	return bytes
}
