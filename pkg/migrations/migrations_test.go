// SPDX-License-Identifier: Apache-2.0

package migrations_test

import (
	"context"
	"testing"
	"testing/fstest"

	"github.com/stretchr/testify/assert"
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
		"find all sql files": {
			dir: fstest.MapFS{
				"01_migration_1.up.sql":   &fstest.MapFile{},
				"01_migration_1.down.sql": &fstest.MapFile{},
				"02_migration_1.up.sql":   &fstest.MapFile{},
			},
			expectedFiles: []string{"01_migration_1.up.sql", "02_migration_1.up.sql"},
		},
		"find all files": {
			dir: fstest.MapFS{
				"01_migration_1.json":     &fstest.MapFile{},
				"03_migration_3.yaml":     &fstest.MapFile{},
				"02_migration_2.yml":      &fstest.MapFile{},
				"04_migration_4.up.sql":   &fstest.MapFile{},
				"04_migration_4.down.sql": &fstest.MapFile{},
			},
			expectedFiles: []string{"01_migration_1.json", "02_migration_2.yml", "03_migration_3.yaml", "04_migration_4.up.sql"},
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

func TestReadSQLMigration(t *testing.T) {
	tests := map[string]struct {
		dir               fstest.MapFS
		filename          string
		expectedMigration *migrations.Migration
	}{
		"read up migration": {
			dir: fstest.MapFS{
				"01_migration_1.up.sql": &fstest.MapFile{Data: []byte("CREATE TABLE test_table(name text)")},
			},
			filename: "01_migration_1.up.sql",
			expectedMigration: &migrations.Migration{
				Name: "01_migration_1",
				Operations: migrations.Operations{
					&migrations.OpSQLInTransaction{
						Up: "CREATE TABLE test_table(name text)",
					},
				},
			},
		},
		"read up and down migration": {
			dir: fstest.MapFS{
				"01_migration_1.up.sql":   &fstest.MapFile{Data: []byte("CREATE TABLE test_table(name text)")},
				"01_migration_1.down.sql": &fstest.MapFile{Data: []byte("DROP TABLE test_table")},
			},
			filename: "01_migration_1.up.sql",
			expectedMigration: &migrations.Migration{
				Name: "01_migration_1",
				Operations: migrations.Operations{
					&migrations.OpSQLInTransaction{
						Up:   "CREATE TABLE test_table(name text)",
						Down: "DROP TABLE test_table",
					},
				},
			},
		},
	}

	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			migration, err := migrations.ReadMigration(test.dir, test.filename)
			assert.NoError(t, err)
			assert.Equal(t, test.expectedMigration, migration)
		})
	}
}
