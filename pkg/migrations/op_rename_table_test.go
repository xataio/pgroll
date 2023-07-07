package migrations_test

import (
	"context"
	"database/sql"
	"testing"

	"pg-roll/pkg/migrations"
	"pg-roll/pkg/roll"
)

func TestRenameTable(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name           string
		migrations     []migrations.Migration
		beforeComplete func(t *testing.T, db *sql.DB)
		afterComplete  func(t *testing.T, db *sql.DB)
	}{
		{
			name: "rename table",
			migrations: []migrations.Migration{
				{
					Name: "01_create_table",
					Operations: migrations.Operations{
						&migrations.OpCreateTable{
							Name: "test_table",
							Columns: []migrations.Column{
								{
									Name: "id",
									Type: "serial",
								},
							},
						},
					},
				},
				{
					Name: "02_rename_table",
					Operations: migrations.Operations{
						&migrations.OpRenameTable{
							From: "test_table",
							To:   "renamed_table",
						},
					},
				},
			},
			beforeComplete: func(t *testing.T, db *sql.DB) {
				// check that the table with the new name can be accessed
				ViewMustExist(t, db, "public", "01_create_table", "test_table")
				ViewMustExist(t, db, "public", "02_rename_table", "renamed_table")
			},
			afterComplete: func(t *testing.T, db *sql.DB) {
				ViewMustExist(t, db, "public", "02_rename_table", "renamed_table")
			},
		},
	}

	for _, tt := range tests {
		withMigratorAndConnectionToContainer(t, func(mig *roll.Roll, db *sql.DB) {
			ctx := context.Background()

			// run all migrations except the last one
			for i := 0; i < len(tt.migrations)-1; i++ {
				if err := mig.Start(ctx, &tt.migrations[i]); err != nil {
					t.Fatalf("Failed to start migration: %v", err)
				}

				if err := mig.Complete(ctx); err != nil {
					t.Fatalf("Failed to complete migration: %v", err)
				}
			}

			// start the last migration
			if err := mig.Start(ctx, &tt.migrations[len(tt.migrations)-1]); err != nil {
				t.Fatalf("Failed to start migration: %v", err)
			}

			// run the beforeComplete hook
			if tt.beforeComplete != nil {
				tt.beforeComplete(t, db)
			}

			// complete the last migration
			if err := mig.Complete(ctx); err != nil {
				t.Fatalf("Failed to complete migration: %v", err)
			}

			// run the afterComplete hook
			if tt.afterComplete != nil {
				tt.afterComplete(t, db)
			}
		})
	}
}
