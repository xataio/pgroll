package migrations_test

import (
	"context"
	"database/sql"
	"fmt"
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/lib/pq"
	"golang.org/x/exp/slices"

	"pg-roll/pkg/migrations"
	"pg-roll/pkg/roll"
)

func TestNewColumnIsUsableAfterMigrationStart(t *testing.T) {
	t.Parallel()

	withMigratorAndConnectionToContainer(t, func(mig *roll.Roll, db *sql.DB) {
		ctx := context.Background()

		if err := mig.Start(ctx, addTableMigration()); err != nil {
			t.Fatalf("Failed to start add table migration: %v", err)
		}
		if err := mig.Complete(ctx); err != nil {
			t.Fatalf("Failed to complete add table migration: %v", err)
		}

		if err := mig.Start(ctx, addColumnMigration()); err != nil {
			t.Fatalf("Failed to start add column migration: %v", err)
		}

		versionSchema := roll.VersionedSchemaName(schema, "2_add_column")
		insertAndSelectRowsFromUsersTable(t, db, versionSchema, "users")
	})
}

func TestNewColumnIsUsableAfterMigrationComplete(t *testing.T) {
	t.Parallel()

	withMigratorAndConnectionToContainer(t, func(mig *roll.Roll, db *sql.DB) {
		ctx := context.Background()

		if err := mig.Start(ctx, addTableMigration()); err != nil {
			t.Fatalf("Failed to start add table migration: %v", err)
		}
		if err := mig.Complete(ctx); err != nil {
			t.Fatalf("Failed to complete add table migration: %v", err)
		}

		if err := mig.Start(ctx, addColumnMigration()); err != nil {
			t.Fatalf("Failed to start add column migration: %v", err)
		}
		if err := mig.Complete(ctx); err != nil {
			t.Fatalf("Failed to complete add column migration: %v", err)
		}

		versionSchema := roll.VersionedSchemaName(schema, "2_add_column")
		insertAndSelectRowsFromUsersTable(t, db, versionSchema, "users")
	})
}

func TestNewColumnIsRemovedAfterMigrationRollback(t *testing.T) {
	t.Parallel()

	withMigratorAndConnectionToContainer(t, func(mig *roll.Roll, db *sql.DB) {
		ctx := context.Background()

		if err := mig.Start(ctx, addTableMigration()); err != nil {
			t.Fatalf("Failed to start add table migration: %v", err)
		}
		if err := mig.Complete(ctx); err != nil {
			t.Fatalf("Failed to complete add table migration: %v", err)
		}

		if err := mig.Start(ctx, addColumnMigration()); err != nil {
			t.Fatalf("Failed to start add column migration: %v", err)
		}
		if err := mig.Rollback(ctx); err != nil {
			t.Fatalf("Failed to roll back add column migration: %v", err)
		}

		var exists bool
		tempColumnName := migrations.TemporaryName("age")
		err := db.QueryRow(`
    SELECT EXISTS 
      (SELECT 1
        FROM information_schema.columns
        WHERE table_schema = $1 
        AND table_name = $2 
        AND column_name = $3
    )`, "public", "users", tempColumnName).Scan(&exists)
		if err != nil {
			t.Fatal(err)
		}

		if exists {
			t.Errorf("Expected column %q to not exist on table %q", tempColumnName, "users")
		}
	})
}

func insertAndSelectRowsFromUsersTable(t *testing.T, db *sql.DB, schemaName, viewName string) {
	//
	// Insert records via the view
	//
	sql := fmt.Sprintf(`INSERT INTO %s.%s (id, name, age) VALUES ($1, $2, $3)`,
		pq.QuoteIdentifier(schemaName),
		pq.QuoteIdentifier(viewName))

	insertStmt, err := db.Prepare(sql)
	if err != nil {
		t.Fatal(err)
	}
	defer insertStmt.Close()

	type user struct {
		ID   int
		Name string
		Age  int
	}
	inserted := []user{{ID: 1, Name: "Alice", Age: 20}, {ID: 2, Name: "Bob", Age: 30}}

	for _, v := range inserted {
		_, err = insertStmt.Exec(v.ID, v.Name, v.Age)
		if err != nil {
			t.Fatal(err)
		}
	}

	//
	// Read the records back via the view
	//
	sql = fmt.Sprintf(`SELECT id, name, age FROM %q.%q`, schemaName, viewName)
	rows, err := db.Query(sql)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()

	var retrieved []user
	for rows.Next() {
		var user user
		if err := rows.Scan(&user.ID, &user.Name, &user.Age); err != nil {
			t.Fatal(err)
		}
		retrieved = append(retrieved, user)
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}

	if !slices.Equal(inserted, retrieved) {
		t.Error("Inserted and retrieved rows do not match")
		t.Error(cmp.Diff(inserted, retrieved))
	}
}

func addTableMigration() *migrations.Migration {
	return &migrations.Migration{
		Name: "1_add_table",
		Operations: migrations.Operations{
			&migrations.OpCreateTable{
				Name: "users",
				Columns: []migrations.Column{
					{
						Name:       "id",
						Type:       "integer",
						PrimaryKey: true,
					},
					{
						Name:   "name",
						Type:   "varchar(255)",
						Unique: true,
					},
				},
			},
		},
	}
}

func addColumnMigration() *migrations.Migration {
	return &migrations.Migration{
		Name: "2_add_column",
		Operations: migrations.Operations{
			&migrations.OpAddColumn{
				Table: "users",
				Column: migrations.Column{
					Name:     "age",
					Type:     "integer",
					Nullable: true,
				},
			},
		},
	}
}
