// SPDX-License-Identifier: Apache-2.0

package testutils

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"net/url"
	"os"
	"testing"
	"time"

	"github.com/lib/pq"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/postgres"
	"github.com/testcontainers/testcontainers-go/wait"
	"github.com/xataio/pgroll/pkg/roll"
	"github.com/xataio/pgroll/pkg/state"
)

// The version of postgres against which the tests are run
// if the POSTGRES_VERSION environment variable is not set.
const defaultPostgresVersion = "15.3"

// tConnStr holds the connection string to the test container created in TestMain.
var tConnStr string

// SharedTestMain starts a postgres container to be used by all tests in a package.
// Each test then connects to the container and creates a new database.
func SharedTestMain(m *testing.M) {
	ctx := context.Background()

	waitForLogs := wait.
		ForLog("database system is ready to accept connections").
		WithOccurrence(2).
		WithStartupTimeout(5 * time.Second)

	pgVersion := os.Getenv("POSTGRES_VERSION")
	if pgVersion == "" {
		pgVersion = defaultPostgresVersion
	}

	ctr, err := postgres.RunContainer(ctx,
		testcontainers.WithImage("postgres:"+pgVersion),
		testcontainers.WithWaitStrategy(waitForLogs),
	)
	if err != nil {
		os.Exit(1)
	}

	tConnStr, err = ctr.ConnectionString(ctx, "sslmode=disable")
	if err != nil {
		os.Exit(1)
	}

	db, err := sql.Open("postgres", tConnStr)
	if err != nil {
		os.Exit(1)
	}

	// create handy role for tests
	_, err = db.ExecContext(ctx, "CREATE ROLE pgroll")
	if err != nil {
		os.Exit(1)
	}

	exitCode := m.Run()

	if err := ctr.Terminate(ctx); err != nil {
		log.Printf("Failed to terminate container: %v", err)
	}

	os.Exit(exitCode)
}

// TestSchema returns the schema in which migration tests apply migrations. By
// default, migrations will be applied to the "public" schema.
func TestSchema() string {
	testSchema := os.Getenv("PGROLL_TEST_SCHEMA")
	if testSchema != "" {
		return testSchema
	}
	return "public"
}

func WithStateInSchemaAndConnectionToContainer(t *testing.T, schema string, fn func(*state.State, *sql.DB)) {
	t.Helper()
	ctx := context.Background()

	tDB, err := sql.Open("postgres", tConnStr)
	if err != nil {
		t.Fatal(err)
	}

	t.Cleanup(func() {
		if err := tDB.Close(); err != nil {
			t.Fatalf("Failed to close database connection: %v", err)
		}
	})

	dbName := randomDBName()

	_, err = tDB.ExecContext(ctx, fmt.Sprintf("CREATE DATABASE %s", pq.QuoteIdentifier(dbName)))
	if err != nil {
		t.Fatal(err)
	}

	u, err := url.Parse(tConnStr)
	if err != nil {
		t.Fatal(err)
	}

	u.Path = "/" + dbName
	connStr := u.String()

	st, err := state.New(ctx, connStr, schema)
	if err != nil {
		t.Fatal(err)
	}

	db, err := sql.Open("postgres", connStr)
	if err != nil {
		t.Fatal(err)
	}

	t.Cleanup(func() {
		if err := db.Close(); err != nil {
			t.Fatalf("Failed to close database connection: %v", err)
		}
	})

	// init the state
	if err := st.Init(ctx); err != nil {
		t.Fatal(err)
	}

	fn(st, db)
}

func WithStateAndConnectionToContainer(t *testing.T, fn func(*state.State, *sql.DB)) {
	WithStateInSchemaAndConnectionToContainer(t, "pgroll", fn)
}

func WithMigratorInSchemaAndConnectionToContainerWithOptions(t *testing.T, schema string, opts []roll.Option, fn func(mig *roll.Roll, db *sql.DB)) {
	t.Helper()
	ctx := context.Background()

	tDB, err := sql.Open("postgres", tConnStr)
	if err != nil {
		t.Fatal(err)
	}

	t.Cleanup(func() {
		if err := tDB.Close(); err != nil {
			t.Fatalf("Failed to close database connection: %v", err)
		}
	})

	dbName := randomDBName()

	_, err = tDB.ExecContext(ctx, fmt.Sprintf("CREATE DATABASE %s", pq.QuoteIdentifier(dbName)))
	if err != nil {
		t.Fatal(err)
	}

	u, err := url.Parse(tConnStr)
	if err != nil {
		t.Fatal(err)
	}

	u.Path = "/" + dbName
	connStr := u.String()

	db, err := sql.Open("postgres", connStr)
	if err != nil {
		t.Fatal(err)
	}

	t.Cleanup(func() {
		if err := db.Close(); err != nil {
			t.Fatalf("Failed to close database connection: %v", err)
		}
	})

	st, err := state.New(ctx, connStr, "pgroll")
	if err != nil {
		t.Fatal(err)
	}

	err = st.Init(ctx)
	if err != nil {
		t.Fatal(err)
	}

	mig, err := roll.New(ctx, connStr, schema, st, opts...)
	if err != nil {
		t.Fatal(err)
	}

	t.Cleanup(func() {
		if err := mig.Close(); err != nil {
			t.Fatalf("Failed to close migrator connection: %v", err)
		}
	})

	_, err = db.ExecContext(ctx, fmt.Sprintf("CREATE SCHEMA IF NOT EXISTS %s", schema))
	if err != nil {
		t.Fatal(err)
	}

	_, err = db.ExecContext(ctx, fmt.Sprintf("GRANT ALL PRIVILEGES ON SCHEMA %s TO pgroll", schema))
	if err != nil {
		t.Fatal(err)
	}

	_, err = db.ExecContext(ctx, fmt.Sprintf("GRANT ALL PRIVILEGES ON DATABASE %s TO pgroll", dbName))
	if err != nil {
		t.Fatal(err)
	}

	fn(mig, db)
}

func WithMigratorInSchemaAndConnectionToContainer(t *testing.T, schema string, fn func(mig *roll.Roll, db *sql.DB)) {
	WithMigratorInSchemaAndConnectionToContainerWithOptions(t, schema, []roll.Option{roll.WithLockTimeoutMs(500)}, fn)
}

func WithMigratorAndConnectionToContainer(t *testing.T, fn func(mig *roll.Roll, db *sql.DB)) {
	WithMigratorInSchemaAndConnectionToContainerWithOptions(t, "public", []roll.Option{roll.WithLockTimeoutMs(500)}, fn)
}
