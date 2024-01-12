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

	exitCode := m.Run()

	if err := ctr.Terminate(ctx); err != nil {
		log.Printf("Failed to terminate container: %v", err)
	}

	os.Exit(exitCode)
}

func WithStateAndConnectionToContainer(t *testing.T, fn func(*state.State, *sql.DB)) {
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

	st, err := state.New(ctx, connStr, "pgroll")
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

	fn(st, db)
}

func WithMigratorInSchemaWithLockTimeoutAndConnectionToContainer(t *testing.T, schema string, lockTimeoutMs int, fn func(mig *roll.Roll, db *sql.DB)) {
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

	st, err := state.New(ctx, connStr, "pgroll")
	if err != nil {
		t.Fatal(err)
	}

	err = st.Init(ctx)
	if err != nil {
		t.Fatal(err)
	}

	mig, err := roll.New(ctx, connStr, schema, st, roll.WithLockTimeoutMs(lockTimeoutMs))
	if err != nil {
		t.Fatal(err)
	}

	t.Cleanup(func() {
		if err := mig.Close(); err != nil {
			t.Fatalf("Failed to close migrator connection: %v", err)
		}
	})

	db, err := sql.Open("postgres", connStr)
	if err != nil {
		t.Fatal(err)
	}

	_, err = db.ExecContext(ctx, fmt.Sprintf("CREATE SCHEMA IF NOT EXISTS %s", schema))
	if err != nil {
		t.Fatal(err)
	}

	t.Cleanup(func() {
		if err := db.Close(); err != nil {
			t.Fatalf("Failed to close database connection: %v", err)
		}
	})

	fn(mig, db)
}

func WithMigratorInSchemaAndConnectionToContainer(t *testing.T, schema string, fn func(mig *roll.Roll, db *sql.DB)) {
	WithMigratorInSchemaWithLockTimeoutAndConnectionToContainer(t, schema, 500, fn)
}

func WithMigratorAndConnectionToContainer(t *testing.T, fn func(mig *roll.Roll, db *sql.DB)) {
	WithMigratorInSchemaWithLockTimeoutAndConnectionToContainer(t, "public", 500, fn)
}
