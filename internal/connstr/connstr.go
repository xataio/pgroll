// SPDX-License-Identifier: Apache-2.0

package connstr

import (
	"fmt"
	"net"
	"net/url"
	"os"
	"strings"
)

// DefaultURL builds the connection string used when no --postgres-url flag (or
// PGROLL_PG_URL environment variable) is provided. It starts from pgroll's
// built-in local defaults and lets the standard libpq environment variables
// override individual fields when they are set. With none of them set it
// returns the historical default of a local Postgres instance.
//
// The following libpq environment variables are respected: PGHOST, PGPORT,
// PGUSER, PGPASSWORD, PGDATABASE and PGSSLMODE.
func DefaultURL() string {
	host := "localhost"
	if v := os.Getenv("PGHOST"); v != "" {
		host = v
	}
	if port := os.Getenv("PGPORT"); port != "" {
		host = net.JoinHostPort(host, port)
	}

	u := &url.URL{
		Scheme: "postgres",
		User:   url.UserPassword(getenvOr("PGUSER", "postgres"), getenvOr("PGPASSWORD", "postgres")),
		Host:   host,
	}

	if db := os.Getenv("PGDATABASE"); db != "" {
		u.Path = "/" + db
	}

	q := url.Values{}
	q.Set("sslmode", getenvOr("PGSSLMODE", "disable"))
	u.RawQuery = q.Encode()

	return u.String()
}

func getenvOr(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

// AppendSearchPathOption take a Postgres connection string in URL format and
// produces the same connection string with the search_path option set to the
// provided schema.
func AppendSearchPathOption(connStr, schema string) (string, error) {
	u, err := url.Parse(connStr)
	if err != nil {
		return "", fmt.Errorf("failed to parse connection string: %w", err)
	}

	if schema == "" {
		return connStr, nil
	}

	q := u.Query()
	q.Set("options", fmt.Sprintf("-c search_path=%s", schema))
	encodedQuery := q.Encode()

	// Replace '+' with '%20' to ensure proper encoding of spaces within the
	// `options` query parameter.
	encodedQuery = strings.ReplaceAll(encodedQuery, "+", "%20")

	u.RawQuery = encodedQuery

	return u.String(), nil
}
