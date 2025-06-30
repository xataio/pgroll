// SPDX-License-Identifier: Apache-2.0

package connstr

import (
	"fmt"
	"net/url"
	"strings"
)

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
