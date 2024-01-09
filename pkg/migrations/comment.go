// SPDX-License-Identifier: Apache-2.0

package migrations

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/lib/pq"
)

func addCommentToColumn(ctx context.Context, conn *sql.DB, tableName, columnName, comment string) error {
	_, err := conn.ExecContext(ctx, fmt.Sprintf(`COMMENT ON COLUMN %s.%s IS %s`,
		pq.QuoteIdentifier(tableName),
		pq.QuoteIdentifier(columnName),
		pq.QuoteLiteral(comment)))

	return err
}

func addCommentToTable(ctx context.Context, conn *sql.DB, tableName, comment string) error {
	_, err := conn.ExecContext(ctx, fmt.Sprintf(`COMMENT ON TABLE %s IS %s`,
		pq.QuoteIdentifier(tableName),
		pq.QuoteLiteral(comment)))

	return err
}
