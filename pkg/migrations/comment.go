// SPDX-License-Identifier: Apache-2.0

package migrations

import (
	"context"
	"fmt"

	"github.com/lib/pq"
	"github.com/xataio/pgroll/pkg/db"
)

func addCommentToColumn(ctx context.Context, conn db.DB, tableName, columnName string, comment *string) error {
	_, err := conn.ExecContext(ctx, fmt.Sprintf(`COMMENT ON COLUMN %s.%s IS %s`,
		pq.QuoteIdentifier(tableName),
		pq.QuoteIdentifier(columnName),
		commentToSQL(comment)))

	return err
}

func addCommentToTable(ctx context.Context, conn db.DB, tableName string, comment *string) error {
	_, err := conn.ExecContext(ctx, fmt.Sprintf(`COMMENT ON TABLE %s IS %s`,
		pq.QuoteIdentifier(tableName),
		commentToSQL(comment)))

	return err
}

func commentToSQL(comment *string) string {
	if comment == nil {
		return "NULL"
	}

	return pq.QuoteLiteral(*comment)
}
