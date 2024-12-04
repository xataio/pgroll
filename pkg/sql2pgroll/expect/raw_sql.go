// SPDX-License-Identifier: Apache-2.0

package expect

import "github.com/xataio/pgroll/pkg/migrations"

func RawSQLOp(sql string) *migrations.OpRawSQL {
	return &migrations.OpRawSQL{Up: sql}
}
