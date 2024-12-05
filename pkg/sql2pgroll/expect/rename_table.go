// SPDX-License-Identifier: Apache-2.0

package expect

import "github.com/xataio/pgroll/pkg/migrations"

var RenameTableOp1 = &migrations.OpRenameTable{
	From: "foo",
	To:   "bar",
}
