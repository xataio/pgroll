// SPDX-License-Identifier: Apache-2.0

package backfill

import "fmt"

type NotPossibleError struct {
	Table string
}

func (e NotPossibleError) Error() string {
	return fmt.Sprintf("a backfill is required but table %q doesn't have a single column primary key or a UNIQUE, NOT NULL column", e.Table)
}
