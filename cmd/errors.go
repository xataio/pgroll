// SPDX-License-Identifier: Apache-2.0

package cmd

import (
	"errors"
	"fmt"
)

var errPGRollNotInitialized = errors.New("pgroll is not initialized, run 'pgroll init' to initialize")

func errBaselineRequired(schema string) error {
	return fmt.Errorf("schema %q is non-empty but has no migration history, run 'pgroll baseline' first", schema)
}
