// SPDX-License-Identifier: Apache-2.0

package main

import (
	"os"

	"github.com/xataio/pgroll/cmd"
)

func main() {
	if err := cmd.Execute(); err != nil {
		os.Exit(1)
	}
}
