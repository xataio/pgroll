// SPDX-License-Identifier: Apache-2.0

package main

import (
	"os"

	"github.com/xataio/pg-roll/cmd"
)

func main() {
	if err := cmd.Execute(); err != nil {
		os.Exit(1)
	}
}
