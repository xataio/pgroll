// SPDX-License-Identifier: Apache-2.0

package cmd

import "errors"

var errPGRollNotInitialized = errors.New("pgroll is not initialized, run 'pgroll init' to initialize")
