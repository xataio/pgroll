// SPDX-License-Identifier: Apache-2.0

package testutils

import "math/rand"

func randomDBName() string {
	const length = 15
	const charset = "abcdefghijklmnopqrstuvwxyz"

	b := make([]byte, length)
	for i := range b {
		b[i] = charset[rand.Intn(len(charset))] // #nosec G404
	}

	return "testdb_" + string(b)
}
