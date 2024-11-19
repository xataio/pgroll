// SPDX-License-Identifier: Apache-2.0

package main

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

// TestBuildChartsRegression is a simple regression test
func TestBuildChartsRegression(t *testing.T) {
	reports, err := loadData("testdata/benchmark-results.json")
	assert.NoError(t, err)
	assert.Len(t, reports, 5)

	generated := generateCharts(reports)
	// 5 versions * 3 benchmarks
	assert.Len(t, generated, 15)
}
