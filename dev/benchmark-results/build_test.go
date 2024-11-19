// SPDX-License-Identifier: Apache-2.0

package main

import (
	"bufio"
	"cmp"
	"encoding/json"
	"fmt"
	"maps"
	"os"
	"slices"
	"sort"
	"strings"
	"testing"

	"github.com/go-echarts/go-echarts/v2/charts"
	"github.com/go-echarts/go-echarts/v2/components"
	"github.com/go-echarts/go-echarts/v2/opts"
	"github.com/stretchr/testify/assert"

	"github.com/xataio/pgroll/internal/benchmarks"
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
