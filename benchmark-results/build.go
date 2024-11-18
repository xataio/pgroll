// SPDX-License-Identifier: Apache-2.0

package main

import (
	"bufio"
	"cmp"
	"encoding/json"
	"fmt"
	"log"
	"maps"
	"os"
	"slices"
	"sort"
	"strings"

	"github.com/go-echarts/go-echarts/v2/charts"
	"github.com/go-echarts/go-echarts/v2/components"
	"github.com/go-echarts/go-echarts/v2/opts"

	"github.com/xataio/pgroll/internal/benchmarks"
)

// This will generate line charts displaying benchmark results over time. Each set of charts will
// apply to a single version of Postgres.

func main() {
	input := mustEnv("FILENAME_BENCHMARK_RESULTS")
	output := mustEnv("FILENAME_BENCHMARK_OUTPUT")

	log.Println("Loading data")
	reports, err := loadData(input)
	if err != nil {
		log.Fatalf("Loading data: %v", err)
	}
	log.Printf("Loaded %d reports", len(reports))

	log.Println("Generating charts")
	allCharts := generateCharts(reports)

	page := components.NewPage()
	page.SetPageTitle("pgroll benchmark results")
	page.SetLayout("flex")

	for _, c := range allCharts {
		page.AddCharts(c)
	}

	f, err := os.Create(output)
	if err != nil {
		log.Fatalf("Creating output file: %v", err)
	}
	defer func() {
		if err := f.Close(); err != nil {
			log.Fatalf("Closing output file: %v", err)
		}
	}()

	if err := page.Render(f); err != nil {
		log.Fatalf("Rendering: %s", err)
	}
}

type dataKey struct {
	postgresVersion string
	benchmarkName   string
	rowCount        int
	sha             string
}

type chartKey struct {
	postgresVersion string
	benchmarkName   string
}

// generateCharts will generate charts grouped by postgres version and benchmark with series for each
// rowCount
func generateCharts(reports []benchmarks.Reports) []*charts.Line {
	// Time data for each sha so we can order them later
	timeOrder := make(map[string]int64) // shortSHA -> timestamp

	// rows/s grouped by dataKey
	groupedData := make(map[dataKey]float64)

	// set of possible row counts
	rowCounts := make(map[int]struct{})

	for _, group := range reports {
		short := shortSHA(group.GitSHA)
		timeOrder[short] = group.Timestamp
		for _, report := range group.Reports {
			key := dataKey{
				postgresVersion: group.PostgresVersion,
				benchmarkName:   trimName(report.Name),
				sha:             short,
				rowCount:        report.RowCount,
			}
			groupedData[key] = report.Result
			rowCounts[report.RowCount] = struct{}{}
		}
	}

	// Now we have the data grouped in a way that makes it easy for us to create each chart

	// Create x-axis for each chart
	xs := make(map[chartKey][]string)
	for d := range groupedData {
		ck := chartKey{postgresVersion: d.postgresVersion, benchmarkName: d.benchmarkName}
		x := xs[ck]
		x = append(x, d.sha)
		xs[ck] = x
	}
	// Sort and deduplicate xs in time order
	for key, x := range xs {
		// Dedupe
		slices.Sort(x)
		x = slices.Compact(x)
		// Sort by time
		slices.SortFunc(x, func(a, b string) int {
			return cmp.Compare(timeOrder[a], timeOrder[b])
		})
		xs[key] = x
	}

	allCharts := make([]*charts.Line, 0, len(xs))

	for ck, xValues := range xs {
		chart := charts.NewLine()
		chart.SetGlobalOptions(
			charts.WithTitleOpts(opts.Title{
				Title: fmt.Sprintf("%s (%s)", ck.benchmarkName, ck.postgresVersion),
			}),
			charts.WithAnimation(false))
		chart.SetXAxis(xValues)

		series := make(map[int][]float64) // rowCount -> rows/s

		// Add series per rowCount
		for _, x := range xValues {
			for rc := range rowCounts {
				dk := dataKey{
					postgresVersion: ck.postgresVersion,
					benchmarkName:   ck.benchmarkName,
					rowCount:        rc,
					sha:             x,
				}
				value, ok := groupedData[dk]
				if !ok {
					continue
				}

				series[rc] = append(series[rc], value)
			}
		}

		// Make sure row counts are consistently sorted
		sortedRowCounts := slices.Collect(maps.Keys(rowCounts))
		slices.Sort(sortedRowCounts)

		for _, rowCount := range sortedRowCounts {
			s := series[rowCount]

			name := fmt.Sprintf("%d", rowCount)
			data := make([]opts.LineData, len(series[rowCount]))
			for i := range series[rowCount] {
				data[i] = opts.LineData{
					Value: s[i],
				}
			}
			chart.AddSeries(name, data)
		}

		allCharts = append(allCharts, chart)
	}

	sort.Slice(allCharts, func(i, j int) bool {
		return allCharts[i].Title.Title < allCharts[j].Title.Title
	})

	return allCharts
}

func loadData(filename string) (allReports []benchmarks.Reports, err error) {
	f, err := os.Open(filename)
	if err != nil {
		return nil, fmt.Errorf("opening file: %w", err)
	}
	defer func() {
		err = f.Close()
	}()

	scanner := bufio.NewScanner(f)

	// Each line represents a collection of results from a single commit
	for scanner.Scan() {
		var reports benchmarks.Reports
		line := scanner.Text()
		if err := json.Unmarshal([]byte(line), &reports); err != nil {
			return nil, fmt.Errorf("unmarshalling reports: %w", err)
		}
		allReports = append(allReports, reports)
	}
	if err := scanner.Err(); err != nil {
		return nil, fmt.Errorf("scanning input: %w", err)
	}

	return allReports, err
}

// Benchmarks are grouped by the number of rows they were tested against. We need to trim this off
// the end.
func trimName(name string) string {
	return strings.TrimPrefix(name[:strings.LastIndex(name, "/")], "Benchmark")
}

// First 7 characters
func shortSHA(sha string) string {
	return sha[:7]
}

func mustEnv(key string) string {
	v := os.Getenv(key)
	if v == "" {
		log.Fatalf("Missing required environment variable: %q", key)
	}
	return v
}
