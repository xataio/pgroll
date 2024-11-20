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
	"github.com/spf13/cobra"
)

var rootCmd = &cobra.Command{
	Use:          "build <inputfile> <outputfile>",
	SilenceUsage: true,
	Args:         cobra.ExactArgs(2),
	RunE: func(cmd *cobra.Command, args []string) error {
		err := buildCharts(args[0], args[1])
		if err != nil {
			cmd.PrintErr(err)
			os.Exit(1)
		}
		return nil
	},
}

// This will generate line charts displaying benchmark results over time. Each set of charts will
// apply to a single version of Postgres.
func main() {
	if err := rootCmd.Execute(); err != nil {
		os.Exit(1)
	}
}

func buildCharts(inputFile, outputFile string) error {
	log.Println("Loading data")
	reports, err := loadData(inputFile)
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

	f, err := os.Create(outputFile)
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
	log.Printf("Charts generated at %s", outputFile)

	return nil
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
func generateCharts(reports []BenchmarkReports) []*charts.Line {
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

func loadData(filename string) (allReports []BenchmarkReports, err error) {
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
		var reports BenchmarkReports
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
// the end if it exists.
func trimName(name string) string {
	name = strings.TrimPrefix(name, "Benchmark")
	if i := strings.LastIndex(name, "/"); i != -1 {
		name = name[:i]
	}
	return name
}

// First 7 characters
func shortSHA(sha string) string {
	return sha[:7]
}

type BenchmarkReports struct {
	GitSHA          string
	PostgresVersion string
	Timestamp       int64
	Reports         []BenchmarkReport
}

func (r *BenchmarkReports) AddReport(report BenchmarkReport) {
	r.Reports = append(r.Reports, report)
}

type BenchmarkReport struct {
	Name     string
	RowCount int
	Unit     string
	Result   float64
}
