// SPDX-License-Identifier: Apache-2.0

package roll

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
