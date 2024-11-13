package benchmarks

import (
	"os"
	"time"
)

type Reports struct {
	GitSHA          string
	PostgresVersion string
	Timestamp       int64
	Reports         []Report
}

func (r *Reports) AddReport(report Report) {
	r.Reports = append(r.Reports, report)
}

func getPostgresVersion() string {
	return os.Getenv("POSTGRES_VERSION")
}

func newReports() *Reports {
	return &Reports{
		GitSHA:          os.Getenv("GITHUB_SHA"),
		PostgresVersion: getPostgresVersion(),
		Timestamp:       time.Now().Unix(),
		Reports:         []Report{},
	}
}

type Report struct {
	Name          string
	RowCount      int
	RowsPerSecond float64
}
