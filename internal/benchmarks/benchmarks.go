package benchmarks

import (
	"os"
	"sync"
	"time"
)

type ReportRecorder struct {
	mu              sync.Mutex
	GitSHA          string
	PostgresVersion string
	Timestamp       int64
	Reports         []Report
}

func (r *ReportRecorder) AddReport(report Report) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.Reports = append(r.Reports, report)
}

func getPostgresVersion() string {
	return os.Getenv("POSTGRES_VERSION")
}

func newReportRecorder() *ReportRecorder {
	return &ReportRecorder{
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
