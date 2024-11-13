package benchmarks

import (
	"os"
	"sync"
	"time"
)

type Reports struct {
	mu              sync.Mutex
	GitSHA          string
	PostgresVersion string
	Timestamp       int64
	Reports         []Report
}

func (r *Reports) AddReport(report Report) {
	r.mu.Lock()
	defer r.mu.Unlock()
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
