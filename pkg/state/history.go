// SPDX-License-Identifier: Apache-2.0

package state

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"time"

	"github.com/lib/pq"
	"sigs.k8s.io/yaml"

	"github.com/xataio/pgroll/pkg/migrations"
)

// Migration represents a single migration in the migration history
// of a schema
type Migration struct {
	Migration migrations.Migration
	CreatedAt time.Time
}

// SchemaHistory returns all migrations applied to a schema in ascending
// timestamp order
func (s *State) SchemaHistory(ctx context.Context, schema string) ([]Migration, error) {
	rows, err := s.pgConn.QueryContext(ctx,
		fmt.Sprintf(`SELECT name, migration, created_at
			FROM %s.migrations
			WHERE schema=$1 ORDER BY created_at`,
			pq.QuoteIdentifier(s.schema)), schema)
	if err != nil {
		return nil, err
	}

	var entries []Migration
	for rows.Next() {
		var name, rawMigration string
		var createdAt time.Time

		if err := rows.Scan(&name, &rawMigration, &createdAt); err != nil {
			return nil, fmt.Errorf("row scan: %w", err)
		}

		var mig migrations.Migration
		err = json.Unmarshal([]byte(rawMigration), &mig)
		if err != nil {
			return nil, err
		}

		entries = append(entries, Migration{
			Migration: mig,
			CreatedAt: createdAt,
		})
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterating rows: %w", err)
	}

	return entries, nil
}

// WriteToFile writes the migration to a file in `targetDir`, prefixing the
// filename with `prefix`. The output format defaults to YAML, but can
// be changed to JSON by setting `useJSON` to true.
func (m *Migration) WriteToFile(targetDir, prefix string, useJSON bool) error {
	err := os.MkdirAll(targetDir, 0o755)
	if err != nil {
		return err
	}

	suffix := "yaml"
	if useJSON {
		suffix = "json"
	}

	fileName := fmt.Sprintf("%s%s.%s", prefix, m.Migration.Name, suffix)
	filePath := filepath.Join(targetDir, fileName)

	file, err := os.Create(filePath)
	if err != nil {
		return err
	}
	defer file.Close()

	if useJSON {
		return m.writeAsJSON(file)
	} else {
		return m.writeAsYAML(file)
	}
}

func (m *Migration) writeAsJSON(w io.Writer) error {
	encoder := json.NewEncoder(w)
	encoder.SetIndent("", "  ")

	return encoder.Encode(m.Migration)
}

func (m *Migration) writeAsYAML(w io.Writer) error {
	yml, err := yaml.Marshal(m.Migration)
	if err != nil {
		return err
	}

	_, err = w.Write(yml)
	return err
}
