// SPDX-License-Identifier: Apache-2.0

package migrations

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"

	"sigs.k8s.io/yaml"
)

type MigrationFormat int

const (
	InvalidMigrationFormat MigrationFormat = iota
	YAMLMigrationFormat
	JSONMigrationFormat
)

var ErrInvalidMigrationFormat = errors.New("invalid migration format")

// MigrationWriter is responsible for writing Migrations and RawMigrations
// to the configured io.Writer instance in either YAML or JSON.
type MigrationWriter struct {
	writer io.Writer
	format MigrationFormat
}

// NewMigrationFormat returns YAML or JSON format
func NewMigrationFormat(useJSON bool) MigrationFormat {
	if useJSON {
		return JSONMigrationFormat
	}
	return YAMLMigrationFormat
}

// Extension returns the extension name for the migration file
func (f MigrationFormat) Extension() string {
	switch f {
	case YAMLMigrationFormat:
		return "yaml"
	case JSONMigrationFormat:
		return "json"
	}
	return ""
}

// NewWriter creates a new MigrationWriter
func NewWriter(w io.Writer, f MigrationFormat) *MigrationWriter {
	return &MigrationWriter{
		writer: w,
		format: f,
	}
}

func (w *MigrationWriter) Write(m *Migration) error {
	return w.writeAny(m)
}

func (w *MigrationWriter) WriteRaw(m *RawMigration) error {
	return w.writeAny(m)
}

func (w *MigrationWriter) writeAny(migration any) error {
	switch w.format {
	case YAMLMigrationFormat:
		yml, err := yaml.Marshal(migration)
		if err != nil {
			return err
		}
		_, err = w.writer.Write(yml)
		return fmt.Errorf("encode yaml migration: %w", err)
	case JSONMigrationFormat:
		enc := json.NewEncoder(w.writer)
		enc.SetIndent("", "  ")
		if err := enc.Encode(migration); err != nil {
			return fmt.Errorf("encode json migration: %w", err)
		}
	default:
		return ErrInvalidMigrationFormat
	}
	return nil
}
