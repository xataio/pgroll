// SPDX-License-Identifier: Apache-2.0

package templates

import (
	"bytes"
	"strings"
	"text/template"

	"github.com/lib/pq"
)

type BatchConfig struct {
	TableName  string
	PrimaryKey []string
	LastValue  []string
	BatchSize  int
}

func BuildSQL(cfg BatchConfig) (string, error) {
	return executeTemplate("sql", SQL, cfg)
}

func executeTemplate(name, content string, cfg BatchConfig) (string, error) {
	ql := pq.QuoteLiteral
	qi := pq.QuoteIdentifier

	tmpl := template.Must(template.New(name).
		Funcs(template.FuncMap{
			"ql": ql,
			"qi": qi,
			"commaSeparate": func(slice []string) string {
				return strings.Join(slice, ", ")
			},
			"quoteIdentifiers": func(slice []string) []string {
				quoted := make([]string, len(slice))
				for i, s := range slice {
					quoted[i] = qi(s)
				}
				return quoted
			},
			"quoteLiterals": func(slice []string) []string {
				quoted := make([]string, len(slice))
				for i, s := range slice {
					quoted[i] = ql(s)
				}
				return quoted
			},
			"updateSetClause": func(tableName string, columns []string) string {
				quoted := make([]string, len(columns))
				for i, c := range columns {
					quoted[i] = qi(c) + " = " + qi(tableName) + "." + qi(c)
				}
				return strings.Join(quoted, ", ")
			},
			"updateWhereClause": func(tableName string, columns []string) string {
				quoted := make([]string, len(columns))
				for i, c := range columns {
					quoted[i] = qi(tableName) + "." + qi(c) + " = batch." + qi(c)
				}
				return strings.Join(quoted, " AND ")
			},
			"updateReturnClause": func(tableName string, columns []string) string {
				quoted := make([]string, len(columns))
				for i, c := range columns {
					quoted[i] = qi(tableName) + "." + qi(c)
				}
				return strings.Join(quoted, ", ")
			},
			"selectLastValue": func(columns []string) string {
				quoted := make([]string, len(columns))
				for i, c := range columns {
					quoted[i] = "LAST_VALUE(" + qi(c) + ") OVER()"
				}
				return strings.Join(quoted, ", ")
			},
		}).
		Parse(content))

	buf := bytes.Buffer{}
	if err := tmpl.Execute(&buf, cfg); err != nil {
		return "", err
	}

	return buf.String(), nil
}
