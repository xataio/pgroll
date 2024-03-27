// SPDX-License-Identifier: Apache-2.0

package migrations

import (
	"bytes"
	"context"
	"database/sql"
	"text/template"

	"github.com/lib/pq"
	"github.com/xataio/pgroll/pkg/migrations/templates"
	"github.com/xataio/pgroll/pkg/schema"
)

type TriggerDirection string

const (
	TriggerDirectionUp   TriggerDirection = "up"
	TriggerDirectionDown TriggerDirection = "down"
)

type triggerConfig struct {
	Name           string
	Direction      TriggerDirection
	Columns        map[string]schema.Column
	SchemaName     string
	TableName      string
	PhysicalColumn string
	StateSchema    string
	TestExpr       string
	ElseExpr       string
	SQL            string
}

func createTrigger(ctx context.Context, conn *sql.DB, tr SQLTransformer, cfg triggerConfig) error {
	sql, err := tr.TransformSQL(cfg.SQL)
	if err != nil {
		return err
	}
	cfg.SQL = sql

	funcSQL, err := buildFunction(cfg)
	if err != nil {
		return err
	}

	_, err = conn.ExecContext(ctx, funcSQL)
	if err != nil {
		return err
	}

	triggerSQL, err := buildTrigger(cfg)
	if err != nil {
		return err
	}

	_, err = conn.ExecContext(ctx, triggerSQL)
	if err != nil {
		return err
	}

	return nil
}

func buildFunction(cfg triggerConfig) (string, error) {
	return executeTemplate("function", templates.Function, cfg)
}

func buildTrigger(cfg triggerConfig) (string, error) {
	return executeTemplate("trigger", templates.Trigger, cfg)
}

func TriggerFunctionName(tableName, columnName string) string {
	return "_pgroll_trigger_" + tableName + "_" + columnName
}

func TriggerName(tableName, columnName string) string {
	return TriggerFunctionName(tableName, columnName)
}

func executeTemplate(name, content string, cfg triggerConfig) (string, error) {
	tmpl := template.Must(template.
		New(name).
		Funcs(template.FuncMap{
			"ql": pq.QuoteLiteral,
			"qi": pq.QuoteIdentifier,
		}).
		Parse(content))

	buf := bytes.Buffer{}
	if err := tmpl.Execute(&buf, cfg); err != nil {
		return "", err
	}

	return buf.String(), nil
}
