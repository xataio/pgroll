// SPDX-License-Identifier: Apache-2.0

package migrations

import (
	"bytes"
	"context"
	"database/sql"
	"errors"
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

func createTrigger(ctx context.Context, op Operation, conn *sql.DB, cfg triggerConfig) error {
	if cfg.Direction == TriggerDirectionDown {
		if _, ok := op.(Downer); !ok {
			return errors.New("down triggers can only be created by operations implementing Downer")
		}
	}

	if cfg.Direction == TriggerDirectionUp {
		if _, ok := op.(Upper); !ok {
			return errors.New("up triggers can only be created by operations implementing Upper")
		}
	}

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
