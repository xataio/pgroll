// SPDX-License-Identifier: Apache-2.0

package backfill

import (
	"bytes"
	"context"
	"database/sql"
	"fmt"
	"strings"
	"text/template"

	"github.com/lib/pq"

	"github.com/xataio/pgroll/pkg/db"
	"github.com/xataio/pgroll/pkg/migrations/templates"
	"github.com/xataio/pgroll/pkg/schema"
)

type TriggerDirection string

const (
	TriggerDirectionUp   TriggerDirection = "up"
	TriggerDirectionDown TriggerDirection = "down"
)

type TriggerConfig struct {
	Name                string
	Direction           TriggerDirection
	Columns             map[string]*schema.Column
	SchemaName          string
	TableName           string
	PhysicalColumn      string
	LatestSchema        string
	SQL                 []string
	NeedsBackfillColumn string
}

type triggerCreator struct {
	triggers map[string]TriggerConfig
}

func (c *triggerCreator) addTrigger(t TriggerConfig) {
	if trigger, exists := c.triggers[t.Name]; !exists {
		c.triggers[t.Name] = t
	} else {
		for _, sql := range t.SQL {
			trigger.SQL = append(trigger.SQL, rewriteTriggerSQL(sql, findColumnName(t.Columns, t.PhysicalColumn), t.PhysicalColumn))
		}
		c.triggers[t.Name] = trigger
	}
}

func (c *triggerCreator) loadTriggers(ctx context.Context, conn db.DB) error {
	for _, t := range c.triggers {
		action := &createTriggerAction{conn: conn, cfg: t}
		if err := action.Execute(ctx); err != nil {
			return fmt.Errorf("creating trigger %q: %w", t.Name, err)
		}
	}
	return nil
}

func findColumnName(columns map[string]*schema.Column, columnName string) string {
	for name, col := range columns {
		if col.Name == columnName {
			return name
		}
	}
	return columnName
}

func rewriteTriggerSQL(sql string, from, to string) string {
	return strings.ReplaceAll(sql, from, fmt.Sprintf("NEW.%s", pq.QuoteIdentifier(to)))
}

type createTriggerAction struct {
	conn db.DB
	cfg  TriggerConfig
}

func (a *createTriggerAction) Execute(ctx context.Context) error {
	// Parenthesize the up/down SQL if it's not parenthesized already
	for i, sql := range a.cfg.SQL {
		if len(sql) > 0 && sql[0] != '(' {
			a.cfg.SQL[i] = "(" + sql + ")"
		}
	}

	a.cfg.NeedsBackfillColumn = CNeedsBackfillColumn

	funcSQL, err := buildFunction(a.cfg)
	if err != nil {
		return err
	}

	triggerSQL, err := buildTrigger(a.cfg)
	if err != nil {
		return err
	}

	return a.conn.WithRetryableTransaction(ctx, func(ctx context.Context, tx *sql.Tx) error {
		_, err := a.conn.ExecContext(ctx,
			fmt.Sprintf("ALTER TABLE %s ADD COLUMN IF NOT EXISTS %s boolean DEFAULT true",
				pq.QuoteIdentifier(a.cfg.TableName),
				pq.QuoteIdentifier(CNeedsBackfillColumn)))
		if err != nil {
			return err
		}

		_, err = a.conn.ExecContext(ctx, funcSQL)
		if err != nil {
			return err
		}

		_, err = a.conn.ExecContext(ctx, triggerSQL)
		return err
	})
}

func buildFunction(cfg TriggerConfig) (string, error) {
	return executeTemplate("function", templates.Function, cfg)
}

func buildTrigger(cfg TriggerConfig) (string, error) {
	fmt.Println(">>>>>>>>>>>>", len(cfg.SQL))
	return executeTemplate("trigger", templates.Trigger, cfg)
}

// TriggerFunctionName returns the name of the trigger function
// for a given table and column.
func TriggerFunctionName(tableName, columnName string) string {
	return "_pgroll_trigger_" + tableName + "_" + columnName
}

// TriggerName returns the name of the trigger for a given table and column.
func TriggerName(tableName, columnName string) string {
	return TriggerFunctionName(tableName, columnName)
}

func executeTemplate(name, content string, cfg TriggerConfig) (string, error) {
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
