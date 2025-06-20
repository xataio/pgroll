// SPDX-License-Identifier: Apache-2.0

package migrations

import (
	"bytes"
	"context"
	"database/sql"
	"fmt"
	"text/template"

	"github.com/lib/pq"

	"github.com/xataio/pgroll/pkg/backfill"
	"github.com/xataio/pgroll/pkg/db"
	"github.com/xataio/pgroll/pkg/migrations/templates"
)

type createTriggerAction struct {
	conn db.DB
	cfg  backfill.TriggerConfig
}

func NewCreateTriggerAction(conn db.DB, cfg backfill.TriggerConfig) DBAction {
	return &createTriggerAction{
		conn: conn,
		cfg:  cfg,
	}
}

func (a *createTriggerAction) Execute(ctx context.Context) error {
	// Parenthesize the up/down SQL if it's not parenthesized already
	if len(a.cfg.SQL) > 0 && a.cfg.SQL[0] != '(' {
		a.cfg.SQL = "(" + a.cfg.SQL + ")"
	}

	a.cfg.NeedsBackfillColumn = backfill.CNeedsBackfillColumn

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
				pq.QuoteIdentifier(backfill.CNeedsBackfillColumn)))
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

func buildFunction(cfg backfill.TriggerConfig) (string, error) {
	return executeTemplate("function", templates.Function, cfg)
}

func buildTrigger(cfg backfill.TriggerConfig) (string, error) {
	return executeTemplate("trigger", templates.Trigger, cfg)
}

func executeTemplate(name, content string, cfg backfill.TriggerConfig) (string, error) {
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
