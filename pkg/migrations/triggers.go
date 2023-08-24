package migrations

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/lib/pq"
	"github.com/xataio/pg-roll/pkg/schema"
)

type TriggerDirection string

const (
	TriggerDirectionUp   TriggerDirection = "up"
	TriggerDirectionDown TriggerDirection = "down"
)

type triggerConfig struct {
	Direction      TriggerDirection
	TestExpr       string
	ElseExpr       string
	SchemaName     string
	StateSchema    string
	Table          string
	Column         string
	PhysicalColumn string
	SQL            string
}

func createTrigger(ctx context.Context, conn *sql.DB, s *schema.Schema, cfg triggerConfig) error {
	// Generate the SQL declarations for the trigger function
	// This results in declarations like:
	//   col1 table.col1%TYPE := NEW.col1;
	// Without these declarations, users would have to reference
	// `col1` as `NEW.col1` in their `up` SQL.
	sqlDeclarations := func(s *schema.Schema) string {
		table := s.GetTable(cfg.Table)

		decls := ""
		for _, c := range table.Columns {
			decls += fmt.Sprintf("%[1]s %[3]s.%[2]s.%[1]s%%TYPE := NEW.%[1]s;\n",
				pq.QuoteIdentifier(c.Name),
				pq.QuoteIdentifier(table.Name),
				pq.QuoteIdentifier(cfg.SchemaName))
		}
		return decls
	}

	cmp := "<>"
	if cfg.Direction == TriggerDirectionDown {
		cmp = "="
	}

	testExpr := "TRUE"
	if cfg.TestExpr != "" {
		testExpr = cfg.TestExpr
	}

	//nolint:gosec // unavoidable SQL injection warning when running arbitrary SQL
	triggerFn := fmt.Sprintf(`CREATE OR REPLACE FUNCTION %[1]s() 
    RETURNS TRIGGER 
    LANGUAGE PLPGSQL
    AS $$
    DECLARE
      %[4]s
      latest_schema text;
      search_path text;
    BEGIN
      SELECT %[5]s || '_' || latest_version INTO latest_schema FROM %[6]s.latest_version(%[5]s);
      SELECT current_setting INTO search_path FROM current_setting('search_path');

      IF search_path %[7]s latest_schema AND %[8]s THEN
        NEW.%[2]s = %[3]s;
      ELSE 
        %[9]s
      END IF;

      RETURN NEW;
    END; $$`,
		pq.QuoteIdentifier(TriggerFunctionName(cfg.Table, cfg.Column)),
		pq.QuoteIdentifier(cfg.PhysicalColumn),
		cfg.SQL,
		sqlDeclarations(s),
		pq.QuoteLiteral(cfg.SchemaName),
		pq.QuoteIdentifier(cfg.StateSchema),
		cmp,
		testExpr,
		cfg.ElseExpr)

	_, err := conn.ExecContext(ctx, triggerFn)
	if err != nil {
		return err
	}

	trigger := fmt.Sprintf(`CREATE OR REPLACE TRIGGER %[1]s
    BEFORE UPDATE OR INSERT
    ON %[2]s
    FOR EACH ROW
    EXECUTE PROCEDURE %[3]s();`,
		pq.QuoteIdentifier(TriggerName(cfg.Table, cfg.Column)),
		pq.QuoteIdentifier(cfg.Table),
		pq.QuoteIdentifier(TriggerFunctionName(cfg.Table, cfg.Column)))

	_, err = conn.ExecContext(ctx, trigger)
	if err != nil {
		return err
	}

	return nil
}

func TriggerFunctionName(tableName, columnName string) string {
	return "_pgroll_trigger_" + tableName + "_" + columnName
}

func TriggerName(tableName, columnName string) string {
	return TriggerFunctionName(tableName, columnName)
}
