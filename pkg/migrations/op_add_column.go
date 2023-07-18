package migrations

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	"github.com/lib/pq"

	"pg-roll/pkg/schema"
)

type OpAddColumn struct {
	Table  string  `json:"table"`
	Up     *string `json:"up"`
	Column Column  `json:"column"`
}

var _ Operation = (*OpAddColumn)(nil)

func (o *OpAddColumn) Start(ctx context.Context, conn *sql.DB, schemaName, stateSchema string, s *schema.Schema) error {
	table := s.GetTable(o.Table)

	if err := addColumn(ctx, conn, *o, table); err != nil {
		return fmt.Errorf("failed to start add column operation: %w", err)
	}

	if o.Up != nil {
		if err := createTrigger(ctx, conn, o, schemaName, stateSchema, s); err != nil {
			return fmt.Errorf("failed to create trigger: %w", err)
		}
	}

	table.AddColumn(o.Column.Name, schema.Column{
		Name: TemporaryName(o.Column.Name),
	})

	return nil
}

func (o *OpAddColumn) Complete(ctx context.Context, conn *sql.DB) error {
	tempName := TemporaryName(o.Column.Name)

	_, err := conn.ExecContext(ctx, fmt.Sprintf("ALTER TABLE IF EXISTS %s RENAME COLUMN %s TO %s",
		pq.QuoteIdentifier(o.Table),
		pq.QuoteIdentifier(tempName),
		pq.QuoteIdentifier(o.Column.Name),
	))
	return err
}

func (o *OpAddColumn) Rollback(ctx context.Context, conn *sql.DB) error {
	tempName := TemporaryName(o.Column.Name)

	_, err := conn.ExecContext(ctx, fmt.Sprintf("ALTER TABLE IF EXISTS %s DROP COLUMN IF EXISTS %s",
		pq.QuoteIdentifier(o.Table),
		pq.QuoteIdentifier(tempName)))
	return err
}

func (o *OpAddColumn) Validate(ctx context.Context, s *schema.Schema) error {
	table := s.GetTable(o.Table)
	if table == nil {
		return TableDoesNotExistError{Name: o.Table}
	}

	if table.GetColumn(o.Column.Name) != nil {
		return ColumnAlreadyExistsError{Name: o.Column.Name, Table: o.Table}
	}

	if !o.Column.Nullable && o.Column.Default == nil {
		return errors.New("adding non-nullable columns without a default is not supported")
	}

	if o.Column.PrimaryKey {
		return errors.New("adding primary key columns is not supported")
	}

	return nil
}

func addColumn(ctx context.Context, conn *sql.DB, o OpAddColumn, t *schema.Table) error {
	o.Column.Name = TemporaryName(o.Column.Name)

	_, err := conn.ExecContext(ctx, fmt.Sprintf("ALTER TABLE %s ADD COLUMN %s",
		pq.QuoteIdentifier(t.Name),
		ColumnToSQL(o.Column),
	))
	return err
}

func createTrigger(ctx context.Context, conn *sql.DB, o *OpAddColumn, schemaName, stateSchema string, s *schema.Schema) error {
	triggerFnName := func(o *OpAddColumn) string {
		return "_pgroll_add_column_" + o.Table + "_" + o.Column.Name
	}
	triggerName := triggerFnName

	sqlDeclarations := func(s *schema.Schema) string {
		table := s.GetTable(o.Table)

		decls := ""
		for _, c := range table.Columns {
			decls += fmt.Sprintf("%[1]s %[2]s.%[1]s%%TYPE := NEW.%[1]s;\n",
				pq.QuoteIdentifier(c.Name),
				pq.QuoteIdentifier(table.Name))
		}
		return decls
	}

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

      IF search_path <> latest_schema THEN
        NEW.%[2]s = %[3]s;
      END IF;

      RETURN NEW;
    END; $$`,
		pq.QuoteIdentifier(triggerFnName(o)),
		pq.QuoteIdentifier(TemporaryName(o.Column.Name)),
		*o.Up,
		sqlDeclarations(s),
		pq.QuoteLiteral(schemaName),
		pq.QuoteIdentifier(stateSchema))

	_, err := conn.ExecContext(ctx, triggerFn)
	if err != nil {
		return err
	}

	trigger := fmt.Sprintf(`CREATE OR REPLACE TRIGGER %[1]s
    BEFORE UPDATE OR INSERT
    ON %[2]s
    FOR EACH ROW
    EXECUTE PROCEDURE %[3]s();`,
		pq.QuoteIdentifier(triggerName(o)),
		pq.QuoteIdentifier(o.Table),
		pq.QuoteIdentifier(triggerFnName(o)))

	_, err = conn.ExecContext(ctx, trigger)
	if err != nil {
		return err
	}

	return nil
}
