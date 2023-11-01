// SPDX-License-Identifier: Apache-2.0

package state

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/lib/pq"
	"github.com/xataio/pgroll/pkg/migrations"
	"github.com/xataio/pgroll/pkg/schema"
)

const sqlInit = `
CREATE SCHEMA IF NOT EXISTS %[1]s;

CREATE TABLE IF NOT EXISTS %[1]s.migrations (
	schema				NAME NOT NULL,
	name				TEXT NOT NULL,
	migration			JSONB NOT NULL,
	created_at			TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
	updated_at			TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,

	parent				TEXT,
	done				BOOLEAN NOT NULL DEFAULT false,
	resulting_schema	JSONB NOT NULL DEFAULT '{}'::jsonb,

    PRIMARY KEY (schema, name),
	FOREIGN KEY	(schema, parent) REFERENCES %[1]s.migrations(schema, name)
);

-- Only one migration can be active at a time
CREATE UNIQUE INDEX IF NOT EXISTS only_one_active ON %[1]s.migrations (schema, name, done) WHERE done = false;

-- Only first migration can exist without parent
CREATE UNIQUE INDEX IF NOT EXISTS only_first_migration_without_parent ON %[1]s.migrations (schema) WHERE parent IS NULL;

-- History is linear
CREATE UNIQUE INDEX IF NOT EXISTS history_is_linear ON %[1]s.migrations (schema, parent);

-- Helper functions

-- Are we in the middle of a migration?
CREATE OR REPLACE FUNCTION %[1]s.is_active_migration_period(schemaname NAME) RETURNS boolean
	AS $$ SELECT EXISTS (SELECT 1 FROM %[1]s.migrations WHERE schema=schemaname AND done=false) $$
    LANGUAGE SQL
    STABLE;

-- Get the latest version name (this is the one with child migrations)
CREATE OR REPLACE FUNCTION %[1]s.latest_version(schemaname NAME) RETURNS text
SECURITY DEFINER
SET search_path = %[1]s, pg_catalog, pg_temp
AS $$ 
  SELECT p.name FROM %[1]s.migrations p 
  WHERE NOT EXISTS (
    SELECT 1 FROM %[1]s.migrations c WHERE schema=schemaname AND c.parent=p.name
  ) 
  AND schema=schemaname $$
LANGUAGE SQL
STABLE;

-- Get the name of the previous version of the schema, or NULL if there is none.
CREATE OR REPLACE FUNCTION %[1]s.previous_version(schemaname NAME) RETURNS text
AS $$
  SELECT parent FROM %[1]s.migrations WHERE name = (SELECT %[1]s.latest_version(schemaname)) AND schema=schemaname;
$$
LANGUAGE SQL
STABLE;

-- Get the JSON representation of the current schema
CREATE OR REPLACE FUNCTION %[1]s.read_schema(schemaname text) RETURNS jsonb
LANGUAGE plpgsql AS $$
DECLARE
	tables jsonb;
BEGIN
	SELECT json_build_object(
		'name', schemaname,
		'tables', (
			SELECT json_object_agg(t.relname, jsonb_build_object(
				'name', t.relname,
				'oid', t.oid,
				'comment', descr.description,
				'columns', (
					SELECT json_object_agg(name, c) FROM (
						SELECT
							attr.attname AS name,
							pg_get_expr(def.adbin, def.adrelid) AS default,
							NOT (
								attr.attnotnull
								OR tp.typtype = 'd'
								AND tp.typnotnull
							) AS nullable,
							CASE
								WHEN 'character varying' :: regtype = ANY(ARRAY [attr.atttypid, tp.typelem]) THEN REPLACE(
									format_type(attr.atttypid, attr.atttypmod),
									'character varying',
									'varchar'
								)
								WHEN 'timestamp with time zone' :: regtype = ANY(ARRAY [attr.atttypid, tp.typelem]) THEN REPLACE(
									format_type(attr.atttypid, attr.atttypmod),
									'timestamp with time zone',
									'timestamptz'
								)
								ELSE format_type(attr.atttypid, attr.atttypmod)
							END AS type,
							descr.description AS comment
						FROM
							pg_attribute AS attr
							INNER JOIN pg_type AS tp ON attr.atttypid = tp.oid
							LEFT JOIN pg_attrdef AS def ON attr.attrelid = def.adrelid
							AND attr.attnum = def.adnum
							LEFT JOIN pg_description AS descr ON attr.attrelid = descr.objoid
							AND attr.attnum = descr.objsubid
						WHERE
							attr.attnum > 0
							AND NOT attr.attisdropped
							AND attr.attrelid = t.oid
						ORDER BY
							attr.attnum
					) c
				),
				'primaryKey', (
				  SELECT json_agg(kcu.column_name) AS primary_key_columns
				  FROM information_schema.table_constraints AS tc 
				  JOIN information_schema.key_column_usage AS kcu 
				  ON tc.constraint_name = kcu.constraint_name
				  WHERE tc.table_name = t.relname
				  AND tc.table_schema = schemaname
				  AND tc.constraint_type = 'PRIMARY KEY'
				),
				'indexes', (
				  SELECT json_object_agg(pi.indexrelid::regclass, json_build_object(
				    'name', pi.indexrelid::regclass
				  ))
				  FROM pg_index pi 
				  INNER JOIN pg_class pgc ON pi.indexrelid = pgc.oid
				  WHERE pi.indrelid = t.oid::regclass
				)
			)) FROM pg_class AS t
				INNER JOIN pg_namespace AS ns ON t.relnamespace = ns.oid
				LEFT JOIN pg_description AS descr ON t.oid = descr.objoid
				AND descr.objsubid = 0
			WHERE
				ns.nspname = schemaname
				AND t.relkind IN ('r', 'p') -- tables only (ignores views, materialized views & foreign tables)
			)
		)
	INTO tables;

	RETURN tables;
END;
$$;

CREATE OR REPLACE FUNCTION %[1]s.raw_migration() RETURNS event_trigger
LANGUAGE plpgsql 
SECURITY DEFINER
SET search_path = %[1]s, pg_catalog, pg_temp AS $$
DECLARE
	schemaname TEXT;
BEGIN
	-- Ignore migrations done by pgroll
	IF (pg_catalog.current_setting('pgroll.internal', 'TRUE') <> 'TRUE') THEN
		RETURN;
	END IF;

	IF tg_event = 'sql_drop' THEN
		-- Guess the schema from drop commands
		SELECT schema_name INTO schemaname FROM pg_catalog.pg_event_trigger_dropped_objects() WHERE schema_name IS NOT NULL;

	ELSIF tg_event = 'ddl_command_end' THEN
		-- Guess the schema from ddl commands, ignore migrations that touch several schemas
		IF (SELECT pg_catalog.count(DISTINCT schema_name) FROM pg_catalog.pg_event_trigger_ddl_commands() WHERE schema_name IS NOT NULL) > 1 THEN
			RAISE NOTICE 'pgroll: ignoring migration that changes several schemas';
			RETURN;
		END IF;

		SELECT schema_name INTO schemaname FROM pg_catalog.pg_event_trigger_ddl_commands() WHERE schema_name IS NOT NULL;
	END IF;

	IF schemaname IS NULL THEN
		RAISE NOTICE 'pgroll: ignoring migration with null schema';
		RETURN;
	END IF;

	-- Ignore migrations done during a migration period
	IF %[1]s.is_active_migration_period(schemaname) THEN
		RAISE NOTICE 'pgroll: ignoring migration during active migration period';
		RETURN;
	END IF;

	-- Someone did a schema change without pgroll, include it in the history
	INSERT INTO %[1]s.migrations (schema, name, migration, resulting_schema, done, parent)
	VALUES (
		schemaname,
		pg_catalog.format('sql_%%s',pg_catalog.substr(pg_catalog.md5(pg_catalog.random()::text), 0, 15)),
		pg_catalog.json_build_object('sql', pg_catalog.json_build_object('up', pg_catalog.current_query())),
		%[1]s.read_schema(schemaname),
		true,
		%[1]s.latest_version(schemaname)
	);
END;
$$;

DROP EVENT TRIGGER IF EXISTS pg_roll_handle_ddl;
CREATE EVENT TRIGGER pg_roll_handle_ddl ON ddl_command_end
	EXECUTE FUNCTION %[1]s.raw_migration();

DROP EVENT TRIGGER IF EXISTS pg_roll_handle_drop;
CREATE EVENT TRIGGER pg_roll_handle_drop ON sql_drop
	EXECUTE FUNCTION %[1]s.raw_migration();

`

type State struct {
	pgConn *sql.DB
	schema string
}

func New(ctx context.Context, pgURL, stateSchema string) (*State, error) {
	conn, err := sql.Open("postgres", pgURL)
	if err != nil {
		return nil, err
	}

	if err := conn.PingContext(ctx); err != nil {
		return nil, err
	}

	_, err = conn.ExecContext(ctx, "SET LOCAL pgroll.internal to 'TRUE'")
	if err != nil {
		return nil, fmt.Errorf("unable to set pgroll.internal to true: %w", err)
	}

	return &State{
		pgConn: conn,
		schema: stateSchema,
	}, nil
}

func (s *State) Init(ctx context.Context) error {
	// ensure pgroll internal tables exist
	// TODO: eventually use migrations for this instead of hardcoding
	_, err := s.pgConn.ExecContext(ctx, fmt.Sprintf(sqlInit, pq.QuoteIdentifier(s.schema)))

	return err
}

func (s *State) Close() error {
	return s.pgConn.Close()
}

func (s *State) Schema() string {
	return s.schema
}

// IsActiveMigrationPeriod returns true if there is an active migration
func (s *State) IsActiveMigrationPeriod(ctx context.Context, schema string) (bool, error) {
	var isActive bool
	err := s.pgConn.QueryRowContext(ctx, fmt.Sprintf("SELECT %s.is_active_migration_period($1)", pq.QuoteIdentifier(s.schema)), schema).Scan(&isActive)
	if err != nil {
		return false, err
	}

	return isActive, nil
}

// GetActiveMigration returns the name & raw content of the active migration (if any), errors out otherwise
func (s *State) GetActiveMigration(ctx context.Context, schema string) (*migrations.Migration, error) {
	var name, rawMigration string
	err := s.pgConn.QueryRowContext(ctx, fmt.Sprintf("SELECT name, migration FROM %s.migrations WHERE schema=$1 AND done=false", pq.QuoteIdentifier(s.schema)), schema).Scan(&name, &rawMigration)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, ErrNoActiveMigration
		}
		return nil, err
	}

	var migration migrations.Migration
	err = json.Unmarshal([]byte(rawMigration), &migration)
	if err != nil {
		return nil, fmt.Errorf("unable to unmarshal migration: %w", err)
	}

	return &migration, nil
}

// LatestVersion returns the name of the latest version schema
func (s *State) LatestVersion(ctx context.Context, schema string) (*string, error) {
	var version *string
	err := s.pgConn.QueryRowContext(ctx,
		fmt.Sprintf("SELECT %s.latest_version($1)", pq.QuoteIdentifier(s.schema)),
		schema).Scan(&version)
	if err != nil {
		return nil, err
	}

	return version, nil
}

// PreviousVersion returns the name of the previous version schema
func (s *State) PreviousVersion(ctx context.Context, schema string) (*string, error) {
	var parent *string
	err := s.pgConn.QueryRowContext(ctx,
		fmt.Sprintf("SELECT %s.previous_version($1)", pq.QuoteIdentifier(s.schema)),
		schema).Scan(&parent)
	if err != nil {
		return nil, err
	}

	return parent, nil
}

// ReadSchema reads & returns the current schema from postgres
func ReadSchema(ctx context.Context, conn *sql.DB, stateSchema, schemaname string) (*schema.Schema, error) {
	var res schema.Schema
	err := conn.QueryRowContext(ctx, fmt.Sprintf("SELECT %[1]s.read_schema($1)", pq.QuoteIdentifier(stateSchema)), schemaname).Scan(&res)
	if err != nil {
		return nil, err
	}

	return &res, nil
}

// Start creates a new migration, storing its name and raw content
// this will effectively activate a new migration period, so `IsActiveMigrationPeriod` will return true
// until the migration is completed
// This method will return the current schema (before the migration is applied)
func (s *State) Start(ctx context.Context, schemaname string, migration *migrations.Migration) (*schema.Schema, error) {
	rawMigration, err := json.Marshal(migration)
	if err != nil {
		return nil, fmt.Errorf("unable to marshal migration: %w", err)
	}

	// create a new migration object and return the previous known schema
	// if there is no previous migration, read the schema from postgres
	stmt := fmt.Sprintf(`
		INSERT INTO %[1]s.migrations (schema, name, parent, migration) VALUES ($1, $2, %[1]s.latest_version($1), $3)
		RETURNING (
			SELECT COALESCE(
				(SELECT resulting_schema FROM %[1]s.migrations WHERE schema=$1 AND name=%[1]s.latest_version($1)),
				%[1]s.read_schema($1))
		)`, pq.QuoteIdentifier(s.schema))

	var rawSchema string
	err = s.pgConn.QueryRowContext(ctx, stmt, schemaname, migration.Name, rawMigration).Scan(&rawSchema)
	if err != nil {
		return nil, err
	}

	var schema schema.Schema
	err = json.Unmarshal([]byte(rawSchema), &schema)
	if err != nil {
		return nil, fmt.Errorf("unable to unmarshal schema: %w", err)
	}

	return &schema, nil
}

// Complete marks a migration as completed
func (s *State) Complete(ctx context.Context, schema, name string) error {
	res, err := s.pgConn.ExecContext(ctx, fmt.Sprintf("UPDATE %[1]s.migrations SET done=$1, resulting_schema=(SELECT %[1]s.read_schema($2)) WHERE schema=$2 AND name=$3 AND done=$4", pq.QuoteIdentifier(s.schema)), true, schema, name, false)
	if err != nil {
		return err
	}

	rows, err := res.RowsAffected()
	if err != nil {
		return err
	}

	if rows == 0 {
		return fmt.Errorf("no migration found with name %s", name)
	}

	return err
}

func (s *State) ReadSchema(ctx context.Context, schemaName string) (*schema.Schema, error) {
	var rawSchema []byte
	err := s.pgConn.QueryRowContext(ctx, fmt.Sprintf("SELECT %s.read_schema($1)", s.schema), schemaName).Scan(&rawSchema)
	if err != nil {
		return nil, err
	}

	var sc schema.Schema
	err = json.Unmarshal(rawSchema, &sc)
	if err != nil {
		return nil, fmt.Errorf("unable to unmarshal schema: %w", err)
	}

	return &sc, nil
}

// Rollback removes a migration from the state (we consider it rolled back, as if it never started)
func (s *State) Rollback(ctx context.Context, schema, name string) error {
	res, err := s.pgConn.ExecContext(ctx, fmt.Sprintf("DELETE FROM %s.migrations WHERE schema=$1 AND name=$2 AND done=$3", pq.QuoteIdentifier(s.schema)), schema, name, false)
	if err != nil {
		return err
	}

	rows, err := res.RowsAffected()
	if err != nil {
		return err
	}

	if rows == 0 {
		return fmt.Errorf("no migration found with name %s", name)
	}

	return nil
}
