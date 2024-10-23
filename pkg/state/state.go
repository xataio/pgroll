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

-- Add a column to tell whether the row represents an auto-detected DDL capture or a pgroll migration
ALTER TABLE %[1]s.migrations ADD COLUMN IF NOT EXISTS migration_type
  VARCHAR(32)
  DEFAULT 'pgroll'
  CONSTRAINT migration_type_check CHECK (migration_type IN ('pgroll', 'inferred')
);

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
-- This ignores previous versions for which no version schema exists, such as
-- versions corresponding to inferred migrations.
CREATE OR REPLACE FUNCTION %[1]s.previous_version(schemaname NAME) RETURNS text
AS $$
  WITH RECURSIVE ancestors AS (
  SELECT name, schema, parent, migration_type, 0 AS depth FROM %[1]s.migrations
    WHERE name = %[1]s.latest_version(schemaname) AND schema = schemaname

  UNION ALL

  SELECT m.name, m.schema, m.parent, m.migration_type, a.depth + 1
    FROM %[1]s.migrations m
    JOIN ancestors a ON m.name = a.parent AND m.schema = a.schema
  )
  SELECT a.name FROM ancestors a
    JOIN information_schema.schemata s
    ON s.schema_name = schemaname || '_' || a.name
    WHERE migration_type = 'pgroll'
    AND a.depth > 0
    ORDER by a.depth ASC
    LIMIT 1;
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
			SELECT COALESCE(json_object_agg(t.relname, jsonb_build_object(
				'name', t.relname,
				'oid', t.oid,
				'comment', descr.description,
				'columns', (
					SELECT COALESCE(json_object_agg(name, c), '{}'::json) FROM (
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
							descr.description AS comment,
							(EXISTS (
								SELECT 1
								FROM pg_constraint
								WHERE conrelid = attr.attrelid
								AND ARRAY[attr.attnum::int] @> conkey::int[]
								AND contype = 'u'
							) OR EXISTS (
								SELECT 1
								FROM pg_index
								JOIN pg_class ON pg_class.oid = pg_index.indexrelid
								WHERE indrelid = attr.attrelid
								AND indisunique
								AND ARRAY[attr.attnum::int] @> pg_index.indkey::int[]
							)) AS unique
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
					SELECT COALESCE(json_agg(pg_attribute.attname), '[]'::json) AS primary_key_columns
					FROM pg_index, pg_attribute
					WHERE
						indrelid = t.oid AND
						nspname = schemaname AND
						pg_attribute.attrelid = t.oid AND
						pg_attribute.attnum = any(pg_index.indkey)
						AND indisprimary
				),
				'indexes', (
				  SELECT COALESCE(json_object_agg(ix_details.name, json_build_object(
				    'name', ix_details.name,
				    'unique', ix_details.indisunique,
				    'columns', ix_details.columns,
				    'predicate', ix_details.predicate
				  )), '{}'::json)
				  FROM (
				    SELECT 
				      replace(reverse(split_part(reverse(pi.indexrelid::regclass::text), '.', 1)), '"', '') as name,
				      pi.indisunique,
				      array_agg(a.attname) AS columns,
				      pg_get_expr(pi.indpred, t.oid) AS predicate
				    FROM pg_index pi
				    JOIN pg_attribute a ON a.attrelid = pi.indrelid AND a.attnum = ANY(pi.indkey)
				    WHERE indrelid = t.oid::regclass
				    GROUP BY pi.indexrelid, pi.indisunique
				  ) as ix_details
				),
				'checkConstraints', (
					SELECT COALESCE(json_object_agg(cc_details.conname, json_build_object(
						'name', cc_details.conname,
						'columns', cc_details.columns,
						'definition', cc_details.definition
					)), '{}'::json)
					FROM (
						SELECT
							cc_constraint.conname,
							array_agg(cc_attr.attname ORDER BY cc_constraint.conkey::int[]) AS columns,
							pg_get_constraintdef(cc_constraint.oid) AS definition
						FROM pg_constraint AS cc_constraint
						INNER JOIN pg_attribute cc_attr ON cc_attr.attrelid = cc_constraint.conrelid AND cc_attr.attnum = ANY(cc_constraint.conkey)
						WHERE cc_constraint.conrelid = t.oid
						AND cc_constraint.contype = 'c'
						GROUP BY cc_constraint.oid, cc_constraint.conname
					) AS cc_details
        ),
				'uniqueConstraints', (
					SELECT COALESCE(json_object_agg(uc_details.conname, json_build_object(
						'name', uc_details.conname,
						'columns', uc_details.columns
					)), '{}'::json)
					FROM (
						SELECT
							uc_constraint.conname,
							array_agg(uc_attr.attname ORDER BY uc_constraint.conkey::int[]) AS columns,
							pg_get_constraintdef(uc_constraint.oid) AS definition
						FROM pg_constraint AS uc_constraint
						INNER JOIN pg_attribute uc_attr ON uc_attr.attrelid = uc_constraint.conrelid AND uc_attr.attnum = ANY(uc_constraint.conkey)
						WHERE uc_constraint.conrelid = t.oid
						AND uc_constraint.contype = 'u'
						GROUP BY uc_constraint.oid, uc_constraint.conname
					) AS uc_details
        ),
				'foreignKeys', (
					SELECT COALESCE(json_object_agg(fk_details.conname, json_build_object(
						'name', fk_details.conname,
						'columns', fk_details.columns,
						'referencedTable', fk_details.referencedTable,
						'referencedColumns', fk_details.referencedColumns,
						'onDelete', fk_details.onDelete
					)), '{}'::json)
					FROM (
						SELECT
							fk_constraint.conname,
							array_agg(fk_attr.attname ORDER BY fk_constraint.conkey::int[]) AS columns,
							fk_cl.relname AS referencedTable,
							array_agg(ref_attr.attname ORDER BY fk_constraint.confkey::int[]) AS referencedColumns,
							CASE
							  WHEN fk_constraint.confdeltype = 'a' THEN 'NO ACTION'
							  WHEN fk_constraint.confdeltype = 'r' THEN 'RESTRICT'
							  WHEN fk_constraint.confdeltype = 'c' THEN 'CASCADE'
							  WHEN fk_constraint.confdeltype = 'd' THEN 'SET DEFAULT'
							  WHEN fk_constraint.confdeltype = 'n' THEN 'SET NULL'
							END as onDelete
						FROM pg_constraint AS fk_constraint
						INNER JOIN pg_class fk_cl ON fk_constraint.confrelid = fk_cl.oid
						INNER JOIN pg_attribute fk_attr ON fk_attr.attrelid = fk_constraint.conrelid AND fk_attr.attnum = ANY(fk_constraint.conkey)
						INNER JOIN pg_attribute ref_attr ON ref_attr.attrelid = fk_constraint.confrelid AND ref_attr.attnum = ANY(fk_constraint.confkey)
						WHERE fk_constraint.conrelid = t.oid
						AND fk_constraint.contype = 'f'
						GROUP BY fk_constraint.conname, fk_cl.relname, fk_constraint.confdeltype
					) AS fk_details
				)
			)), '{}'::json) FROM pg_class AS t
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
	migration_id TEXT;
BEGIN
	-- Ignore migrations done by pgroll
	IF (pg_catalog.current_setting('pgroll.internal', 'TRUE') <> 'TRUE') THEN
		RETURN;
	END IF;

  IF tg_event = 'sql_drop' AND tg_tag = 'DROP SCHEMA' THEN
    -- Take the schema name from the drop schema command
    SELECT object_identity INTO schemaname FROM pg_event_trigger_dropped_objects();

  ELSIF tg_event = 'sql_drop' and tg_tag != 'ALTER TABLE' THEN
		-- Guess the schema from drop commands
		SELECT schema_name INTO schemaname FROM pg_catalog.pg_event_trigger_dropped_objects() WHERE schema_name IS NOT NULL;

	ELSIF tg_event = 'ddl_command_end' THEN
		-- Guess the schema from ddl commands, ignore migrations that touch several schemas
		IF (SELECT pg_catalog.count(DISTINCT schema_name) FROM pg_catalog.pg_event_trigger_ddl_commands() WHERE schema_name IS NOT NULL) > 1 THEN
			RETURN;
		END IF;

		IF tg_tag = 'CREATE SCHEMA' THEN
			SELECT object_identity INTO schemaname FROM pg_event_trigger_ddl_commands();
		ELSE
			SELECT schema_name INTO schemaname FROM pg_catalog.pg_event_trigger_ddl_commands() WHERE schema_name IS NOT NULL;
		END IF;
	END IF;

	IF schemaname IS NULL THEN
		RETURN;
	END IF;

	-- Ignore migrations done during a migration period
	IF %[1]s.is_active_migration_period(schemaname) THEN
		RETURN;
	END IF;

	-- Remove any duplicate inferred migrations with the same timestamp for this
	-- schema. We assume such migrations are multi-statement batched migrations
	-- and we are only interested in the last one in the batch.
	DELETE FROM %[1]s.migrations
	WHERE schema = schemaname
	AND created_at = current_timestamp
	AND migration_type = 'inferred'
	AND migration->'operations'->0->'sql'->>'up' = current_query();

	-- Someone did a schema change without pgroll, include it in the history
	SELECT INTO migration_id pg_catalog.format('sql_%%s',pg_catalog.substr(pg_catalog.md5(pg_catalog.random()::text), 0, 15));

	INSERT INTO %[1]s.migrations (schema, name, migration, resulting_schema, done, parent, migration_type, created_at, updated_at)
	VALUES (
		schemaname,
		migration_id,
		pg_catalog.json_build_object(
			'name', migration_id,
			'operations', (
				SELECT pg_catalog.json_agg(
					pg_catalog.json_build_object(
						'sql', pg_catalog.json_build_object(
							'up', pg_catalog.current_query()
						)
					)
				)
			)
		),
		%[1]s.read_schema(schemaname),
		true,
		%[1]s.latest_version(schemaname),
		'inferred',
		statement_timestamp(),
		statement_timestamp()
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
	dsn, err := pq.ParseURL(pgURL)
	if err != nil {
		dsn = pgURL
	}

	dsn += " search_path=" + stateSchema

	conn, err := sql.Open("postgres", dsn)
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

// Init initializes the required pg_roll schema to store the state
func (s *State) Init(ctx context.Context) error {
	tx, err := s.pgConn.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()

	// Try to obtain an advisory lock.
	// The key is an arbitrary number, used to distinguish the lock from other locks.
	// The lock is automatically released when the transaction is committed or rolled back.
	const key int64 = 0x2c03057fb9525b
	_, err = tx.ExecContext(ctx, "SELECT pg_advisory_xact_lock($1)", key)
	if err != nil {
		return err
	}

	// Perform pgroll state initialization
	_, err = tx.ExecContext(ctx, fmt.Sprintf(sqlInit, pq.QuoteIdentifier(s.schema), pq.QuoteLiteral(s.schema)))
	if err != nil {
		return err
	}

	return tx.Commit()
}

func (s *State) Close() error {
	return s.pgConn.Close()
}

// Schema returns the schema name
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

// Status returns the current migration status of the specified schema
func (s *State) Status(ctx context.Context, schema string) (*Status, error) {
	latestVersion, err := s.LatestVersion(ctx, schema)
	if err != nil {
		return nil, err
	}
	if latestVersion == nil {
		latestVersion = new(string)
	}

	isActive, err := s.IsActiveMigrationPeriod(ctx, schema)
	if err != nil {
		return nil, err
	}

	var status MigrationStatus
	if *latestVersion == "" {
		status = NoneMigrationStatus
	} else if isActive {
		status = InProgressMigrationStatus
	} else {
		status = CompleteMigrationStatus
	}

	return &Status{
		Schema:  schema,
		Version: *latestVersion,
		Status:  status,
	}, nil
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

	var unmarshalledSchema schema.Schema
	err = json.Unmarshal([]byte(rawSchema), &unmarshalledSchema)
	if err != nil {
		return nil, fmt.Errorf("unable to unmarshal schema: %w", err)
	}

	return &unmarshalledSchema, nil
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

// ReadSchema reads the schema for the specified schema name
func (s *State) ReadSchema(ctx context.Context, schemaName string) (*schema.Schema, error) {
	var rawSchema []byte
	err := s.pgConn.QueryRowContext(ctx, fmt.Sprintf("SELECT %s.read_schema($1)", pq.QuoteIdentifier(s.schema)), schemaName).Scan(&rawSchema)
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
