CREATE SCHEMA IF NOT EXISTS placeholder;

CREATE TABLE IF NOT EXISTS placeholder.migrations
(
    schema           NAME      NOT NULL,
    name             TEXT      NOT NULL,
    migration        JSONB     NOT NULL,
    created_at       TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at       TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,

    parent           TEXT,
    done             BOOLEAN   NOT NULL DEFAULT false,
    resulting_schema JSONB     NOT NULL DEFAULT '{}'::jsonb,

    PRIMARY KEY (schema, name),
    FOREIGN KEY (schema, parent) REFERENCES placeholder.migrations (schema, name)
);

-- Only one migration can be active at a time
CREATE UNIQUE INDEX IF NOT EXISTS only_one_active ON placeholder.migrations (schema, name, done) WHERE done = false;

-- Only first migration can exist without parent
CREATE UNIQUE INDEX IF NOT EXISTS only_first_migration_without_parent ON placeholder.migrations (schema) WHERE parent IS NULL;

-- History is linear
CREATE UNIQUE INDEX IF NOT EXISTS history_is_linear ON placeholder.migrations (schema, parent);

-- Add a column to tell whether the row represents an auto-detected DDL capture or a pgroll migration
ALTER TABLE placeholder.migrations
    ADD COLUMN IF NOT EXISTS migration_type
        VARCHAR(32)
        DEFAULT 'pgroll'
        CONSTRAINT migration_type_check CHECK (migration_type IN ('pgroll', 'inferred')
            );

-- Helper functions

-- Are we in the middle of a migration?
CREATE OR REPLACE FUNCTION placeholder.is_active_migration_period(schemaname NAME) RETURNS boolean
AS
$$
SELECT EXISTS (SELECT 1 FROM placeholder.migrations WHERE schema = schemaname AND done = false)
$$
    LANGUAGE SQL
    STABLE;

-- Get the latest version name (this is the one with child migrations)
CREATE OR REPLACE FUNCTION placeholder.latest_version(schemaname NAME) RETURNS text
    SECURITY DEFINER
    SET search_path = placeholder, pg_catalog, pg_temp
AS
$$
SELECT p.name
FROM placeholder.migrations p
WHERE NOT EXISTS (SELECT 1 FROM placeholder.migrations c WHERE schema = schemaname AND c.parent = p.name)
  AND schema = schemaname
$$
    LANGUAGE SQL
    STABLE;

-- Get the name of the previous version of the schema, or NULL if there is none.
-- This ignores previous versions for which no version schema exists, such as
-- versions corresponding to inferred migrations.
CREATE OR REPLACE FUNCTION placeholder.previous_version(schemaname NAME) RETURNS text
AS
$$
WITH RECURSIVE ancestors AS (SELECT name, schema, parent, migration_type, 0 AS depth
                             FROM placeholder.migrations
                             WHERE name = placeholder.latest_version(schemaname)
                               AND schema = schemaname

                             UNION ALL

                             SELECT m.name, m.schema, m.parent, m.migration_type, a.depth + 1
                             FROM placeholder.migrations m
                                      JOIN ancestors a ON m.name = a.parent AND m.schema = a.schema)
SELECT a.name
FROM ancestors a
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
CREATE OR REPLACE FUNCTION placeholder.read_schema(schemaname text) RETURNS jsonb
    LANGUAGE plpgsql AS
$$
DECLARE
    tables jsonb;
BEGIN
    SELECT json_build_object(
                   'name', schemaname,
                   'tables', (SELECT COALESCE(json_object_agg(t.relname, jsonb_build_object(
                    'name', t.relname,
                    'oid', t.oid,
                    'comment', descr.description,
                    'columns', (SELECT COALESCE(json_object_agg(name, c), '{}'::json)
                                FROM (SELECT attr.attname                                                                                        AS name,
                                             pg_get_expr(def.adbin, def.adrelid)                                                                 AS default,
                                             NOT (
                                                 attr.attnotnull
                                                     OR tp.typtype = 'd'
                                                     AND tp.typnotnull
                                                 )                                                                                               AS nullable,
                                             CASE
                                                 WHEN 'character varying' :: regtype = ANY
                                                      (ARRAY [attr.atttypid, tp.typelem]) THEN REPLACE(
                                                         format_type(attr.atttypid, attr.atttypmod),
                                                         'character varying',
                                                         'varchar'
                                                                                               )
                                                 WHEN 'timestamp with time zone' :: regtype = ANY
                                                      (ARRAY [attr.atttypid, tp.typelem]) THEN REPLACE(
                                                         format_type(attr.atttypid, attr.atttypmod),
                                                         'timestamp with time zone',
                                                         'timestamptz'
                                                                                               )
                                                 ELSE format_type(attr.atttypid, attr.atttypmod)
                                                 END                                                                                             AS type,
                                             descr.description                                                                                   AS comment,
                                             (EXISTS (SELECT 1
                                                      FROM pg_constraint
                                                      WHERE conrelid = attr.attrelid
                                                        AND ARRAY [attr.attnum::int] @> conkey::int[]
                                                        AND contype = 'u') OR EXISTS (SELECT 1
                                                                                      FROM pg_index
                                                                                               JOIN pg_class ON pg_class.oid = pg_index.indexrelid
                                                                                      WHERE indrelid = attr.attrelid
                                                                                        AND indisunique
                                                                                        AND ARRAY [attr.attnum::int] @> pg_index.indkey::int[])) AS unique
                                      FROM pg_attribute AS attr
                                               INNER JOIN pg_type AS tp ON attr.atttypid = tp.oid
                                               LEFT JOIN pg_attrdef AS def ON attr.attrelid = def.adrelid
                                          AND attr.attnum = def.adnum
                                               LEFT JOIN pg_description AS descr ON attr.attrelid = descr.objoid
                                          AND attr.attnum = descr.objsubid
                                      WHERE attr.attnum > 0
                                        AND NOT attr.attisdropped
                                        AND attr.attrelid = t.oid
                                      ORDER BY attr.attnum) c),
                    'primaryKey', (SELECT COALESCE(json_agg(pg_attribute.attname), '[]'::json) AS primary_key_columns
                                   FROM pg_index,
                                        pg_attribute
                                   WHERE indrelid = t.oid
                                     AND nspname = schemaname
                                     AND pg_attribute.attrelid = t.oid
                                     AND pg_attribute.attnum = any (pg_index.indkey)
                                     AND indisprimary),
                    'indexes', (SELECT COALESCE(json_object_agg(ix_details.name, json_build_object(
                            'name', ix_details.name,
                            'unique', ix_details.indisunique,
                            'columns', ix_details.columns,
                            'predicate', ix_details.predicate
                                                                                 )), '{}'::json)
                                FROM (SELECT replace(
                                                     reverse(split_part(reverse(pi.indexrelid::regclass::text), '.', 1)),
                                                     '"', '')               as name,
                                             pi.indisunique,
                                             array_agg(a.attname)           AS columns,
                                             pg_get_expr(pi.indpred, t.oid) AS predicate
                                      FROM pg_index pi
                                               JOIN pg_attribute a
                                                    ON a.attrelid = pi.indrelid AND a.attnum = ANY (pi.indkey)
                                      WHERE indrelid = t.oid::regclass
                                      GROUP BY pi.indexrelid, pi.indisunique) as ix_details),
                    'checkConstraints', (SELECT COALESCE(json_object_agg(cc_details.conname, json_build_object(
                            'name', cc_details.conname,
                            'columns', cc_details.columns,
                            'definition', cc_details.definition
                                                                                             )), '{}'::json)
                                         FROM (SELECT cc_constraint.conname,
                                                      array_agg(cc_attr.attname ORDER BY cc_constraint.conkey::int[]) AS columns,
                                                      pg_get_constraintdef(cc_constraint.oid)                         AS definition
                                               FROM pg_constraint AS cc_constraint
                                                        INNER JOIN pg_attribute cc_attr
                                                                   ON cc_attr.attrelid = cc_constraint.conrelid AND
                                                                      cc_attr.attnum = ANY (cc_constraint.conkey)
                                               WHERE cc_constraint.conrelid = t.oid
                                                 AND cc_constraint.contype = 'c'
                                               GROUP BY cc_constraint.oid, cc_constraint.conname) AS cc_details),
                    'uniqueConstraints', (SELECT COALESCE(json_object_agg(uc_details.conname, json_build_object(
                            'name', uc_details.conname,
                            'columns', uc_details.columns
                                                                                              )), '{}'::json)
                                          FROM (SELECT uc_constraint.conname,
                                                       array_agg(uc_attr.attname ORDER BY uc_constraint.conkey::int[]) AS columns,
                                                       pg_get_constraintdef(uc_constraint.oid)                         AS definition
                                                FROM pg_constraint AS uc_constraint
                                                         INNER JOIN pg_attribute uc_attr
                                                                    ON uc_attr.attrelid = uc_constraint.conrelid AND
                                                                       uc_attr.attnum = ANY (uc_constraint.conkey)
                                                WHERE uc_constraint.conrelid = t.oid
                                                  AND uc_constraint.contype = 'u'
                                                GROUP BY uc_constraint.oid, uc_constraint.conname) AS uc_details),
                    'foreignKeys', (SELECT COALESCE(json_object_agg(fk_details.conname, json_build_object(
                            'name', fk_details.conname,
                            'columns', fk_details.columns,
                            'referencedTable', fk_details.referencedTable,
                            'referencedColumns', fk_details.referencedColumns,
                            'onDelete', fk_details.onDelete
                                                                                        )), '{}'::json)
                                    FROM (SELECT fk_constraint.conname,
                                                 array_agg(fk_attr.attname ORDER BY fk_constraint.conkey::int[])   AS columns,
                                                 fk_cl.relname                                                     AS referencedTable,
                                                 array_agg(ref_attr.attname ORDER BY fk_constraint.confkey::int[]) AS referencedColumns,
                                                 CASE
                                                     WHEN fk_constraint.confdeltype = 'a' THEN 'NO ACTION'
                                                     WHEN fk_constraint.confdeltype = 'r' THEN 'RESTRICT'
                                                     WHEN fk_constraint.confdeltype = 'c' THEN 'CASCADE'
                                                     WHEN fk_constraint.confdeltype = 'd' THEN 'SET DEFAULT'
                                                     WHEN fk_constraint.confdeltype = 'n' THEN 'SET NULL'
                                                     END                                                           as onDelete
                                          FROM pg_constraint AS fk_constraint
                                                   INNER JOIN pg_class fk_cl ON fk_constraint.confrelid = fk_cl.oid
                                                   INNER JOIN pg_attribute fk_attr
                                                              ON fk_attr.attrelid = fk_constraint.conrelid AND
                                                                 fk_attr.attnum = ANY (fk_constraint.conkey)
                                                   INNER JOIN pg_attribute ref_attr
                                                              ON ref_attr.attrelid = fk_constraint.confrelid AND
                                                                 ref_attr.attnum = ANY (fk_constraint.confkey)
                                          WHERE fk_constraint.conrelid = t.oid
                                            AND fk_constraint.contype = 'f'
                                          GROUP BY fk_constraint.conname, fk_cl.relname,
                                                   fk_constraint.confdeltype) AS fk_details)
                                                                         )), '{}'::json)
                              FROM pg_class AS t
                                       INNER JOIN pg_namespace AS ns ON t.relnamespace = ns.oid
                                       LEFT JOIN pg_description AS descr ON t.oid = descr.objoid
                                  AND descr.objsubid = 0
                              WHERE ns.nspname = schemaname
                                AND t.relkind IN ('r', 'p') -- tables only (ignores views, materialized views & foreign tables)
                   )
           )
    INTO tables;

    RETURN tables;
END;
$$;

CREATE OR REPLACE FUNCTION placeholder.raw_migration() RETURNS event_trigger
    LANGUAGE plpgsql
    SECURITY DEFINER
    SET search_path = placeholder, pg_catalog, pg_temp AS
$$
DECLARE
    schemaname   TEXT;
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
        SELECT schema_name
        INTO schemaname
        FROM pg_catalog.pg_event_trigger_dropped_objects()
        WHERE schema_name IS NOT NULL;

    ELSIF tg_event = 'ddl_command_end' THEN
        -- Guess the schema from ddl commands, ignore migrations that touch several schemas
        IF (SELECT pg_catalog.count(DISTINCT schema_name)
            FROM pg_catalog.pg_event_trigger_ddl_commands()
            WHERE schema_name IS NOT NULL) > 1 THEN
            RETURN;
        END IF;

        IF tg_tag = 'CREATE SCHEMA' THEN
            SELECT object_identity INTO schemaname FROM pg_event_trigger_ddl_commands();
        ELSE
            SELECT schema_name
            INTO schemaname
            FROM pg_catalog.pg_event_trigger_ddl_commands()
            WHERE schema_name IS NOT NULL;
        END IF;
    END IF;

    IF schemaname IS NULL THEN
        RETURN;
    END IF;

    -- Ignore migrations done during a migration period
    IF placeholder.is_active_migration_period(schemaname) THEN
        RETURN;
    END IF;

    -- Remove any duplicate inferred migrations with the same timestamp for this
    -- schema. We assume such migrations are multi-statement batched migrations
    -- and we are only interested in the last one in the batch.
    DELETE
    FROM placeholder.migrations
    WHERE schema = schemaname
      AND created_at = current_timestamp
      AND migration_type = 'inferred'
      AND migration -> 'operations' -> 0 -> 'sql' ->> 'up' = current_query();

-- Someone did a schema change without pgroll, include it in the history
    SELECT INTO migration_id pg_catalog.format('sql_%s',
                                               pg_catalog.substr(pg_catalog.md5(pg_catalog.random()::text), 0, 15));

    INSERT INTO placeholder.migrations (schema, name, migration, resulting_schema, done, parent, migration_type,
                                        created_at, updated_at)
    VALUES (schemaname,
            migration_id,
            pg_catalog.json_build_object(
                    'name', migration_id,
                    'operations', (SELECT pg_catalog.json_agg(
                                                  pg_catalog.json_build_object(
                                                          'sql', pg_catalog.json_build_object(
                                                          'up', pg_catalog.current_query()
                                                                 )
                                                  )
                                          ))
            ),
            placeholder.read_schema(schemaname),
            true,
            placeholder.latest_version(schemaname),
            'inferred',
            statement_timestamp(),
            statement_timestamp());
END;
$$;

DROP EVENT TRIGGER IF EXISTS pg_roll_handle_ddl;
CREATE EVENT TRIGGER pg_roll_handle_ddl ON ddl_command_end
EXECUTE FUNCTION placeholder.raw_migration();

DROP EVENT TRIGGER IF EXISTS pg_roll_handle_drop;
CREATE EVENT TRIGGER pg_roll_handle_drop ON sql_drop
EXECUTE FUNCTION placeholder.raw_migration();