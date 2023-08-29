package templates

const Function = `CREATE OR REPLACE FUNCTION {{ .Name | qi }}()
    RETURNS TRIGGER
    LANGUAGE PLPGSQL
    AS $$
    DECLARE
      {{- $schemaName := .SchemaName  }}
      {{- $tableName := .TableName  }}
      {{ range .Columns }} 
      {{- .Name | qi }} {{ $schemaName | qi }}.{{ $tableName | qi}}.{{ .Name | qi }}%TYPE := NEW.{{ .Name | qi }};
      {{ end -}}
      latest_schema text;
      search_path text;
    BEGIN
      SELECT {{ .SchemaName | ql }} || '_' || latest_version
        INTO latest_schema
        FROM {{ .StateSchema | qi }}.latest_version({{ .SchemaName | ql }});

      SELECT current_setting
        INTO search_path
        FROM current_setting('search_path');

      IF search_path {{- if eq .Direction "up" }} != {{- else }} = {{ end }} latest_schema {{ if .TestExpr  -}} AND {{ .TestExpr }} {{ end -}} THEN
        NEW.{{ .PhysicalColumn | qi  }} = {{ .SQL }};
      {{- if .ElseExpr }}
      ELSE
        {{ .ElseExpr }};
      {{- end }}
      END IF;

      RETURN NEW;
    END; $$
`
