package migrations

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/xataio/pg-roll/pkg/schema"
)

func TestBuildFunction(t *testing.T) {
	testCases := []struct {
		name     string
		config   triggerConfig
		expected string
	}{
		{
			name: "simple up trigger",
			config: triggerConfig{
				Name:      "triggerName",
				Direction: TriggerDirectionUp,
				Columns: map[string]schema.Column{
					"id":       {Name: "id", Type: "int"},
					"username": {Name: "username", Type: "text"},
					"product":  {Name: "product", Type: "text"},
					"review":   {Name: "review", Type: "text"},
				},
				SchemaName:     "public",
				TableName:      "reviews",
				StateSchema:    "pgroll",
				PhysicalColumn: "review",
				SQL:            `NEW."_pgroll_new_review"`,
			},
			expected: `CREATE OR REPLACE FUNCTION "triggerName"()
    RETURNS TRIGGER
    LANGUAGE PLPGSQL
    AS $$
    DECLARE
      "id" "public"."reviews"."id"%TYPE := NEW."id";
      "product" "public"."reviews"."product"%TYPE := NEW."product";
      "review" "public"."reviews"."review"%TYPE := NEW."review";
      "username" "public"."reviews"."username"%TYPE := NEW."username";
      latest_schema text;
      search_path text;
    BEGIN
      SELECT 'public' || '_' || latest_version
        INTO latest_schema
        FROM "pgroll".latest_version('public');

      SELECT current_setting
        INTO search_path
        FROM current_setting('search_path');

      IF search_path != latest_schema THEN
        NEW."review" = NEW."_pgroll_new_review";
      END IF;

      RETURN NEW;
    END; $$
`,
		},
		{
			name: "complete up trigger",
			config: triggerConfig{
				Name:      "triggerName",
				Direction: TriggerDirectionUp,
				Columns: map[string]schema.Column{
					"id":       {Name: "id", Type: "int"},
					"username": {Name: "username", Type: "text"},
					"product":  {Name: "product", Type: "text"},
					"review":   {Name: "review", Type: "text"},
				},
				SchemaName:     "public",
				TableName:      "reviews",
				StateSchema:    "pgroll",
				TestExpr:       `NEW."review" IS NULL`,
				PhysicalColumn: "_pgroll_new_review",
				ElseExpr:       `NEW."_pgroll_new_review" = NEW."review"`,
				SQL:            "product || 'is good'",
			},
			expected: `CREATE OR REPLACE FUNCTION "triggerName"()
    RETURNS TRIGGER
    LANGUAGE PLPGSQL
    AS $$
    DECLARE
      "id" "public"."reviews"."id"%TYPE := NEW."id";
      "product" "public"."reviews"."product"%TYPE := NEW."product";
      "review" "public"."reviews"."review"%TYPE := NEW."review";
      "username" "public"."reviews"."username"%TYPE := NEW."username";
      latest_schema text;
      search_path text;
    BEGIN
      SELECT 'public' || '_' || latest_version
        INTO latest_schema
        FROM "pgroll".latest_version('public');

      SELECT current_setting
        INTO search_path
        FROM current_setting('search_path');

      IF search_path != latest_schema AND NEW."review" IS NULL THEN
        NEW."_pgroll_new_review" = product || 'is good';
      ELSE
        NEW."_pgroll_new_review" = NEW."review";
      END IF;

      RETURN NEW;
    END; $$
`,
		},
	}

	for _, tc := range testCases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			sql, err := buildFunction(tc.config)
			assert.NoError(t, err)
			assert.Equal(t, tc.expected, sql)
		})
	}
}

func TestBuildTrigger(t *testing.T) {
	testCases := []struct {
		name     string
		config   triggerConfig
		expected string
	}{
		{
			name: "trigger",
			config: triggerConfig{
				Name:      "triggerName",
				TableName: "reviews",
			},
			expected: `CREATE OR REPLACE TRIGGER "triggerName"
    BEFORE UPDATE OR INSERT
    ON "reviews"
    FOR EACH ROW
    EXECUTE PROCEDURE "triggerName"();
`,
		},
	}

	for _, tc := range testCases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			sql, err := buildTrigger(tc.config)
			assert.NoError(t, err)
			assert.Equal(t, tc.expected, sql)
		})
	}
}
