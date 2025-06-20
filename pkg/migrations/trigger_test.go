// SPDX-License-Identifier: Apache-2.0

package migrations

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/xataio/pgroll/pkg/backfill"
	"github.com/xataio/pgroll/pkg/schema"
)

func TestBuildFunction(t *testing.T) {
	testCases := []struct {
		name     string
		config   backfill.TriggerConfig
		expected string
	}{
		{
			name: "simple up trigger",
			config: backfill.TriggerConfig{
				Name:      "triggerName",
				Direction: backfill.TriggerDirectionUp,
				Columns: map[string]*schema.Column{
					"id":       {Name: "id", Type: "int"},
					"username": {Name: "username", Type: "text"},
					"product":  {Name: "product", Type: "text"},
					"review":   {Name: "review", Type: "text"},
				},
				SchemaName:          "public",
				LatestSchema:        "public_01_migration_name",
				TableName:           "reviews",
				PhysicalColumn:      "_pgroll_new_review",
				NeedsBackfillColumn: backfill.CNeedsBackfillColumn,
				SQL:                 "product || 'is good'",
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
      SELECT current_setting
        INTO search_path
        FROM current_setting('search_path');

      IF search_path != 'public_01_migration_name' THEN
        NEW."_pgroll_new_review" = product || 'is good';
        NEW."_pgroll_needs_backfill" = false;
      END IF;

      RETURN NEW;
    END; $$
`,
		},
		{
			name: "simple down trigger",
			config: backfill.TriggerConfig{
				Name:      "triggerName",
				Direction: backfill.TriggerDirectionDown,
				Columns: map[string]*schema.Column{
					"id":       {Name: "id", Type: "int"},
					"username": {Name: "username", Type: "text"},
					"product":  {Name: "product", Type: "text"},
					"review":   {Name: "review", Type: "text"},
				},
				SchemaName:          "public",
				LatestSchema:        "public_01_migration_name",
				TableName:           "reviews",
				PhysicalColumn:      "review",
				NeedsBackfillColumn: backfill.CNeedsBackfillColumn,
				SQL:                 `NEW."_pgroll_new_review"`,
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
      SELECT current_setting
        INTO search_path
        FROM current_setting('search_path');

      IF search_path = 'public_01_migration_name' THEN
        NEW."review" = NEW."_pgroll_new_review";
        NEW."_pgroll_needs_backfill" = false;
      END IF;

      RETURN NEW;
    END; $$
`,
		},
		{
			name: "down trigger with aliased column",
			config: backfill.TriggerConfig{
				Name:      "triggerName",
				Direction: backfill.TriggerDirectionDown,
				Columns: map[string]*schema.Column{
					"id":       {Name: "id", Type: "int"},
					"username": {Name: "username", Type: "text"},
					"product":  {Name: "product", Type: "text"},
					"review":   {Name: "review", Type: "text"},
					"rating":   {Name: "_pgroll_new_rating", Type: "integer"},
				},
				SchemaName:          "public",
				LatestSchema:        "public_01_migration_name",
				TableName:           "reviews",
				PhysicalColumn:      "rating",
				NeedsBackfillColumn: backfill.CNeedsBackfillColumn,
				SQL:                 `CAST(rating as text)`,
			},
			expected: `CREATE OR REPLACE FUNCTION "triggerName"()
    RETURNS TRIGGER
    LANGUAGE PLPGSQL
    AS $$
    DECLARE
      "id" "public"."reviews"."id"%TYPE := NEW."id";
      "product" "public"."reviews"."product"%TYPE := NEW."product";
      "rating" "public"."reviews"."_pgroll_new_rating"%TYPE := NEW."_pgroll_new_rating";
      "review" "public"."reviews"."review"%TYPE := NEW."review";
      "username" "public"."reviews"."username"%TYPE := NEW."username";
      latest_schema text;
      search_path text;
    BEGIN
      SELECT current_setting
        INTO search_path
        FROM current_setting('search_path');

      IF search_path = 'public_01_migration_name' THEN
        NEW."rating" = CAST(rating as text);
        NEW."_pgroll_needs_backfill" = false;
      END IF;

      RETURN NEW;
    END; $$
`,
		},
	}

	for _, tc := range testCases {
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
		config   backfill.TriggerConfig
		expected string
	}{
		{
			name: "trigger",
			config: backfill.TriggerConfig{
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
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			sql, err := buildTrigger(tc.config)
			assert.NoError(t, err)
			assert.Equal(t, tc.expected, sql)
		})
	}
}
