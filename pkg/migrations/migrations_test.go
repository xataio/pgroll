// SPDX-License-Identifier: Apache-2.0

package migrations_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/xataio/pgroll/pkg/migrations"
	"github.com/xataio/pgroll/pkg/schema"
)

func TestMigrationsIsolated(t *testing.T) {
	t.Parallel()

	migration := migrations.Migration{
		Name: "sql",
		Operations: migrations.Operations{
			&migrations.OpRawSQL{
				Up: `foo`,
			},
			&migrations.OpCreateTable{Name: "foo"},
		},
	}

	err := migration.Validate(context.TODO(), schema.New())
	var wantErr migrations.InvalidMigrationError
	assert.ErrorAs(t, err, &wantErr)
}

func TestMigrationsIsolatedValid(t *testing.T) {
	t.Parallel()

	migration := migrations.Migration{
		Name: "sql",
		Operations: migrations.Operations{
			&migrations.OpRawSQL{
				Up: `foo`,
			},
		},
	}
	err := migration.Validate(context.TODO(), schema.New())
	assert.NoError(t, err)
}

func TestOnCompleteSQLMigrationsAreNotIsolated(t *testing.T) {
	t.Parallel()

	migration := migrations.Migration{
		Name: "sql",
		Operations: migrations.Operations{
			&migrations.OpRawSQL{
				Up:         `foo`,
				OnComplete: true,
			},
			&migrations.OpCreateTable{Name: "foo"},
		},
	}
	err := migration.Validate(context.TODO(), schema.New())
	assert.NoError(t, err)
}
