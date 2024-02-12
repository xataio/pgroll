// SPDX-License-Identifier: Apache-2.0

package migrations

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/xataio/pgroll/pkg/schema"
)

func TestMigrationsIsolated(t *testing.T) {
	migration := Migration{
		Name: "sql",
		Operations: Operations{
			&OpRawSQL{
				Up: `foo`,
			},
			&OpCreateTable{Name: "foo"},
		},
	}

	err := migration.Validate(context.TODO(), schema.New())
	var wantErr InvalidMigrationError
	assert.ErrorAs(t, err, &wantErr)
}

func TestMigrationsIsolatedValid(t *testing.T) {
	migration := Migration{
		Name: "sql",
		Operations: Operations{
			&OpRawSQL{
				Up: `foo`,
			},
		},
	}
	err := migration.Validate(context.TODO(), schema.New())
	assert.NoError(t, err)

	// Test onComplete
	migration = Migration{
		Name: "sql",
		Operations: Operations{
			&OpRawSQL{
				Up:         `foo`,
				OnComplete: true,
			},
			&OpCreateTable{Name: "foo"},
		},
	}
	err = migration.Validate(context.TODO(), schema.New())
	assert.NoError(t, err)
}
