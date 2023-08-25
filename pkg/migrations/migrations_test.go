package migrations

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/xataio/pg-roll/pkg/schema"
)

func TestMigrationsIsolated(t *testing.T) {
	migration := Migration{
		Name: "sql",
		Operations: Operations{
			&OpRawSQL{
				Up: `foo`,
			},
			&OpRenameColumn{},
		},
	}

	err := migration.Validate(context.TODO(), schema.New())
	var wantErr InvalidMigrationError
	assert.ErrorAs(t, err, &wantErr)
	assert.Equal(t, wantErr.Reason, "operation \"sql\" cannot be executed with other operations")

	migration = Migration{
		Name: "sql",
		Operations: Operations{
			&OpRawSQL{
				Up: `foo`,
			},
		},
	}
	err = migration.Validate(context.TODO(), schema.New())
	assert.NoError(t, err)
}
