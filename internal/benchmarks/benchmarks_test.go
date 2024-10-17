// SPDX-License-Identifier: Apache-2.0

package benchmarks

import (
	"context"
	"database/sql"
	"strconv"
	"testing"
	"time"

	"github.com/lib/pq"
	"github.com/oapi-codegen/nullable"
	"github.com/stretchr/testify/require"

	"github.com/xataio/pgroll/pkg/migrations"
	"github.com/xataio/pgroll/pkg/roll"
	"github.com/xataio/pgroll/pkg/testutils"
)

func TestMain(m *testing.M) {
	testutils.SharedTestMain(m)
}

func BenchmarkBackfill(b *testing.B) {
	// TODO: Run against different Postgres versions

	ctx := context.Background()
	testSchema := testutils.TestSchema()
	var opts []roll.Option

	migCreateTable := migrations.Migration{
		Name: "01_create_table",
		Operations: migrations.Operations{
			&migrations.OpCreateTable{
				Name: "users",
				Columns: []migrations.Column{
					{
						Name: "id",
						Type: "serial",
						Pk:   ptr(true),
					},
					{
						Name:     "name",
						Type:     "varchar(255)",
						Nullable: ptr(true),
						Unique:   ptr(false),
					},
				},
			},
		},
	}

	migAlterColumn := migrations.Migration{
		Name: "02_alter_column",
		Operations: migrations.Operations{
			&migrations.OpAlterColumn{
				Table:    "users",
				Column:   "name",
				Up:       "(SELECT CASE WHEN name IS NULL THEN 'placeholder' ELSE name END)",
				Down:     "user_name",
				Comment:  nullable.NewNullableWithValue("the name of the user"),
				Nullable: ptr(false),
			},
		},
	}

	seed := func(b *testing.B, rowCount int, db *sql.DB) {
		seedStart := time.Now()
		defer func() {
			b.Logf("Seeded %d rows in %s", rowCount, time.Since(seedStart))
		}()

		tx, err := db.Begin()
		require.NoError(b, err)
		defer tx.Rollback()

		stmt, err := tx.PrepareContext(ctx, pq.CopyInSchema(testSchema, "users", "name"))
		require.NoError(b, err)

		for i := 0; i < rowCount; i++ {
			_, err = stmt.ExecContext(ctx, nil)
			require.NoError(b, err)
		}

		_, err = stmt.ExecContext(ctx)
		require.NoError(b, err)
		require.NoError(b, tx.Commit())
	}

	for _, rowCount := range []int{10_000, 100_000, 1_000_000} {
		b.Run(strconv.Itoa(rowCount), func(b *testing.B) {
			testutils.WithMigratorInSchemaAndConnectionToContainerWithOptions(b, testSchema, opts, func(mig *roll.Roll, db *sql.DB) {
				// Setup
				require.NoError(b, mig.Start(ctx, &migCreateTable))
				require.NoError(b, mig.Complete(ctx))
				seed(b, rowCount, db)
				b.ResetTimer()

				// Backfill
				b.StartTimer()
				require.NoError(b, mig.Start(ctx, &migAlterColumn))
				require.NoError(b, mig.Complete(ctx))
				b.StopTimer()
				b.Logf("Backfilled %d rows in %s", rowCount, b.Elapsed())
				rowsPerSecond := float64(rowCount) / b.Elapsed().Seconds()
				b.ReportMetric(rowsPerSecond, "rows/s")
				require.NoError(b, mig.Close())
			})
		})
	}
}

func ptr[T any](x T) *T { return &x }
