// SPDX-License-Identifier: Apache-2.0

package flags

import (
	"time"

	"github.com/spf13/viper"
)

func PostgresURL() string {
	return viper.GetString("PG_URL")
}

func Schema() string {
	return viper.GetString("SCHEMA")
}

func StateSchema() string {
	return viper.GetString("STATE_SCHEMA")
}

func LockTimeout() int {
	return viper.GetInt("LOCK_TIMEOUT")
}

func BackfillBatchSize() int { return viper.GetInt("BACKFILL_BATCH_SIZE") }

func BackfillBatchDelay() time.Duration { return viper.GetDuration("BACKFILL_BATCH_DELAY") }

func SkipValidation() bool { return viper.GetBool("SKIP_VALIDATION") }

func Role() string {
	return viper.GetString("ROLE")
}
