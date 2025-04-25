// SPDX-License-Identifier: Apache-2.0

package flags

import (
	"github.com/spf13/cobra"
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

func SkipValidation() bool { return viper.GetBool("SKIP_VALIDATION") }

func Role() string {
	return viper.GetString("ROLE")
}

func PgConnectionFlags(cmd *cobra.Command) {
	cmd.PersistentFlags().String("postgres-url", "postgres://postgres:postgres@localhost?sslmode=disable", "Postgres URL")
	cmd.PersistentFlags().String("schema", "public", "Postgres schema to use for the migration")
	cmd.PersistentFlags().String("pgroll-schema", "pgroll", "Postgres schema to use for pgroll internal state")
	cmd.PersistentFlags().Int("lock-timeout", 500, "Postgres lock timeout in milliseconds for pgroll DDL operations")
	cmd.PersistentFlags().String("role", "", "Optional postgres role to set when executing migrations")

	viper.BindPFlag("PG_URL", cmd.PersistentFlags().Lookup("postgres-url"))
	viper.BindPFlag("SCHEMA", cmd.PersistentFlags().Lookup("schema"))
	viper.BindPFlag("STATE_SCHEMA", cmd.PersistentFlags().Lookup("pgroll-schema"))
	viper.BindPFlag("LOCK_TIMEOUT", cmd.PersistentFlags().Lookup("lock-timeout"))
	viper.BindPFlag("ROLE", cmd.PersistentFlags().Lookup("role"))
}
