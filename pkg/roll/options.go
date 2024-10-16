// SPDX-License-Identifier: Apache-2.0

package roll

import (
	"time"

	"github.com/xataio/pgroll/pkg/migrations"
)

type options struct {
	// lock timeout in milliseconds for pgroll DDL operations
	lockTimeoutMs int

	// optional role to set before executing migrations
	role string

	// optional SQL transformer to apply to all user-defined SQL statements
	sqlTransformer migrations.SQLTransformer

	// disable pgroll version schemas creation and deletion
	disableVersionSchemas bool

	// disable creation of version schema for raw SQL migrations
	noVersionSchemaForRawSQL bool

	// additional entries to add to the search_path during migration execution
	searchPath []string

	// the number of rows to backfill in each batch
	backfillBatchSize int

	// the duration to delay after each batch is run
	backfillBatchDelay time.Duration

	migrationHooks MigrationHooks
}

// MigrationHooks defines hooks that can be set to be called at various points
// during the migration process
type MigrationHooks struct {
	// BeforeStartDDL is called before the DDL phase of migration start
	BeforeStartDDL func(*Roll) error
	// AfterStartDDL is called after the DDL phase of migration start is complete
	AfterStartDDL func(*Roll) error
	// BeforeCompleteDDL is called before the DDL phase of migration complete
	BeforeCompleteDDL func(*Roll) error
	// AfterCompleteDDL is called after the DDL phase of migration complete is complete
	AfterCompleteDDL func(*Roll) error
}

type Option func(*options)

// WithLockTimeoutMs sets the lock timeout in milliseconds for pgroll DDL operations
func WithLockTimeoutMs(lockTimeoutMs int) Option {
	return func(o *options) {
		o.lockTimeoutMs = lockTimeoutMs
	}
}

// WithRole sets the role to set before executing migrations
func WithRole(role string) Option {
	return func(o *options) {
		o.role = role
	}
}

// WithDisableViewsManagement disables pgroll version schemas management
// when passed, pgroll will not create or drop version schemas
func WithDisableViewsManagement() Option {
	return func(o *options) {
		o.disableVersionSchemas = true
	}
}

// WithNoVersionSchemaForRawSQL disables the creation of version schema for raw SQL migrations
func WithNoVersionSchemaForRawSQL() Option {
	return func(o *options) {
		o.noVersionSchemaForRawSQL = true
	}
}

// WithMigrationHooks sets the migration hooks for the Roll instance
// Migration hooks are called at various points during the migration process
// to allow for custom behavior to be injected
func WithMigrationHooks(hooks MigrationHooks) Option {
	return func(o *options) {
		o.migrationHooks = hooks
	}
}

// WithSQLTransformer sets the SQL transformer to apply to all user-defined SQL
// statements before they are executed.
// This is useful to sanitize or modify user defined SQL statements before they
// are executed.
func WithSQLTransformer(transformer migrations.SQLTransformer) Option {
	return func(o *options) {
		o.sqlTransformer = transformer
	}
}

// WithSearchPath sets the search_path to use during migration execution. The
// schema in which the migration is run is always included in the search path,
// regardless of this setting.
func WithSearchPath(schemas ...string) Option {
	return func(o *options) {
		o.searchPath = schemas
	}
}

// WithBackfillBatchSize sets the number of rows backfilled in each batch.
func WithBackfillBatchSize(batchSize int) Option {
	return func(o *options) {
		o.backfillBatchSize = batchSize
	}
}

// WithBackfillBatchDelay sets the delay after each batch is run.
func WithBackfillBatchDelay(delay time.Duration) Option {
	return func(o *options) {
		o.backfillBatchDelay = delay
	}
}
