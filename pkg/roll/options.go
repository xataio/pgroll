// SPDX-License-Identifier: Apache-2.0

package roll

type options struct {
	// lock timeout in milliseconds for pgroll DDL operations
	lockTimeoutMs int

	// optional role to set before executing migrations
	role string

	// disable pgroll version schemas creation and deletion
	disableVersionSchemas bool

	// disable creation of version schema for raw SQL migrations
	noVersionSchemaForRawSQL bool

	// additional entries to add to the search_path during migration execution
	searchPath []string

	// whether to skip validation
	skipValidation bool

	migrationHooks MigrationHooks

	verbose bool
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

// WithSearchPath sets the search_path to use during migration execution. The
// schema in which the migration is run is always included in the search path,
// regardless of this setting.
func WithSearchPath(schemas ...string) Option {
	return func(o *options) {
		o.searchPath = schemas
	}
}

// WithSkipValidation controls whether or not to perform validation on
// migrations. If set to true, validation will be skipped.
func WithSkipValidation(skip bool) Option {
	return func(o *options) {
		o.skipValidation = skip
	}
}

func WithLogging(enabled bool) Option {
	return func(o *options) {
		if enabled {
			o.verbose = enabled
		}
	}
}
