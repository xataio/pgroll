// SPDX-License-Identifier: Apache-2.0

package roll

type options struct {
	// lock timeout in milliseconds for pgroll DDL operations
	lockTimeoutMs int

	// optional role to set before executing migrations
	role string

	// disable pgroll version schemas creation and deletion
	disableVersionSchemas bool
	migrationHooks        MigrationHooks
}

// MigrationHooks defines hooks that can be set to be called at various points
// during the migration process
type MigrationHooks struct {
	// BeforeStartDDL is called before the DDL phase of migration start
	BeforeStartDDL func(*Roll) error
	// AfterStartDDL is called after the DDL phase of migration start is complete
	AfterStartDDL func(*Roll) error
	// BeforeBackfill is called before the backfill phase of migration start
	BeforeBackfill func(*Roll) error
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

// WithMigrationHooks sets the migration hooks for the Roll instance
// Migration hooks are called at various points during the migration process
// to allow for custom behavior to be injected
func WithMigrationHooks(hooks MigrationHooks) Option {
	return func(o *options) {
		o.migrationHooks = hooks
	}
}
