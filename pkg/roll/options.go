// SPDX-License-Identifier: Apache-2.0

package roll

type options struct {
	// lock timeout in milliseconds for pgroll DDL operations
	lockTimeoutMs int

	// optional role to set before executing migrations
	role string

	// disable pgroll version schemas creation and deletion
	disableVersionSchemas bool

	// a map of setting/value pairs to be set for the duration of migration start
	settingsOnMigrationStart map[string]string

	// whether to make a no-op schema change in between completing the DDL
	// operations for migration start and performing backfills
	kickstartReplication bool
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

// WithSettingsOnMigrationStart defines a map of Postgres setting/value pairs
// to be set for the duration of the DDL phase of migration start. Settings
// will be restored to their previous values once the DDL phase is complete.
func WithSettingsOnMigrationStart(settings map[string]string) Option {
	return func(o *options) {
		o.settingsOnMigrationStart = settings
	}
}

// WithKickstartReplication defines an option that when set will make a no-op
// schema change in between completing the DDL operations for migration start
// and performing backfills. This can be used to ensure that schema replication
// is up-to-date before starting backfills.
func WithKickstartReplication() Option {
	return func(o *options) {
		o.kickstartReplication = true
	}
}
