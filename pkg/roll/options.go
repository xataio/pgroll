// SPDX-License-Identifier: Apache-2.0

package roll

type options struct {
	// lock timeout in milliseconds for pgroll DDL operations
	lockTimeoutMs int

	// optional role to set before executing migrations
	role string

	// disable pgroll version schemas creation and deletion
	disableVersionSchemas bool
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
