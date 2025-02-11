// SPDX-License-Identifier: Apache-2.0

package backfill

import (
	"time"
)

type Config struct {
	batchSize  int
	batchDelay time.Duration
	callbacks  []CallbackFn
}

const (
	DefaultBatchSize int           = 1000
	DefaultDelay     time.Duration = 0
)

type OptionFn func(*Config)

func NewConfig(opts ...OptionFn) *Config {
	c := &Config{
		batchSize:  DefaultBatchSize,
		batchDelay: DefaultDelay,
		callbacks:  make([]CallbackFn, 0),
	}

	for _, opt := range opts {
		opt(c)
	}
	return c
}

// WithBatchSize sets the batch size for the backfill operation.
func WithBatchSize(batchSize int) OptionFn {
	return func(o *Config) {
		o.batchSize = batchSize
	}
}

// WithBatchDelay sets the delay between batches for the backfill operation.
func WithBatchDelay(delay time.Duration) OptionFn {
	return func(o *Config) {
		o.batchDelay = delay
	}
}

func (c *Config) AddCallback(fn CallbackFn) {
	c.callbacks = append(c.callbacks, fn)
}
