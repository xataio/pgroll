// SPDX-License-Identifier: Apache-2.0

package backfill

import "time"

type OptionFn func(*Backfill)

// WithBatchSize sets the batch size for the backfill operation.
func WithBatchSize(batchSize int) OptionFn {
	return func(o *Backfill) {
		o.batchSize = batchSize
	}
}

// WithBatchDelay sets the delay between batches for the backfill operation.
func WithBatchDelay(delay time.Duration) OptionFn {
	return func(o *Backfill) {
		o.batchDelay = delay
	}
}

// WithCallbacks sets the callbacks for the backfill operation.
// Callbacks are invoked after each batch is processed.
func WithCallbacks(cbs ...CallbackFn) OptionFn {
	return func(o *Backfill) {
		o.callbacks = cbs
	}
}

// WithStateSchema sets in which `pgroll` stores its internal state.
func WithStateSchema(schema string) OptionFn {
	return func(o *Backfill) {
		o.stateSchema = schema
	}
}
