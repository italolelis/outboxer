package outboxer

import "time"

// Option represents the outboxer options.
type Option func(*Outboxer)

// WithDataStore sets the data store where events will be stored before sending.
func WithDataStore(ds DataStore) Option {
	return func(o *Outboxer) {
		o.ds = ds
	}
}

// WithEventStream sets the event stream to where events will be sent.
func WithEventStream(es EventStream) Option {
	return func(o *Outboxer) {
		o.es = es
	}
}

// WithCheckInterval sets the frequency that outboxer will check for new events.
func WithCheckInterval(t time.Duration) Option {
	return func(o *Outboxer) {
		o.checkInterval = t
	}
}

// WithCleanupInterval sets the frequency that outboxer will clean old events from the data store.
func WithCleanupInterval(t time.Duration) Option {
	return func(o *Outboxer) {
		o.cleanUpInterval = t
	}
}

// WithCleanUpOlderThan sets the date that the clean up process should start removing from.
func WithCleanUpOlderThan(t time.Duration) Option {
	return func(o *Outboxer) {
		o.cleanUpOlderThan = t
	}
}

// WithCleanUpBatchSize sets the clean up process batch size.
func WithCleanUpBatchSize(s int32) Option {
	return func(o *Outboxer) {
		o.cleanUpBatchSize = s
	}
}

// WithMessageBatchSize sets how many messages will be sent at a time.
func WithMessageBatchSize(s int32) Option {
	return func(o *Outboxer) {
		o.messageBatchSize = s
	}
}
