// Package outboxer is an implementation of the outbox pattern.
// The producer of messages can durably store those messages in a local outbox before sending to a Message Endpoint.
// The durable local storage may be implemented in the Message Channel directly, especially when combined
// with Idempotent Messages.
package outboxer

import (
	"context"
	"database/sql/driver"
	"errors"
	"time"
)

var (
	// ErrMissingEventStream is used when no event stream is provided
	ErrMissingEventStream = errors.New("an event stream is required for the outboxer to work")

	// ErrMissingDataStore is used when no data store is provided
	ErrMissingDataStore = errors.New("a data store is required for the outboxer to work")
)

// DataStore defines the data store methods
type DataStore interface {
	// Tries to find the given message in the outbox.
	GetEvents(context.Context) ([]*OutboxMessage, error)
	Add(context.Context, *OutboxMessage) error
	AddWithinTx(context.Context, *OutboxMessage, func(driver.Tx) error) error
	SetAsDispatched(context.Context, int64) error
	Remove(context.Context) error
}

// EventStream defines the event stream methods
type EventStream interface {
	Send(context.Context, *OutboxMessage) error
}

// Outboxer implements the outbox pattern
type Outboxer struct {
	ds              DataStore
	es              EventStream
	checkInterval   time.Duration
	cleanUpInterval time.Duration

	errChan chan error
	okChan  chan struct{}
}

// New creates a new instance of Outboxer
func New(opts ...Option) (*Outboxer, error) {
	o := Outboxer{errChan: make(chan error), okChan: make(chan struct{})}
	for _, opt := range opts {
		opt(&o)
	}

	if o.ds == nil {
		return nil, ErrMissingDataStore
	}

	if o.es == nil {
		return nil, ErrMissingEventStream
	}

	return &o, nil
}

// ErrChan returns the error channel
func (o *Outboxer) ErrChan() <-chan error {
	return o.errChan
}

// OkChan returns the ok channel that is used when each message is successfully delivered
func (o *Outboxer) OkChan() <-chan struct{} {
	return o.okChan
}

// Send sends a message
func (o *Outboxer) Send(ctx context.Context, m *OutboxMessage) error {
	if err := o.ds.Add(ctx, m); err != nil {
		return err
	}

	return nil
}

// SendWithinTx encapsulate any database call within a transaction
func (o *Outboxer) SendWithinTx(ctx context.Context, evt *OutboxMessage, fn func(driver.Tx) error) error {
	if err := o.ds.AddWithinTx(ctx, evt, fn); err != nil {
		return err
	}

	return nil
}

// Start encapsulates two go routines. Starts the dispatcher, which is responsible for getting the messages
// from the data store and sending to the event stream.
// Starts the cleanup process, that makes sure old messages are removed from the data store.
func (o *Outboxer) Start(ctx context.Context) {
	go o.StartDispatcher(ctx)
	go o.StartCleanup(ctx)
}

// StartDispatcher starts the dispatcher, which is responsible for getting the messages
// from the data store and sending to the event stream.
func (o *Outboxer) StartDispatcher(ctx context.Context) {
	ticker := time.NewTicker(o.checkInterval)
	for {
		select {
		case <-ticker.C:
			evts, err := o.ds.GetEvents(ctx)
			if err != nil {
				o.errChan <- err
				break
			}

			for _, e := range evts {
				if err := o.es.Send(ctx, e); err != nil {
					o.errChan <- err
				} else {
					if err := o.ds.SetAsDispatched(ctx, e.ID); err != nil {
						o.errChan <- err
					} else {
						o.okChan <- struct{}{}
					}
				}
			}
		case <-ctx.Done():
			return
		}
	}
}

// StartCleanup starts the cleanup process, that makes sure old messages are removed from the data store.
func (o *Outboxer) StartCleanup(ctx context.Context) {
	ticker := time.NewTicker(o.cleanUpInterval)
	for {
		select {
		case <-ticker.C:
			if err := o.ds.Remove(ctx); err != nil {
				o.errChan <- err
			}

		case <-ctx.Done():
			return
		}
	}
}

// Stop closes all channels
func (o *Outboxer) Stop() {
	close(o.errChan)
	close(o.okChan)
}
