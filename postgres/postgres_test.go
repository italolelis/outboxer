// +build integration

package postgres

import (
	"context"
	"database/sql"
	"errors"
	"testing"
	"time"

	"github.com/italolelis/outboxer"
	"github.com/italolelis/outboxer/amqp"
	_ "github.com/lib/pq"
)

type inMemES struct {
	ok bool
}

// Send mocks the behaviour of the event store
func (inmem *inMemES) Send(context.Context, *outboxer.OutboxMessage) error {
	if inmem.ok {
		return nil
	}

	return errors.New("mock returned an error")
}

func TestDatastore(t *testing.T) {
	t.Parallel()

	tests := []struct {
		scenario string
		function func(*testing.T)
	}{
		{
			"successfully save the event into the event store",
			testSaveEventSuccessfully,
		},
	}

	for _, test := range tests {
		t.Run(test.scenario, func(t *testing.T) {
			test.function(t)
		})
	}
}

func testSaveEventSuccessfully(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	db, err := sql.Open("postgres", "postgres://coffee:qwerty123@localhost/reception?sslmode=disable")
	if err != nil {
		t.Fatalf("could not connect to postgres: %s", err)
	}

	ds, err := WithInstance(ctx, db)
	if err != nil {
		t.Fatalf("could not setup the data store: %s", err)
	}

	o, err := outboxer.New(
		outboxer.WithDataStore(ds),
		outboxer.WithEventStream(&inMemES{true}),
		outboxer.WithCheckInterval(1*time.Second),
		outboxer.WithCleanupInterval(5*time.Second),
	)
	if err != nil {
		t.Fatalf("could not create an outboxer instance: %s", err)
	}

	o.Start(ctx)
	defer o.Stop()

	if err = o.Send(ctx, &outboxer.OutboxMessage{
		Payload: []byte("test payload"),
		Options: map[string]interface{}{
			amqp.ExchangeNameOption: "test",
			amqp.ExchangeTypeOption: "test.send",
		},
	}); err != nil {
		t.Fatalf("could not send message: %s", err)
	}

	for {
		select {
		case err := <-o.ErrChan():
			t.Fatalf("could not send message: %s", err)
		case <-o.OkChan():
			t.Log("message received")
			return
		}
	}
}
