package postgres

import (
	"context"
	"database/sql"
	"os"
	"testing"
	"time"

	"github.com/italolelis/outboxer"
	_ "github.com/lib/pq"
)

func TestDatastore(t *testing.T) {
	t.Parallel()

	tests := []struct {
		scenario string
		function func(context.Context, *testing.T, outboxer.DataStore)
	}{
		{
			"successfully add the message into the event store",
			testAddSuccessfully,
		},
		{
			"successfully set the message as dispatched",
			testSetDispatchedSuccessfully,
		},
		{
			"add messages within a transaction",
			testAddTx,
		},
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	db, err := sql.Open("postgres", os.Getenv("DS_DSN"))
	if err != nil {
		t.Fatalf("failed to connect to postgres: %s", err)
	}

	ds, err := WithInstance(ctx, db)
	if err != nil {
		t.Fatalf("failed to setup the data store: %s", err)
	}
	defer ds.Close()

	for _, test := range tests {
		t.Run(test.scenario, func(t *testing.T) {
			test.function(ctx, t, ds)
		})
	}
}

func testAddSuccessfully(ctx context.Context, t *testing.T, ds outboxer.DataStore) {
	if err := ds.Add(ctx, &outboxer.OutboxMessage{
		Payload: []byte("test payload"),
	}); err != nil {
		t.Fatalf("failed to add message in the data store: %s", err)
	}

	msgs, err := ds.GetEvents(ctx, 10)
	if err != nil {
		t.Fatalf("failed to retrieve messages from the data store: %s", err)
	}

	if len(msgs) != 1 {
		t.Fatalf("was expecting 1 message in the data store but got %d", len(msgs))
	}

	for _, m := range msgs {
		err := ds.SetAsDispatched(ctx, m.ID)
		if err != nil {
			t.Fatalf("failed to set message as dispatched: %s", err)
		}
	}

	if err := ds.Remove(ctx, time.Now(), 10); err != nil {
		t.Fatalf("failed to remove messages: %s", err)
	}
}

func testSetDispatchedSuccessfully(ctx context.Context, t *testing.T, ds outboxer.DataStore) {
	if err := ds.Add(ctx, &outboxer.OutboxMessage{
		Payload: []byte("test payload"),
	}); err != nil {
		t.Fatalf("failed to add message in the data store: %s", err)
	}

	msgs, err := ds.GetEvents(ctx, 10)
	if err != nil {
		t.Fatalf("failed to retrieve messages from the data store: %s", err)
	}

	for _, m := range msgs {
		err := ds.SetAsDispatched(ctx, m.ID)
		if err != nil {
			t.Fatalf("failed to set message as dispatched: %s", err)
		}
	}

	if err := ds.Remove(ctx, time.Now(), 10); err != nil {
		t.Fatalf("failed to remove messages: %s", err)
	}
}

func testAddTx(ctx context.Context, t *testing.T, ds outboxer.DataStore) {
	fn := func(tx outboxer.ExecerContext) error {
		_, err := tx.ExecContext(ctx, "SELECT * from event_store LIMIT 1")
		return err
	}

	if err := ds.AddWithinTx(ctx, &outboxer.OutboxMessage{
		Payload: []byte("test payload"),
	}, fn); err != nil {
		t.Fatalf("failed to add message in the data store: %s", err)
	}

	msgs, err := ds.GetEvents(ctx, 10)
	if err != nil {
		t.Fatalf("failed to retrieve messages from the data store: %s", err)
	}

	for _, m := range msgs {
		err := ds.SetAsDispatched(ctx, m.ID)
		if err != nil {
			t.Fatalf("failed to set message as dispatched: %s", err)
		}
	}

	if err := ds.Remove(ctx, time.Now(), 10); err != nil {
		t.Fatalf("failed to remove messages: %s", err)
	}
}
