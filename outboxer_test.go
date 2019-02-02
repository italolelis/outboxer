package outboxer_test

import (
	"context"
	"database/sql"
	"os"
	"testing"
	"time"

	"github.com/italolelis/outboxer"

	amqpOut "github.com/italolelis/outboxer/amqp"
	"github.com/italolelis/outboxer/postgres"
	_ "github.com/lib/pq"
	"github.com/streadway/amqp"
)

func TestIntegrationOutboxer(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	db, err := sql.Open("postgres", os.Getenv("DS_DSN"))
	if err != nil {
		t.Fatalf("could not connect to amqp: %s", err)
	}

	conn, err := amqp.Dial(os.Getenv("ES_DSN"))
	if err != nil {
		t.Fatalf("could not connect to amqp: %s", err)
	}

	t.Log("created postgres datastore")
	ds, err := postgres.WithInstance(ctx, db)
	if err != nil {
		t.Fatalf("could not setup the data store: %s", err)
	}
	defer ds.Close()

	t.Log("created amqp event stream")
	es := amqpOut.NewAMQP(conn)
	o, err := outboxer.New(
		outboxer.WithDataStore(ds),
		outboxer.WithEventStream(es),
		outboxer.WithCheckInterval(1*time.Second),
		outboxer.WithCleanupInterval(5*time.Second),
		outboxer.WithCleanUpBefore(time.Now().AddDate(0, 0, -5)),
		outboxer.WithCleanUpBatchSize(10),
		outboxer.WithMessageBatchSize(10),
	)
	if err != nil {
		t.Fatalf("could not create an outboxer instance: %s", err)
	}

	t.Log("started to listen for new messages")
	o.Start(ctx)
	defer o.Stop()

	done := make(chan struct{})

	go func(ob *outboxer.Outboxer) {
		for {
			select {
			case err := <-ob.ErrChan():
				t.Fatalf("could not send message: %s", err)
				return
			case <-ob.OkChan():
				t.Log("message received")
				done <- struct{}{}
				return
			case <-ctx.Done():
				return
			}
		}
	}(o)

	t.Log("sending message...")
	if err = o.Send(ctx, &outboxer.OutboxMessage{
		Payload: []byte("test payload"),
		Options: map[string]interface{}{
			amqpOut.ExchangeNameOption: "test",
			amqpOut.ExchangeTypeOption: "topic",
			amqpOut.RoutingKeyOption:   "test.send",
		},
	}); err != nil {
		t.Fatalf("could not send message: %s", err)
	}

	t.Log("waiting for succesfully sent messages...")
	<-done
	t.Log("messages received")
}
