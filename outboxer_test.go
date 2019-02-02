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
	t.Parallel()

	tests := []struct {
		scenario string
		function func(*testing.T)
	}{
		{
			"send successful message",
			testSendSuccessfulMessage,
		},
	}

	for _, test := range tests {
		t.Run(test.scenario, func(t *testing.T) {
			test.function(t)
		})
	}
}

func testSendSuccessfulMessage(t *testing.T) {
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

	ds, err := postgres.WithInstance(ctx, db)
	if err != nil {
		t.Fatalf("could not setup the data store: %s", err)
	}
	defer ds.Close()

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

	o.Start(ctx)
	defer o.Stop()

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
