package outboxer_test

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"time"

	"github.com/italolelis/outboxer"
	amqpOut "github.com/italolelis/outboxer/es/amqp"
	"github.com/italolelis/outboxer/storage/postgres"
	amqp "github.com/rabbitmq/amqp091-go"
)

// nolint
func ExampleNew() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	db, err := sql.Open("postgres", os.Getenv("DS_DSN"))
	if err != nil {
		fmt.Printf("could not connect to amqp: %s", err)
		return
	}

	conn, err := amqp.Dial(os.Getenv("ES_DSN"))
	if err != nil {
		fmt.Printf("could not connect to amqp: %s", err)
		return
	}

	// we need to create a data store instance first
	ds, err := postgres.WithInstance(ctx, db)
	if err != nil {
		fmt.Printf("could not setup the data store: %s", err)
		return
	}
	defer ds.Close()

	// we create an event stream passing the amqp connection
	es := amqpOut.NewAMQP(conn)

	// now we create an outboxer instance passing the data store and event stream
	o, err := outboxer.New(
		outboxer.WithDataStore(ds),
		outboxer.WithEventStream(es),
		outboxer.WithCheckInterval(1*time.Second),
		outboxer.WithMessageBatchSize(10),
	)
	if err != nil {
		fmt.Printf("could not create an outboxer instance: %s", err)
		return
	}

	// here we initialize the outboxer checks and cleanup go rotines
	o.Start(ctx)
	defer o.Stop()

	// finally we are ready to send messages
	if err = o.Send(ctx, &outboxer.OutboxMessage{
		Payload: []byte("test payload"),
		Options: map[string]interface{}{
			amqpOut.ExchangeNameOption: "test",
			amqpOut.ExchangeTypeOption: "topic",
			amqpOut.RoutingKeyOption:   "test.send",
		},
	}); err != nil {
		fmt.Printf("could not send message: %s", err)
		return
	}

	// we can also listen for errors and ok messages that were send
	for {
		select {
		case err := <-o.ErrChan():
			fmt.Printf("could not send message: %s", err)
		case <-o.OkChan():
			fmt.Printf("message received")
			return
		}
	}
}
