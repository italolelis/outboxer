package outboxer_test

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"time"

	"github.com/italolelis/outboxer"
	amqpOut "github.com/italolelis/outboxer/amqp"
	"github.com/italolelis/outboxer/postgres"
	"github.com/streadway/amqp"
)

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

	ds, err := postgres.WithInstance(ctx, db)
	if err != nil {
		fmt.Printf("could not setup the data store: %s", err)
		return
	}
	defer ds.Close()

	es := amqpOut.NewAMQP(conn)
	o, err := outboxer.New(
		outboxer.WithDataStore(ds),
		outboxer.WithEventStream(es),
		outboxer.WithCheckInterval(1*time.Second),
		outboxer.WithCleanupInterval(5*time.Second),
		outboxer.WithCleanUpBefore(time.Now().AddDate(0, 0, -5)),
	)
	if err != nil {
		fmt.Printf("could not create an outboxer instance: %s", err)
		return
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
		fmt.Printf("could not send message: %s", err)
		return
	}

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
