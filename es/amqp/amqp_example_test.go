package amqp_test

import (
	"context"
	"fmt"
	"os"

	"github.com/italolelis/outboxer"
	amqpOut "github.com/italolelis/outboxer/es/amqp"
	amqp "github.com/rabbitmq/amqp091-go"
)

func ExampleNewAMQP() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	conn, err := amqp.Dial(os.Getenv("ES_DSN"))
	if err != nil {
		fmt.Printf("failed to connect to amqp: %s", err)
		return
	}

	defer conn.Close()

	es := amqpOut.NewAMQP(conn)

	// this is done internally by outboxer
	if err := es.Send(ctx, &outboxer.OutboxMessage{
		Payload: []byte("test payload"),
		Options: map[string]interface{}{
			amqpOut.ExchangeNameOption: "test",
			amqpOut.ExchangeTypeOption: "topic",
			amqpOut.RoutingKeyOption:   "test.send",
		},
	}); err != nil {
		fmt.Printf("an error was not expected: %s", err)
		return
	}
}
