package amqp_test

import (
	"context"
	"testing"

	amqptest "github.com/NeowayLabs/wabbit/amqp"
	"github.com/NeowayLabs/wabbit/amqptest/server"
	"github.com/italolelis/outboxer"
	"github.com/italolelis/outboxer/es/amqp"
)

func TestAMQP_EventStream(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	fakeServer := server.NewServer("amqp://localhost:5672/%2f")
	fakeServer.Start()

	conn, err := amqptest.Dial("amqp://localhost:5672/%2f")
	if err != nil {
		t.Fatalf("an error was not expected: %s", err)
	}

	defer conn.Close()

	es := amqp.NewAMQP(conn.Connection)
	if err := es.Send(ctx, &outboxer.OutboxMessage{
		Payload: []byte("test payload"),
		Options: map[string]interface{}{
			amqp.ExchangeNameOption: "test",
			amqp.ExchangeTypeOption: "topic",
			amqp.RoutingKeyOption:   "test.send",
			amqp.ExchangeDurable:    true,
			amqp.ExchangeAutoDelete: false,
			amqp.ExchangeInternal:   false,
			amqp.ExchangeNoWait:     false,
		},
	}); err != nil {
		t.Fatalf("an error was not expected: %s", err)
	}
}
