package amqp_test

import (
	"context"
	"os"
	"testing"

	"github.com/italolelis/outboxer"
	"github.com/italolelis/outboxer/amqp"
	amqpraw "github.com/streadway/amqp"
)

func TestAMQP_EventStream(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	endpoint := os.Getenv("ES_DSN")
	if endpoint == "" {
		endpoint = "amqp://localhost/"
	}

	conn, err := amqpraw.Dial(endpoint)
	if err != nil {
		t.Fatalf("failed to connect to amqp: %s", err)
	}

	defer conn.Close()

	es := amqp.NewAMQP(conn)
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
