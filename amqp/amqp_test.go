package amqp

import (
	"context"
	"os"
	"testing"

	"github.com/italolelis/outboxer"
	"github.com/streadway/amqp"
)

func TestAMQP_EventStream(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	endpoint := os.Getenv("ES_DSN")
	if endpoint == "" {
		endpoint = "amqp://localhost/"
	}

	conn, err := amqp.Dial(endpoint)
	if err != nil {
		t.Fatalf("failed to connect to amqp: %s", err)
	}
	defer conn.Close()

	es := NewAMQP(conn)
	if err := es.Send(ctx, &outboxer.OutboxMessage{
		Payload: []byte("test payload"),
		Options: map[string]interface{}{
			ExchangeNameOption: "test",
			ExchangeTypeOption: "topic",
			RoutingKeyOption:   "test.send",
			ExchangeDurable:    true,
			ExchangeAutoDelete: false,
			ExchangeInternal:   false,
			ExchangeNoWait:     false,
		},
	}); err != nil {
		t.Fatalf("an error was not expected: %s", err)
	}
}
