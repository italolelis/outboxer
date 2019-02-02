package amqp

import (
	"context"
	"os"
	"testing"

	"github.com/italolelis/outboxer"
	"github.com/streadway/amqp"
)

func TestAMQPEventStream(t *testing.T) {
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

	conn, err := amqp.Dial(os.Getenv("ES_DSN"))
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
		},
	}); err != nil {
		t.Fatalf("an error was not expected: %s", err)
	}
}
