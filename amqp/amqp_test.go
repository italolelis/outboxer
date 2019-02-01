// +build integration

package amqp

import (
	"context"
	"database/sql/driver"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/italolelis/outboxer"
	"github.com/streadway/amqp"
)

type inMemDS struct {
	data []*outboxer.OutboxMessage
}

func (inmem *inMemDS) GetEvents(ctx context.Context) ([]*outboxer.OutboxMessage, error) {
	return inmem.data, nil
}

func (inmem *inMemDS) SetAsDispatched(ctx context.Context, id int64) error {
	for _, m := range inmem.data {
		if m.ID == id {
			m.Dispatched = true
			m.DispatchedAt = time.Now()
			return nil
		}
	}

	return fmt.Errorf("message not found")
}

func (inmem *inMemDS) Add(ctx context.Context, m *outboxer.OutboxMessage) error {
	inmem.data = append(inmem.data, m)

	return nil
}

func (inmem *inMemDS) AddWithinTx(ctx context.Context, m *outboxer.OutboxMessage, fn func(driver.Tx) error) error {
	return inmem.Add(ctx, m)
}

func (inmem *inMemDS) Remove(ctx context.Context) error {
	for i, m := range inmem.data {
		if m.DispatchedAt == time.Now().AddDate(0, 0, -5) {
			inmem.data = append(inmem.data[:i], inmem.data[i+1:]...)
			return nil
		}
	}

	return errors.New("event not found")
}

func TestRabbit(t *testing.T) {
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

	conn, err := amqp.Dial("amqp://localhost/")
	if err != nil {
		t.Fatalf("could not connect to amqp: %s", err)
	}

	es := NewAMQP(conn)
	o, err := outboxer.New(
		outboxer.WithDataStore(&inMemDS{}),
		outboxer.WithEventStream(es),
		outboxer.WithCheckInterval(1*time.Second),
		outboxer.WithCleanupInterval(5*time.Second),
	)
	if err != nil {
		t.Fatalf("could not create an outboxer instance: %s", err)
	}

	o.Start(ctx)
	defer o.Stop()

	if err = o.Send(ctx, &outboxer.OutboxMessage{
		Payload: []byte("test payload"),
		Options: map[string]interface{}{
			"exchange.name": "test",
			"routing_key":   "test.send",
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
