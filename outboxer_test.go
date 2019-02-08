package outboxer_test

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/italolelis/outboxer"

	amqpOut "github.com/italolelis/outboxer/amqp"
	_ "github.com/lib/pq"
)

type inMemDS struct {
	data []*outboxer.OutboxMessage
}

func (inmem *inMemDS) GetEvents(ctx context.Context, batchSize int32) ([]*outboxer.OutboxMessage, error) {
	return inmem.data, nil
}

func (inmem *inMemDS) SetAsDispatched(ctx context.Context, id int64) error {
	for _, m := range inmem.data {
		if m.ID == id {
			m.Dispatched = true
			m.DispatchedAt.Time = time.Now()
			return nil
		}
	}

	return fmt.Errorf("message not found")
}

func (inmem *inMemDS) Add(ctx context.Context, m *outboxer.OutboxMessage) error {
	inmem.data = append(inmem.data, m)

	return nil
}

func (inmem *inMemDS) AddWithinTx(ctx context.Context, m *outboxer.OutboxMessage, fn func(outboxer.ExecerContext) error) error {
	return inmem.Add(ctx, m)
}

func (inmem *inMemDS) Remove(ctx context.Context, cleanUpBefore time.Time, batchSize int32) error {
	for i, m := range inmem.data {
		if m.DispatchedAt.Time == cleanUpBefore {
			inmem.data = append(inmem.data[:i], inmem.data[i+1:]...)
			return nil
		}
	}

	return errors.New("event not found")
}

type inMemES struct {
	ok bool
}

// Send mocks the behaviour of the event store
func (inmem *inMemES) Send(context.Context, *outboxer.OutboxMessage) error {
	if inmem.ok {
		return nil
	}

	return errors.New("mock returned an error")
}

func TestIntegrationOutboxer(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	o, err := outboxer.New(
		outboxer.WithDataStore(&inMemDS{}),
		outboxer.WithEventStream(&inMemES{true}),
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

	t.Log("waiting for successfully sent messages...")
	<-done
}
