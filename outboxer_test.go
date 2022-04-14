package outboxer_test

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/italolelis/outboxer"
	amqpOut "github.com/italolelis/outboxer/es/amqp"
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

type inMemES struct {
	ok bool
}

// Send mocks the behavior of the event store
func (inmem *inMemES) Send(context.Context, *outboxer.OutboxMessage) error {
	if inmem.ok {
		return nil
	}

	return errors.New("mock returned an error")
}

func TestOutboxer_Send(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	o, err := outboxer.New(
		outboxer.WithDataStore(&inMemDS{}),
		outboxer.WithEventStream(&inMemES{true}),
		outboxer.WithCheckInterval(1*time.Second),
		outboxer.WithMessageBatchSize(10),
	)
	if err != nil {
		t.Fatalf("could not create an outboxer instance: %s", err)
	}

	fmt.Println("started to listen for new messages")

	o.Start(ctx)
	defer o.Stop()

	done := make(chan struct{})

	go func(ob *outboxer.Outboxer) {
		for {
			select {
			case err := <-ob.ErrChan():
				t.Errorf("could not send message: %s", err)
				return
			case <-ob.OkChan():
				fmt.Println("message received")
				done <- struct{}{}
				return
			case <-ctx.Done():
				return
			}
		}
	}(o)

	fmt.Println("sending message...")

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

	fmt.Println("waiting for successfully sent messages...")
	<-done
}

func TestOutboxer_SendWithinTx(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	o, err := outboxer.New(
		outboxer.WithDataStore(&inMemDS{}),
		outboxer.WithEventStream(&inMemES{true}),
		outboxer.WithCheckInterval(1*time.Second),
		outboxer.WithMessageBatchSize(10),
	)
	if err != nil {
		t.Fatalf("could not create an outboxer instance: %s", err)
	}

	fmt.Println("started to listen for new messages")
	o.Start(ctx)

	defer o.Stop()

	done := make(chan struct{})

	go func(ob *outboxer.Outboxer) {
		for {
			select {
			case err := <-ob.ErrChan():
				t.Errorf("could not send message: %s", err)
				return
			case <-ob.OkChan():
				fmt.Println("message received")
				done <- struct{}{}
				return
			case <-ctx.Done():
				return
			}
		}
	}(o)

	if err = o.SendWithinTx(ctx, &outboxer.OutboxMessage{
		Payload: []byte("test payload"),
		Options: map[string]interface{}{
			amqpOut.ExchangeNameOption: "test",
			amqpOut.ExchangeTypeOption: "topic",
			amqpOut.RoutingKeyOption:   "test.send",
		},
	}, func(execer outboxer.ExecerContext) error {
		if _, err := execer.ExecContext(ctx, "SELECT * FROM orders LIMIT 1"); err != nil {
			return err
		}
		return nil
	}); err != nil {
		t.Fatalf("could not send message within transaction: %s", err)
	}

	fmt.Println("waiting for successfully sent messages...")
	<-done
}

func TestOutboxer_WithWrongParams(t *testing.T) {
	_, err := outboxer.New(
		outboxer.WithEventStream(&inMemES{true}),
	)
	if err == nil {
		t.Fatalf("this should return an error ErrMissingDataStore")
	}

	_, err = outboxer.New(
		outboxer.WithDataStore(&inMemDS{}),
	)
	if err == nil {
		t.Fatalf("this should return an error ErrMissingEventStream")
	}
}
