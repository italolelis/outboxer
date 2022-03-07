// Package amqp is the AMQP implementation of an event stream.
package amqp

import (
	"context"
	"fmt"

	"github.com/italolelis/outboxer"
	amqp "github.com/rabbitmq/amqp091-go"
)

const (
	defaultExchangeType = "topic"

	// ExchangeNameOption is the exchange name option.
	ExchangeNameOption = "exchange.name"

	// ExchangeTypeOption is the exchange type option.
	ExchangeTypeOption = "exchange.type"

	// ExchangeDurable is the exchange durable option.
	ExchangeDurable = "exchange.durable"

	// ExchangeAutoDelete is the exchange auto delete option.
	ExchangeAutoDelete = "exchange.auto_delete"

	// ExchangeInternal is the exchange internal option.
	ExchangeInternal = "exchange.internal"

	// ExchangeNoWait is the exchange no wait option.
	ExchangeNoWait = "exchange.no_wait"

	// RoutingKeyOption is the routing key option.
	RoutingKeyOption = "routing_key"
)

// AMQP is the wrapper for the AMQP library.
type AMQP struct {
	conn *amqp.Connection
}

type options struct {
	exchange     string
	exchangeType string
	routingKey   string
	durable      bool
	autoDelete   bool
	internal     bool
	noWait       bool
}

// NewAMQP creates a new instance of AMQP.
func NewAMQP(conn *amqp.Connection) *AMQP {
	return &AMQP{conn: conn}
}

// Send sends the message to the event stream.
func (r *AMQP) Send(ctx context.Context, evt *outboxer.OutboxMessage) error {
	ch, err := r.conn.Channel()
	if err != nil {
		return err
	}

	defer ch.Close()

	opts := r.parseOptions(evt.Options)
	if err := ch.ExchangeDeclare(
		opts.exchange,     // name
		opts.exchangeType, // type
		opts.durable,      // durable
		opts.autoDelete,   // auto-deleted
		opts.internal,     // internal
		opts.noWait,       // noWait
		nil,               // arguments
	); err != nil {
		return fmt.Errorf("exchange declare: %w", err)
	}

	if err = ch.Publish(
		opts.exchange,   // publish to an exchange
		opts.routingKey, // routing to 0 or more queues
		false,           // mandatory
		false,           // immediate
		amqp.Publishing{
			ContentType:  "text/plain",
			Body:         evt.Payload,
			DeliveryMode: amqp.Transient, // 1=non-persistent, 2=persistent
			Priority:     0,              // 0-9
			Headers:      amqp.Table(evt.Headers),
		},
	); err != nil {
		return fmt.Errorf("exchange publish: %w", err)
	}

	return nil
}

func (r *AMQP) parseOptions(opts outboxer.DynamicValues) *options {
	opt := options{exchangeType: defaultExchangeType, durable: true}

	if data, ok := opts[ExchangeNameOption]; ok {
		opt.exchange = data.(string)
	}

	if data, ok := opts[ExchangeTypeOption]; ok {
		opt.exchangeType = data.(string)
	}

	if data, ok := opts[ExchangeDurable]; ok {
		opt.durable = data.(bool)
	}

	if data, ok := opts[ExchangeAutoDelete]; ok {
		opt.autoDelete = data.(bool)
	}

	if data, ok := opts[ExchangeInternal]; ok {
		opt.internal = data.(bool)
	}

	if data, ok := opts[ExchangeNoWait]; ok {
		opt.noWait = data.(bool)
	}

	if data, ok := opts[RoutingKeyOption]; ok {
		opt.routingKey = data.(string)
	}

	return &opt
}
