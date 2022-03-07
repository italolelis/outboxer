// Package pubsub is the GCP PubSub implementation of an event stream.
package pubsub

import (
	"context"

	"cloud.google.com/go/pubsub"
	"github.com/italolelis/outboxer"
)

const (
	// TopicNameOption is the topic name option.
	TopicNameOption = "topic_name"

	// OrderingKeyOption is the ordering key option.
	OrderingKeyOption = "ordering_key"
)

// Pubsub is the wrapper for the Pubsub library.
type Pubsub struct {
	client *pubsub.Client
}

type options struct {
	topicName   string
	orderingKey string
}

// New creates a new instance of Kinesis.
func New(c *pubsub.Client) *Pubsub {
	return &Pubsub{client: c}
}

// Send sends the message to the event stream.
func (p *Pubsub) Send(ctx context.Context, evt *outboxer.OutboxMessage) error {
	opts := p.parseOptions(evt.Options)

	topic := p.client.Topic(opts.topicName)
	_ = topic.Publish(ctx, &pubsub.Message{
		Data:        evt.Payload,
		OrderingKey: opts.orderingKey,
	})

	return nil
}

func (p *Pubsub) parseOptions(opts outboxer.DynamicValues) *options {
	opt := options{}

	if data, ok := opts[TopicNameOption]; ok {
		opt.topicName = data.(string)
	}

	if data, ok := opts[OrderingKeyOption]; ok {
		opt.orderingKey = data.(string)
	}

	return &opt
}
