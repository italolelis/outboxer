// Package kinesis is the AWS Kinesis implementation of an event stream.
package kinesis

import (
	"context"
	"fmt"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/kinesis"
	"github.com/aws/aws-sdk-go/service/kinesis/kinesisiface"
	"github.com/italolelis/outboxer"
)

const (
	defaultExchangeType = "topic"

	// StreamNameOption is the stream name option
	StreamNameOption = "stream_name"

	// ExplicitHashKeyOption is the explicit hash key option
	ExplicitHashKeyOption = "explicit_hash_key"

	// PartitionKeyOption is the partition key option
	PartitionKeyOption = "partition_key"

	// SequenceNumberForOrderingOption is the sequence number for ordering option
	SequenceNumberForOrderingOption = "partition_key"
)

// Kinesis is the wrapper for the Kinesis library
type Kinesis struct {
	conn kinesisiface.KinesisAPI
}

type options struct {
	streamName                *string
	explicitHashKey           *string
	partitionKey              *string
	sequenceNumberForOrdering *string
}

// New creates a new instance of Kinesis
func New(conn kinesisiface.KinesisAPI) *Kinesis {
	return &Kinesis{conn: conn}
}

// Send sends the message to the event stream
func (r *Kinesis) Send(ctx context.Context, evt *outboxer.OutboxMessage) error {
	opts := r.parseOptions(evt.Options)

	_, err := r.conn.PutRecordWithContext(ctx, &kinesis.PutRecordInput{
		Data:                      evt.Payload,
		StreamName:                opts.streamName,
		ExplicitHashKey:           opts.explicitHashKey,
		SequenceNumberForOrdering: opts.sequenceNumberForOrdering,
		PartitionKey:              opts.partitionKey,
	})
	if err != nil {
		return fmt.Errorf("failed to publish message: %w", err)
	}

	return nil
}

func (r *Kinesis) parseOptions(opts outboxer.DynamicValues) *options {
	opt := options{partitionKey: aws.String(time.Now().Format(time.RFC3339Nano))}

	if data, ok := opts[StreamNameOption]; ok {
		opt.streamName = aws.String(data.(string))
	}

	if data, ok := opts[ExplicitHashKeyOption]; ok {
		opt.explicitHashKey = aws.String(data.(string))
	}

	if data, ok := opts[PartitionKeyOption]; ok {
		opt.partitionKey = aws.String(data.(string))
	}

	if data, ok := opts[SequenceNumberForOrderingOption]; ok {
		opt.sequenceNumberForOrdering = aws.String(data.(string))
	}

	return &opt
}
