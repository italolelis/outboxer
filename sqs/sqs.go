// Package SQS is the AWS SQS implementation of an event stream.
package sqs

import (
	"context"
	"fmt"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/sqs"
	"github.com/aws/aws-sdk-go/service/sqs/sqsiface"
	"github.com/italolelis/outboxer"
)

const (
	// QueueNameOption is the queue name option.
	QueueNameOption = "queue_name"

	// ExplicitHashKeyOption is the explicit hash key option.
	DelaySecondsOption = "delay_seconds"

	// MessageGroupIDOption is the grouping id sequence option.
	MessageGroupIDOption = "message_group_id"

	// MessageDedupIDOption is the deduplication id option.
	MessageDedupIDOption = "message_dedup_id"
)

// SQS is the wrapper for the SQS library.
type SQS struct {
	conn sqsiface.SQSAPI
}

type options struct {
	queueName    *string
	delaySeconds *int64
	msgGroupID   *string
	msgDedupID   *string
}

type sqsOption func(*options)

// New creates a new instance of SQS.
func New(conn sqsiface.SQSAPI) *SQS {
	return &SQS{conn: conn}
}

// Send sends the message to the event stream.
func (r *SQS) Send(ctx context.Context, evt *outboxer.OutboxMessage) error {
	opts := newOptions(
		withQueueName(evt.Options),
		withDelaySeconds(evt.Options),
		withGroupID(evt.Options),
		withDedupID(evt.Options),
	)

	input := &sqs.SendMessageInput{
		QueueUrl:               opts.queueName,
		MessageBody:            aws.String(string((evt.Payload))),
		DelaySeconds:           opts.delaySeconds,
		MessageGroupId:         opts.msgGroupID,
		MessageDeduplicationId: opts.msgDedupID,
	}

	msgAttributes := r.parseHeaders(evt.Headers)
	if len(msgAttributes) > 0 {
		input.MessageAttributes = msgAttributes
	}

	_, err := r.conn.SendMessageWithContext(ctx, input)
	if err != nil {
		return fmt.Errorf("failed to publish message: %w", err)
	}

	return nil
}

func newOptions(opts ...sqsOption) *options {
	opt := &options{
		delaySeconds: nil,
		msgDedupID:   nil,
	}

	for _, option := range opts {
		if option == nil {
			continue
		}

		option(opt)
	}

	return opt
}

func withQueueName(opts outboxer.DynamicValues) sqsOption {
	if data, ok := opts[QueueNameOption]; ok {
		return func(opt *options) {
			opt.queueName = aws.String(data.(string))
		}
	}

	return nil
}

func withDelaySeconds(opts outboxer.DynamicValues) sqsOption {
	if data, ok := opts[DelaySecondsOption]; ok {
		return func(opt *options) {
			opt.delaySeconds = aws.Int64(data.(int64))
		}
	}

	return nil
}

func withGroupID(opts outboxer.DynamicValues) sqsOption {
	if data, ok := opts[MessageGroupIDOption]; ok {
		return func(opt *options) {
			opt.msgGroupID = aws.String(data.(string))
		}
	}

	return nil
}

func withDedupID(opts outboxer.DynamicValues) sqsOption {
	if data, ok := opts[MessageDedupIDOption]; ok {
		return func(opt *options) {
			opt.msgDedupID = aws.String(data.(string))
		}
	}

	return nil
}

func (r *SQS) parseHeaders(headers outboxer.DynamicValues) (response map[string]*sqs.MessageAttributeValue) {
	if len(headers) == 0 {
		return
	}

	response = make(map[string]*sqs.MessageAttributeValue)

	for key, value := range headers {
		response[key] = &sqs.MessageAttributeValue{
			DataType:    aws.String("String"),
			StringValue: aws.String(value.(string)),
		}
	}

	return response
}
