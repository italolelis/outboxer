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
	// QueueNameOption is the queue name option
	QueueNameOption = "queue_name"

	// ExplicitHashKeyOption is the explicit hash key option
	DelaySecondsOption = "delay_seconds"

	// MessageGroupIdOption is the grouping id sequence option
	MessageGroupIdOption = "message_group_id"

	// MessageDedupIdOption is the deduplication id option
	MessageDedupIdOption = "message_dedup_id"
)

// Sqs is the wrapper for the Sqs library
type Sqs struct {
	conn sqsiface.SQSAPI
}

type options struct {
	queueName    *string
	delaySeconds *int64
	msgGroupId   *string
	msgDedupId   *string
}

// New creates a new instance of Sqs
func New(conn sqsiface.SQSAPI) *Sqs {
	return &Sqs{conn: conn}
}

// Send sends the message to the event stream
func (r *Sqs) Send(ctx context.Context, evt *outboxer.OutboxMessage) error {
	opts := r.parseOptions(evt.Options)

	input := &sqs.SendMessageInput{
		QueueUrl:               opts.queueName,
		MessageBody:            aws.String(string((evt.Payload))),
		DelaySeconds:           opts.delaySeconds,
		MessageGroupId:         opts.msgGroupId,
		MessageDeduplicationId: opts.msgDedupId,
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

func (r *Sqs) parseOptions(opts outboxer.DynamicValues) *options {
	opt := options{}

	if data, ok := opts[QueueNameOption]; ok {
		opt.queueName = aws.String(data.(string))
	}

	if data, ok := opts[DelaySecondsOption]; ok {
		opt.delaySeconds = aws.Int64(data.(int64))
	}

	if data, ok := opts[MessageGroupIdOption]; ok {
		opt.msgGroupId = aws.String(data.(string))
	}

	if data, ok := opts[MessageDedupIdOption]; ok {
		opt.msgDedupId = aws.String(data.(string))
	}

	return &opt
}

func (r *Sqs) parseHeaders(headers outboxer.DynamicValues) (response map[string]*sqs.MessageAttributeValue) {
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
