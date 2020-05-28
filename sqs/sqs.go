package sqs

import (
	"context"
	"fmt"
	awsSqs "github.com/aws/aws-sdk-go/service/sqs"
	"github.com/italolelis/outboxer"
)

const (
	// QueueURL is the URL of the SQS queue
	QueueURL = "queue_url"

	// MessageGroupID is the Group ID (when using Fifo queues)
	MessageGroupID = "message_group_id"
)

type Sqs struct {
	client *awsSqs.SQS
}

func New(client *awsSqs.SQS) *Sqs {
	return &Sqs{
		client,
	}
}

func (s *Sqs) Send(ctx context.Context, evt *outboxer.OutboxMessage) error {
	queueURL, exists := evt.Options[QueueURL].(string)
	if !exists {
		return fmt.Errorf("message does not have a queue url")
	}

	PayloadAsString := string(evt.Payload)
	message := awsSqs.SendMessageInput{
		QueueUrl:    &queueURL,
		MessageBody: &PayloadAsString,
	}

	groupIDFromOptions, exists := evt.Options[MessageGroupID].(string)
	if exists {
		message.MessageGroupId = &groupIDFromOptions
	}

	_, err := s.client.SendMessage(&message)
	if err != nil {
		return fmt.Errorf("failed to publish message: %w", err)
	}

	return nil
}
