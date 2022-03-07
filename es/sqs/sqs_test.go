package sqs_test

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/aws/request"
	sqsraw "github.com/aws/aws-sdk-go/service/sqs"
	"github.com/aws/aws-sdk-go/service/sqs/sqsiface"
	"github.com/italolelis/outboxer"
	"github.com/italolelis/outboxer/es/sqs"
)

type mockedSQS struct {
	sqsiface.SQSAPI
	Resp sqsraw.ReceiveMessageOutput
}

func (m mockedSQS) SendMessageWithContext(aws.Context, *sqsraw.SendMessageInput, ...request.Option) (*sqsraw.SendMessageOutput, error) {
	return nil, nil
}

func TestSQS_EventStream(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sqsClient := &mockedSQS{}

	tests := []struct {
		message   string
		queueName string
		attrs     map[string]*string
		groupId   *string
	}{
		{message: "test payload", queueName: "test", attrs: nil, groupId: nil},
		{message: "test fifo payload", queueName: "test.fifo", attrs: map[string]*string{"FifoQueue": aws.String("true")}, groupId: aws.String("123")},
	}

	for _, tc := range tests {
		queueName := tc.queueName
		queueArn := fmt.Sprintf("%v/000000000000/%v", "https://test", queueName)

		var err error

		if err != nil {
			var kErr awserr.Error
			if errors.As(err, &kErr) {
				if kErr.Code() == "ResourceInUseException" {
					t.Log(kErr.Message())
				}
			} else {
				t.Fatalf("failed to create queue: %s", err)
			}
		}

		options := map[string]interface{}{
			sqs.QueueNameOption: queueArn,
		}

		if tc.groupId != nil {
			options[sqs.MessageGroupIDOption] = *tc.groupId
		}

		es := sqs.New(sqsClient)
		if err := es.Send(ctx, &outboxer.OutboxMessage{
			Payload: []byte(tc.message),
			Options: options,
			Headers: map[string]interface{}{
				"HeaderKey": queueArn,
			},
		}); err != nil {
			t.Fatalf("an error was not expected: %s", err)
		}
	}
}
