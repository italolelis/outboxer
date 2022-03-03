package sqs

import (
	"context"
	"errors"
	"fmt"
	"os"
	"testing"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/sqs"
	"github.com/italolelis/outboxer"
)

func TestSQS_EventStream(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	endpoint := os.Getenv("SQS_ENDPOINT")
	if endpoint == "" {
		endpoint = "http://localhost:4566"
	}

	sess, err := session.NewSession(&aws.Config{
		CredentialsChainVerboseErrors: aws.Bool(true),
		Credentials:                   credentials.NewStaticCredentials("foo", "var", ""),
		Endpoint:                      aws.String(endpoint),
		Region:                        aws.String("us-east-1"),
	})
	if err != nil {
		t.Fatalf("failed to setup an aws session: %s", err)
	}

	sqsClient := sqs.New(sess)

	tests := []struct {
		message   string
		queueName *string
		attrs     map[string]*string
		groupId   *string
	}{
		{message: "test payload", queueName: aws.String("test"), attrs: nil, groupId: nil},
		{message: "test fifo payload", queueName: aws.String("test.fifo"), attrs: map[string]*string{"FifoQueue": aws.String("true")}, groupId: aws.String("123")},
	}

	for _, tc := range tests {

		queueName := tc.queueName
		queueArn := aws.String(fmt.Sprintf("%v/000000000000/%v", endpoint, *queueName))

		var err error
		if len(tc.attrs) > 0 {
			_, err = sqsClient.CreateQueueWithContext(ctx, &sqs.CreateQueueInput{QueueName: queueName, Attributes: tc.attrs})
		} else {
			_, err = sqsClient.CreateQueueWithContext(ctx, &sqs.CreateQueueInput{QueueName: queueName})
		}

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
			QueueNameOption: *queueArn,
		}

		if tc.groupId != nil {
			options[MessageGroupIdOption] = *tc.groupId
		}

		es := New(sqsClient)
		if err := es.Send(ctx, &outboxer.OutboxMessage{
			Payload: []byte(tc.message),
			Options: options,
			Headers: map[string]interface{}{
				"HeaderKey": *queueArn,
			},
		}); err != nil {
			t.Fatalf("an error was not expected: %s", err)
		}

	}

}
