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

	queueName := aws.String("test")
	queueArn := aws.String(fmt.Sprintf("%v/000000000000/%v", endpoint, *queueName))

	sqsClient := sqs.New(sess)
	if _, err := sqsClient.CreateQueueWithContext(ctx, &sqs.CreateQueueInput{QueueName: queueName}); err != nil {
		var kErr awserr.Error
		if errors.As(err, &kErr) {
			if kErr.Code() == "ResourceInUseException" {
				t.Log(kErr.Message())
			}
		} else {
			t.Fatalf("failed to create queue: %s", err)
		}
	}

	es := New(sqsClient)
	if err := es.Send(ctx, &outboxer.OutboxMessage{
		Payload: []byte("test payload"),
		Options: map[string]interface{}{
			QueueNameOption: *queueArn,
		},
	}); err != nil {
		t.Fatalf("an error was not expected: %s", err)
	}
}

func TestSQSFIFO_EventStream(t *testing.T) {
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

	queueName := aws.String("test.fifo")
	queueArn := aws.String(fmt.Sprintf("%v/000000000000/%v", endpoint, *queueName))
	groupId := aws.String("123")

	attrs := map[string]*string{"FifoQueue": aws.String("true")}

	sqsClient := sqs.New(sess)
	if _, err := sqsClient.CreateQueueWithContext(ctx, &sqs.CreateQueueInput{QueueName: queueName, Attributes: attrs}); err != nil {
		var kErr awserr.Error
		if errors.As(err, &kErr) {
			if kErr.Code() == "ResourceInUseException" {
				t.Log(kErr.Message())
			}
		} else {
			t.Fatalf("failed to create queue: %s", err)
		}
	}

	es := New(sqsClient)
	if err := es.Send(ctx, &outboxer.OutboxMessage{
		Payload: []byte("test fifo payload"),
		Options: map[string]interface{}{
			QueueNameOption:      *queueArn,
			MessageGroupIdOption: *groupId,
		},
		Headers: map[string]interface{}{
			"HeaderKey": *groupId,
		},
	}); err != nil {
		t.Fatalf("an error was not expected: %s", err)
	}
}
