package sqs

import (
	"context"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/sqs"
	"github.com/italolelis/outboxer"
	"os"
	"testing"
)

func TestSqs_EventStream(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sess := session.Must(session.NewSessionWithOptions(session.Options{
		SharedConfigState: session.SharedConfigEnable,
	}))

	endpoint := os.Getenv("SQS_ENDPOINT")
	if endpoint == "" {
		endpoint = "http://localhost:4576"
	}

	svc := sqs.New(sess, &aws.Config{
		Region:   aws.String("us-east-1"),
		Endpoint: &endpoint,
	})

	queue, err := svc.CreateQueue(&sqs.CreateQueueInput{
		QueueName: aws.String("testing"),
	})
	if err != nil {
		t.Error(err)
		return
	}

	stream := New(svc)
	err = stream.Send(ctx, &outboxer.OutboxMessage{
		Payload: []byte("test payload"),
		Options: map[string]interface{}{
			QueueURL: *queue.QueueUrl,
		},
	})

	if err != nil {
		t.Error(err)
	}
}
