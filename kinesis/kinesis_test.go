package kinesis

import (
	"context"
	"errors"
	"os"
	"testing"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/kinesis"
	"github.com/italolelis/outboxer"
)

func TestKinesis_EventStream(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	endpoint := os.Getenv("KINESIS_ENDPOINT")
	if endpoint == "" {
		endpoint = "http://localhost:4568"
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

	streamName := aws.String("test")

	kinesisClient := kinesis.New(sess)
	if _, err := kinesisClient.CreateStreamWithContext(ctx, &kinesis.CreateStreamInput{
		ShardCount: aws.Int64(1),
		StreamName: streamName,
	}); err != nil {
		var kErr awserr.Error
		if errors.As(err, &kErr) {
			if kErr.Code() == "ResourceInUseException" {
				t.Log(kErr.Message())
			}
		} else {
			t.Fatalf("failed to create stream: %s", err)
		}
	}

	if err := kinesisClient.WaitUntilStreamExistsWithContext(ctx, &kinesis.DescribeStreamInput{StreamName: streamName}); err != nil {
		t.Fatalf("failed to wait for stream creation: %s", err)
	}

	es := New(kinesisClient)
	if err := es.Send(ctx, &outboxer.OutboxMessage{
		Payload: []byte("test payload"),
		Options: map[string]interface{}{
			StreamNameOption: *streamName,
		},
	}); err != nil {
		t.Fatalf("an error was not expected: %s", err)
	}
}
