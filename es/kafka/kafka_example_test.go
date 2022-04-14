package kafka_test

import (
	"context"
	"github.com/italolelis/outboxer/es/kafka"
	"time"

	"github.com/Shopify/sarama"
	"github.com/italolelis/outboxer"
)

func ExampleNewSyncKafka() {
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()

	cfg := sarama.NewConfig()

	// must set these as this guarantee event delivery to kafka
	cfg.Producer.RequiredAcks = sarama.WaitForAll
	cfg.Producer.Return.Successes = true
	cfg.Producer.Return.Errors = true

	client, err := sarama.NewClient([]string{"localhost:9092"}, cfg)
	if err != nil {
		panic(err)
	}
	defer client.Close()

	es, err := kafka.NewSyncKafka(client)
	if err != nil {
		panic(err)
	}

	if err := es.Send(ctx, &outboxer.OutboxMessage{
		ID:      int64(10),
		Payload: []byte("testing"),
		Options: map[string]interface{}{
			kafka.Topic: "test_topic",
		},
	}); err != nil {
		panic(err)
	}
}
