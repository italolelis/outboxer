//go:generate mockgen -destination=./$GOFILE.mock_test.go -package=$GOPACKAGE github.com/Shopify/sarama SyncProducer
package kafka

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"time"

	"github.com/Shopify/sarama"
	"github.com/italolelis/outboxer"
)

var errInvalidProducerConfig = errors.New("invalid kafka producer config")

const (
	Topic     = "topic"
	MetaData  = "metadata"
	Partition = "partition"
)

type saramaDispatcher struct {
	sarama.SyncProducer
}

func newSaramaDispatcher(client sarama.Client) (*saramaDispatcher, error) {
	err := checkProducerConfig(client.Config())
	if err != nil {
		return nil, err
	}

	p, err := sarama.NewSyncProducerFromClient(client)
	if err != nil {
		return nil, err
	}

	return &saramaDispatcher{p}, nil
}

func (d *saramaDispatcher) Dispatch(ctx context.Context, msg *outboxer.OutboxMessage) error {
	errChan := make(chan error)

	go func() {
		errChan <- d.dispatchMessage(msg)
	}()

	select {
	case err := <-errChan:
		return err
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (d *saramaDispatcher) dispatchMessage(message *outboxer.OutboxMessage) error {
	producerMsg, err := buildSaramaProducerMessage(*message)
	if err != nil {
		return err
	}

	_, _, err = d.SendMessage(&producerMsg)
	if err != nil {
		return err
	}

	return nil
}

func buildSaramaProducerMessage(message outboxer.OutboxMessage) (sarama.ProducerMessage, error) {
	producerMsg := sarama.ProducerMessage{
		Headers: make([]sarama.RecordHeader, len(message.Headers)),
	}

	opts := message.Options
	topic, ok := opts[Topic]

	if !ok {
		return producerMsg, fmt.Errorf("%w: %s", errKafkaOptionMandatory, Topic)
	}

	producerMsg.Topic, ok = topic.(string)

	if !ok || producerMsg.Topic == "" {
		return producerMsg, fmt.Errorf(optionTypeErrFmt, errKafkaOptionType, Topic, "str", topic)
	}

	if data, ok := opts[MetaData]; ok {
		producerMsg.Metadata = data
	}

	if data, ok := opts[Partition]; ok {
		switch data.(type) {
		case float64:
			producerMsg.Partition = int32(data.(float64))
		case int32:
			producerMsg.Partition = data.(int32)
		default:
			return producerMsg, fmt.Errorf(optionTypeErrFmt, errKafkaOptionType, Partition, int32(1), data)
		}
	} else {
		producerMsg.Key = sarama.StringEncoder(strconv.FormatInt(message.ID, 10))
	}

	i := 0

	for key, val := range message.Headers {
		var v []byte
		switch val.(type) {
		case []byte:
			v = val.([]byte)
		case string:
			v = []byte(val.(string))
		default:
			return producerMsg, fmt.Errorf(optionTypeErrFmt, errKafkaOptionType, "Headers", map[string][]byte{}, val)
		}

		producerMsg.Headers[i] = sarama.RecordHeader{
			Key:   []byte(key),
			Value: v,
		}
		i++
	}

	producerMsg.Value = sarama.ByteEncoder(message.Payload)
	producerMsg.Timestamp = time.Now().UTC()

	return producerMsg, nil
}

func checkProducerConfig(cfg *sarama.Config) error {
	// Ref: https://pkg.go.dev/github.com/shopify/sarama#Config
	if !(cfg.Producer.Return.Errors && cfg.Producer.Return.Successes) {
		return fmt.Errorf("Producer.Return.Errors and Producer.Return.Successes must be true: %w", errInvalidProducerConfig)
	}

	// Prevent event loss by waiting for ack from all replicas.
	if cfg.Producer.RequiredAcks != sarama.WaitForAll {
		return fmt.Errorf("Producer.RequiredAcks: %w", errInvalidProducerConfig)
	}

	return nil
}
