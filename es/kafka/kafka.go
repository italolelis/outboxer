package kafka

import (
	"context"
	"errors"
	"fmt"

	"github.com/Shopify/sarama"
	"github.com/italolelis/outboxer"
)

var (
	errKafkaOptionType      = errors.New("wrong type for kafka option")
	errKafkaOptionMandatory = errors.New("option is mandatory")
	optionTypeErrFmt        = "%w: %s should be %T but got %T"
)

type dispatcher interface {
	Dispatch(ctx context.Context, msg *outboxer.OutboxMessage) error
}

type SyncKafka struct {
	dispatcher
}

func NewSyncKafka(client sarama.Client) (*SyncKafka, error) {
	d, err := newSaramaDispatcher(client)
	if err != nil {
		return nil, err
	}

	return &SyncKafka{d}, nil
}

func (p *SyncKafka) Send(ctx context.Context, message *outboxer.OutboxMessage) error {
	err := p.Dispatch(ctx, message)
	if err != nil {
		return fmt.Errorf("publish failed: %w", err)
	}

	return nil
}
